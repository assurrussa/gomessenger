package batchruntime_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
)

func TestValidateResultExactCover(t *testing.T) {
	t.Parallel()
	first := key(t, "01991387-6880-7000-8000-000000000001")
	second := key(t, "01991387-6880-7000-8000-000000000002")
	sentinel := errors.New("retry")
	got, err := batchruntime.ValidateResult(
		[]messenger.BatchItemKey{first, second}, messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: second, Err: sentinel}, {Key: first},
		}})
	if err != nil || got[0] != nil || !errors.Is(got[1], sentinel) {
		t.Fatalf("ValidateResult() = (%#v, %v)", got, err)
	}
	invalid := []messenger.BatchResult{
		{Items: []messenger.BatchItemResult{{Key: first}}},
		{Items: []messenger.BatchItemResult{{Key: first}, {Key: first}}},
		{Items: []messenger.BatchItemResult{{Key: first}, {Key: key(t, "01991387-6880-7000-8000-000000000003")}}},
	}
	for index, result := range invalid {
		if _, err := batchruntime.ValidateResult(
			[]messenger.BatchItemKey{first, second}, result,
		); !errors.Is(err, messenger.ErrInvalidBatchResult) {
			t.Fatalf("case %d: error = %v", index, err)
		}
	}
}

func TestInvokeMiddlewareOrderNextOncePanicAndCancellation(t *testing.T) {
	message := messenger.Message[int]{Metadata: messenger.Metadata{
		ID:     key(t, "01991387-6880-7000-8000-000000000011").MessageID,
		Source: "urn:test", Headers: map[string]string{"trace": "original"},
	}}
	var calls []string
	middleware := func(name string) messenger.BatchMiddleware {
		return func(
			ctx context.Context,
			metadata []messenger.Metadata,
			_ string,
			next messenger.BatchHandlerFunc,
		) (messenger.BatchResult, error) {
			calls = append(calls, name+":before")
			metadata[0].Headers["trace"] = "mutated"
			result, err := next(ctx)
			calls = append(calls, name+":after")
			return result, err
		}
	}
	result, err := batchruntime.Invoke(t.Context(), []messenger.Message[int]{message}, "handler", func(
		_ context.Context, messages []messenger.Message[int],
	) (messenger.BatchResult, error) {
		calls = append(calls, "handler")
		if messages[0].Metadata.Headers["trace"] != "original" {
			t.Fatal("middleware mutated handler metadata")
		}
		return messenger.BatchResult{Items: []messenger.BatchItemResult{{Key: key(t,
			"01991387-6880-7000-8000-000000000011")}}}, nil
	}, []messenger.BatchMiddleware{middleware("first"), middleware("second")}, nil)
	if err != nil || len(result.Items) != 1 {
		t.Fatalf("Invoke() = (%#v, %v)", result, err)
	}
	want := []string{"first:before", "second:before", "handler", "second:after", "first:after"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}

	double := messenger.BatchMiddleware(func(ctx context.Context, _ []messenger.Metadata, _ string,
		next messenger.BatchHandlerFunc,
	) (messenger.BatchResult, error) {
		_, _ = next(ctx)
		return next(ctx)
	})
	if _, err := batchruntime.Invoke(t.Context(), []messenger.Message[int]{message}, "handler",
		func(context.Context, []messenger.Message[int]) (messenger.BatchResult, error) {
			return messenger.BatchResult{}, nil
		}, []messenger.BatchMiddleware{double}, nil); !errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("double next error = %v", err)
	}

	if _, err := batchruntime.Invoke(t.Context(), []messenger.Message[int]{message}, "handler",
		func(context.Context, []messenger.Message[int]) (messenger.BatchResult, error) { panic("boom") },
		nil, nil); err == nil {
		t.Fatal("panic error = nil")
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := batchruntime.Invoke(canceled, []messenger.Message[int]{message}, "handler",
		func(context.Context, []messenger.Message[int]) (messenger.BatchResult, error) {
			return messenger.BatchResult{}, nil
		}, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestClassifyPrecedence(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("failed")
	err := messenger.Permanent(messenger.DeferAfter(messenger.RetryAfter(sentinel, time.Second), 2*time.Second))
	if kind, delay := batchruntime.Classify(err); kind != batchruntime.FailurePermanent || delay != 0 {
		t.Fatalf("Classify() = (%v, %v)", kind, delay)
	}
}

func key(t *testing.T, id string) messenger.BatchItemKey {
	t.Helper()
	parsed, err := messenger.ParseMessageID(id)
	if err != nil {
		t.Fatal(err)
	}
	return messenger.BatchItemKey{Source: "urn:test", MessageID: parsed}
}

func TestInvokeTimeoutClearsNonEmptyResult(t *testing.T) {
	t.Parallel()
	itemKey := key(t, "01991387-6880-7000-8000-000000000081")
	message := messenger.Message[int]{
		Metadata: messenger.Metadata{ID: itemKey.MessageID, Source: itemKey.Source},
		Payload:  42,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result, err := batchruntime.Invoke(ctx, []messenger.Message[int]{message}, "handler",
		func(_ context.Context, _ []messenger.Message[int]) (messenger.BatchResult, error) {
			cancel()
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: itemKey, Err: nil},
				},
			}, nil
		}, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("err was wrapped in ErrInvalidBatchResult: %v", err)
	}
	if len(result.Items) != 0 {
		t.Fatalf("result items not cleared: %#v", result)
	}
}

func TestInvokeOuterMiddlewareTimeoutClearsResult(t *testing.T) {
	t.Parallel()
	itemKey := key(t, "01991387-6880-7000-8000-000000000081")
	message := messenger.Message[int]{
		Metadata: messenger.Metadata{ID: itemKey.MessageID, Source: itemKey.Source},
		Payload:  42,
	}
	ctx, cancel := context.WithCancel(context.Background())
	middleware := messenger.BatchMiddleware(func(
		mwCtx context.Context,
		_ []messenger.Metadata,
		_ string,
		next messenger.BatchHandlerFunc,
	) (messenger.BatchResult, error) {
		res, err := next(mwCtx)
		cancel()
		return res, err
	})
	result, err := batchruntime.Invoke(ctx, []messenger.Message[int]{message}, "handler",
		func(_ context.Context, _ []messenger.Message[int]) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: itemKey, Err: nil},
				},
			}, nil
		}, []messenger.BatchMiddleware{middleware}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("err was wrapped in ErrInvalidBatchResult: %v", err)
	}
	if len(result.Items) != 0 {
		t.Fatalf("result items not cleared: %#v", result)
	}
}

func TestInvokeGuardedNextConcurrentCalls(t *testing.T) {
	t.Parallel()
	itemKey := key(t, "01991387-6880-7000-8000-000000000081")
	message := messenger.Message[int]{
		Metadata: messenger.Metadata{ID: itemKey.MessageID, Source: itemKey.Source},
		Payload:  42,
	}
	concurrentMiddleware := messenger.BatchMiddleware(func(
		ctx context.Context,
		_ []messenger.Metadata,
		_ string,
		next messenger.BatchHandlerFunc,
	) (messenger.BatchResult, error) {
		var wg sync.WaitGroup
		var errs [2]error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errs[0] = next(ctx)
		}()
		go func() {
			defer wg.Done()
			_, errs[1] = next(ctx)
		}()
		wg.Wait()
		if errs[0] != nil {
			return messenger.BatchResult{}, errs[0]
		}
		return messenger.BatchResult{}, errs[1]
	})
	_, err := batchruntime.Invoke(context.Background(), []messenger.Message[int]{message}, "handler",
		func(context.Context, []messenger.Message[int]) (messenger.BatchResult, error) {
			return messenger.BatchResult{}, nil
		}, []messenger.BatchMiddleware{concurrentMiddleware}, nil)
	if !errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("err = %v, want ErrInvalidBatchResult", err)
	}
}
