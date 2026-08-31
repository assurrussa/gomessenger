package messenger_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const testHandlerLiteral = "handler"

func TestBatchConfigNormalize(t *testing.T) {
	t.Parallel()

	got, err := (messenger.BatchConfig{}).Normalize(2)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got.MaxMessages != messenger.DefaultBatchMaxMessages ||
		got.MaxBytes != messenger.DefaultBatchMaxBytes || got.MaxWait != messenger.DefaultBatchMaxWait {
		t.Fatalf("Normalize() = %#v", got)
	}

	cases := []struct {
		name        string
		config      messenger.BatchConfig
		concurrency int
	}{
		{name: "negative messages", config: messenger.BatchConfig{MaxMessages: -1}, concurrency: 1},
		{name: "negative bytes", config: messenger.BatchConfig{MaxBytes: -1}, concurrency: 1},
		{name: "negative wait", config: messenger.BatchConfig{MaxWait: -1}, concurrency: 1},
		{name: "nil middleware", config: messenger.BatchConfig{Middlewares: []messenger.BatchMiddleware{nil}}, concurrency: 1},
		{name: "delivery overflow", config: messenger.BatchConfig{MaxMessages: math.MaxInt}, concurrency: 2},
		{name: "byte overflow", config: messenger.BatchConfig{MaxBytes: math.MaxInt}, concurrency: 2},
		{name: "invalid concurrency", concurrency: 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testCase.config.Normalize(testCase.concurrency); !errors.Is(err, messenger.ErrInvalidMessage) {
				t.Fatalf("Normalize() error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestChainBatchHandlerOrderAndShortCircuit(t *testing.T) {
	t.Parallel()

	var calls []string
	wrap := func(name string, shortCircuit bool) messenger.BatchHandlerMiddleware[int] {
		return func(next messenger.BatchHandler[int]) messenger.BatchHandler[int] {
			return func(ctx context.Context, messages []messenger.Message[int]) (messenger.BatchResult, error) {
				calls = append(calls, name+":before")
				if shortCircuit {
					return messenger.BatchResult{}, errors.New("short")
				}
				result, err := next(ctx, messages)
				calls = append(calls, name+":after")
				return result, err
			}
		}
	}
	handler := messenger.ChainBatchHandler(
		func(context.Context, []messenger.Message[int]) (messenger.BatchResult, error) {
			calls = append(calls, testHandlerLiteral)
			return messenger.BatchResult{}, nil
		},
		wrap("first", false), wrap("second", false),
	)
	if handler == nil {
		t.Fatal("ChainBatchHandler() returned nil")
	}
	if _, err := handler(t.Context(), nil); err != nil {
		t.Fatalf("handler error = %v", err)
	}
	want := []string{"first:before", "second:before", testHandlerLiteral, "second:after", "first:after"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}

	calls = nil
	short := messenger.ChainBatchHandler(handler, wrap("short", true))
	if _, err := short(t.Context(), nil); err == nil {
		t.Fatal("short-circuit error = nil")
	}
	if !reflect.DeepEqual(calls, []string{"short:before"}) {
		t.Fatalf("short-circuit calls = %#v", calls)
	}
	if messenger.ChainBatchHandler[int](nil) != nil ||
		messenger.ChainBatchHandler(handler, messenger.BatchHandlerMiddleware[int](nil)) != nil {
		t.Fatal("invalid batch chain was accepted")
	}
}

func TestDeferAfterWrappingAndBounds(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("later")
	if messenger.DeferAfter(nil, time.Second) != nil {
		t.Fatal("DeferAfter(nil) != nil")
	}
	for _, delay := range []time.Duration{0, -time.Nanosecond} {
		if err := messenger.DeferAfter(sentinel, delay); !errors.Is(err, messenger.ErrInvalidMessage) ||
			!errors.Is(err, sentinel) {
			t.Fatalf("DeferAfter(%v) = %v", delay, err)
		}
	}
	wrapped := messenger.Permanent(messenger.DeferAfter(messenger.RetryAfter(sentinel, time.Second), 2*time.Second))
	if !messenger.IsPermanent(wrapped) {
		t.Fatal("Permanent marker was lost")
	}
	if delay, ok := messenger.DeferDelay(wrapped); !ok || delay != 2*time.Second {
		t.Fatalf("DeferDelay() = (%v, %v)", delay, ok)
	}
	if delay, ok := messenger.RetryDelay(wrapped); !ok || delay != time.Second {
		t.Fatalf("RetryDelay() = (%v, %v)", delay, ok)
	}
}

func TestBatchResultBuilder(t *testing.T) {
	t.Parallel()

	id1, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000002")
	if err != nil {
		t.Fatal(err)
	}
	id3, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000003")
	if err != nil {
		t.Fatal(err)
	}

	msg1 := messenger.Message[string]{
		Metadata: messenger.Metadata{ID: id1, Source: testSource},
		Payload:  "hello",
	}
	msg2 := messenger.Message[string]{
		Metadata: messenger.Metadata{ID: id2, Source: testSource},
		Payload:  "world",
	}
	msg3 := messenger.Message[string]{
		Metadata: messenger.Metadata{ID: id3, Source: testSource},
		Payload:  "foo",
	}

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		builder := messenger.NewBatchResultBuilder([]messenger.Message[string](nil))
		if builder.HasErrors() {
			t.Fatal("empty builder has errors")
		}
		res := builder.Build()
		if len(res.Items) != 0 {
			t.Fatalf("empty builder produced items: %v", res.Items)
		}
	})

	t.Run("default success and fail", func(t *testing.T) {
		t.Parallel()
		messages := []messenger.Message[string]{msg1, msg2, msg3}
		builder := messenger.NewBatchResultBuilder(messages)

		if builder.HasErrors() {
			t.Fatal("initial builder has errors")
		}
		if err := builder.Error(msg2); err != nil {
			t.Fatalf("expected nil error for msg2, got %v", err)
		}

		failErr := errors.New("something went wrong")
		builder.Fail(msg2, failErr)

		if !builder.HasErrors() {
			t.Fatal("builder should have errors")
		}
		if !errors.Is(builder.Error(msg2), failErr) {
			t.Fatalf("expected %v, got %v", failErr, builder.Error(msg2))
		}

		// Re-marking msg2 as OK
		builder.OK(msg2)
		if builder.HasErrors() {
			t.Fatal("builder should have no errors after OK")
		}

		// Fail msg3 via Key
		key3 := messenger.BatchItemKey{Source: msg3.Metadata.Source, MessageID: msg3.Metadata.ID}
		builder.FailKey(key3, failErr)

		res := builder.Build()
		if len(res.Items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(res.Items))
		}
		if res.Items[0].Key.MessageID != id1 || res.Items[0].Err != nil {
			t.Fatalf("unexpected item 0: %+v", res.Items[0])
		}
		if res.Items[1].Key.MessageID != id2 || res.Items[1].Err != nil {
			t.Fatalf("unexpected item 1: %+v", res.Items[1])
		}
		if res.Items[2].Key.MessageID != id3 || !errors.Is(res.Items[2].Err, failErr) {
			t.Fatalf("unexpected item 2: %+v", res.Items[2])
		}
	})
}
