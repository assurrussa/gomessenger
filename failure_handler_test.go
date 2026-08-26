package messenger_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const testSafeOperationFailure = "messenger: operation failed"

type testAdapterPanicError struct{ handlerID string }

func (err testAdapterPanicError) Error() string { return "adapter panic details" }

func (err testAdapterPanicError) HandlerPanicID() string { return err.handlerID }

func TestFailureDispositionsPreserveCauses(t *testing.T) {
	cause := errors.New("failure")
	permanent := messenger.Permanent(cause)
	if !errors.Is(permanent, cause) || !messenger.IsPermanent(fmt.Errorf("wrapped: %w", permanent)) ||
		messenger.Permanent(nil) != nil {
		t.Fatalf("permanent = %v", permanent)
	}
	retry := messenger.RetryAfter(cause, 125*time.Millisecond)
	delay, ok := messenger.RetryDelay(fmt.Errorf("wrapped: %w", retry))
	if !errors.Is(retry, cause) || !ok || delay != 125*time.Millisecond {
		t.Fatalf("retry = %v, %s, %v", retry, delay, ok)
	}
	if messenger.RetryAfter(nil, time.Second) != nil {
		t.Fatal("nil retry cause did not remain nil")
	}
	if err := messenger.RetryAfter(cause, 0); !errors.Is(err, messenger.ErrInvalidMessage) || !errors.Is(err, cause) {
		t.Fatalf("invalid retry = %v", err)
	}
	if _, ok := messenger.RetryDelay(cause); ok {
		t.Fatal("ordinary error has retry delay")
	}
}

func TestHandlePayloadAdapter(t *testing.T) {
	if messenger.HandlePayload[int](nil) != nil {
		t.Fatal("nil payload handler did not remain nil")
	}
	var handled int
	handler := messenger.HandlePayload(func(_ context.Context, value int) error {
		handled = value
		return nil
	})
	if err := handler(t.Context(), messenger.Message[int]{Payload: 42}); err != nil || handled != 42 {
		t.Fatalf("handle = %d, %v", handled, err)
	}
}

func TestHandlerPanicReportingAndFailureSanitizingKeepOperationalErrorsSafe(t *testing.T) {
	secret := "database password leaked"
	stack := []byte("sensitive stack")
	var report messenger.PanicReport
	err := messenger.ReportHandlerPanic(
		t.Context(),
		messenger.PanicReporterFunc(func(_ context.Context, received messenger.PanicReport) {
			report = received
		}),
		testRuntimeServiceID,
		secret,
		stack,
	)
	var panicErr messenger.HandlerPanicError
	if !errors.As(err, &panicErr) || panicErr.HandlerPanicID() != testRuntimeServiceID ||
		strings.Contains(err.Error(), secret) {
		t.Fatalf("panic error = %#v, %v", panicErr, err)
	}
	stack[0] = 'X'
	if report.HandlerID != testRuntimeServiceID || report.Value != secret || string(report.Stack) != "sensitive stack" {
		t.Fatalf("panic report = %#v", report)
	}

	cause := errors.New("password=secret")
	safe := messenger.SanitizeError(nil, cause)
	if safe.Error() != testSafeOperationFailure || !errors.Is(safe, cause) {
		t.Fatalf("sanitized error = %q, %v", safe, errors.Is(safe, cause))
	}
	panicking := messenger.FailureSanitizerFunc(func(error) string { panic("broken sanitizer") })
	if got := messenger.SanitizeFailure(panicking, cause); got != testSafeOperationFailure {
		t.Fatalf("panicking sanitizer result = %q", got)
	}
	unicode := messenger.FailureSanitizerFunc(func(error) string { return "éclair" })
	if got := messenger.BoundedFailureText(unicode, cause, 3); got != "éc" {
		t.Fatalf("bounded UTF-8 failure = %q", got)
	}
	if got := messenger.BoundedFailureText(unicode, cause, 1); got != "" {
		t.Fatalf("single-byte UTF-8 bound = %q", got)
	}
	if got := messenger.BoundedFailureText(unicode, cause, 0); got != "" {
		t.Fatalf("zero failure bound = %q", got)
	}
	short := messenger.FailureSanitizerFunc(func(error) string { return "ok" })
	if got := messenger.BoundedFailureText(short, cause, 8); got != "ok" {
		t.Fatalf("short bounded failure = %q", got)
	}
	if messenger.SanitizeError(nil, nil) != nil {
		t.Fatal("nil sanitized error became non-nil")
	}
	var nilSanitizer messenger.FailureSanitizerFunc
	if got := nilSanitizer.SanitizeFailure(cause); got != "" {
		t.Fatalf("nil sanitizer function = %q", got)
	}
	emptyPanicErr := messenger.ReportHandlerPanic(nil, nil, "", nil, nil) //nolint:staticcheck // Tests nil guard.
	var emptyPanic messenger.HandlerPanicError
	if !errors.As(emptyPanicErr, &emptyPanic) || emptyPanic.HandlerPanicID() != "" ||
		emptyPanic.Error() != "messenger: handler panicked" {
		t.Fatal("empty panic error exposed unstable details")
	}
}

func TestDefaultFailureSanitizerClassifiesSafePublicErrors(t *testing.T) {
	sanitizer := messenger.DefaultFailureSanitizer()
	if got := sanitizer.SanitizeFailure(nil); got != "" {
		t.Fatalf("nil failure = %q", got)
	}
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  messenger.ReportHandlerPanic(t.Context(), nil, testRuntimeServiceID, nil, nil),
			want: fmt.Sprintf("messenger: handler %s panicked", testRuntimeServiceID),
		},
		{
			err:  testAdapterPanicError{handlerID: testRuntimeServiceID},
			want: fmt.Sprintf("messenger: handler %s panicked", testRuntimeServiceID),
		},
		{err: context.Canceled, want: context.Canceled.Error()},
		{err: context.DeadlineExceeded, want: context.DeadlineExceeded.Error()},
		{err: messenger.ErrEnvelopeTooLarge, want: messenger.ErrEnvelopeTooLarge.Error()},
		{err: messenger.ErrDescriptorConflict, want: messenger.ErrDescriptorConflict.Error()},
		{err: messenger.ErrUnsupportedCapability, want: messenger.ErrUnsupportedCapability.Error()},
		{err: messenger.ErrMessageExpired, want: messenger.ErrMessageExpired.Error()},
		{err: messenger.ErrMessageNotReady, want: messenger.ErrMessageNotReady.Error()},
		{err: messenger.ErrInvalidMessage, want: messenger.ErrInvalidMessage.Error()},
		{err: errors.New("private failure"), want: testSafeOperationFailure},
	}
	for _, test := range tests {
		if got := sanitizer.SanitizeFailure(fmt.Errorf("wrapped: %w", test.err)); got != test.want {
			t.Errorf("sanitize %v = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestHandlerCompletionErrorRejectsLateSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := messenger.HandlerCompletionError(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("late success = %v", err)
	}
	cause := errors.New("handler failed")
	if err := messenger.HandlerCompletionError(ctx, cause); !errors.Is(err, cause) {
		t.Fatalf("handler error = %v", err)
	}
	//nolint:staticcheck // Verifies the exported nil-context guard.
	if err := messenger.HandlerCompletionError(nil, nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil handler context = %v", err)
	}
}
