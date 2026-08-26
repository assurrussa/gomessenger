package nats

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type natsObservabilityLogEntry struct {
	message string
}

type natsObservabilityLogRecorder struct {
	mu      sync.Mutex
	entries []natsObservabilityLogEntry
}

func (recorder *natsObservabilityLogRecorder) Log(
	_ context.Context,
	_ messenger.LogLevel,
	message string,
	_ ...messenger.LogAttr,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.entries = append(recorder.entries, natsObservabilityLogEntry{message: message})
}

func (recorder *natsObservabilityLogRecorder) snapshot() []natsObservabilityLogEntry {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]natsObservabilityLogEntry(nil), recorder.entries...)
}

type nilNATSExtractPropagator struct{}

func (nilNATSExtractPropagator) Inject(context.Context, map[string]string) {}

func (nilNATSExtractPropagator) Extract(context.Context, map[string]string) context.Context {
	return nil
}

type natsPanicRecorder struct {
	handlerID string
	value     any
	stack     []byte
}

func (recorder *natsPanicRecorder) ReportPanic(
	_ context.Context,
	handlerID string,
	recovered any,
	stack []byte,
) {
	recorder.handlerID = handlerID
	recorder.value = recovered
	recorder.stack = stack
}

func TestDeliveryContextFallbackPreservesParentCancellation(t *testing.T) {
	logger := &natsObservabilityLogRecorder{}
	parent, cancel := context.WithCancel(t.Context())
	config := HandlerConfig{
		ConsumerID: testDLQConsumerID,
		Logger:     logger,
		Propagator: nilNATSExtractPropagator{},
	}
	extracted := extractDeliveryContext(parent, config, map[string]string{"traceparent": "invalid"})
	cancel()
	select {
	case <-extracted.Done():
	case <-time.After(time.Second):
		t.Fatal("fallback context detached from parent cancellation")
	}
	entries := logger.snapshot()
	if len(entries) != 1 || entries[0].message != "context propagator returned nil" {
		t.Fatalf("fallback logs = %#v", entries)
	}
}

func TestMiddlewarePanicUsesConfiguredReporterAndSafeError(t *testing.T) {
	reporter := &natsPanicRecorder{}
	err := invokeMiddlewares(
		t.Context(),
		messenger.Metadata{},
		testDLQConsumerID,
		func(context.Context) error { panic("secret NATS panic") },
		nil,
		reporter,
	)
	if err == nil || !strings.Contains(err.Error(), "handler "+testDLQConsumerID+" panicked") ||
		strings.Contains(err.Error(), "secret NATS panic") {
		t.Fatalf("panic error = %v", err)
	}
	var panicErr interface {
		error
		HandlerPanicID() string
	}
	if !errors.As(err, &panicErr) || panicErr.HandlerPanicID() != testDLQConsumerID {
		t.Fatalf("panic classification = %#v, %v", panicErr, err)
	}
	if reporter.handlerID != testDLQConsumerID || reporter.value != "secret NATS panic" || len(reporter.stack) == 0 {
		t.Fatalf("panic report = %q, %#v, %d bytes", reporter.handlerID, reporter.value, len(reporter.stack))
	}
}

func TestHandlerCompletionUsesReplacementContextAfterMiddlewareReturnsNil(t *testing.T) {
	replacement, cancelReplacement := context.WithCancel(t.Context())
	config := HandlerConfig{Middlewares: []messenger.Middleware{
		func(_ context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
			cancelReplacement()
			_ = next(replacement)
			return nil
		},
	}}
	if err := applyObservabilityDefaults(&config); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	var calls int
	err := invokeMiddlewares(t.Context(), messenger.Metadata{}, testDLQConsumerID, func(ctx context.Context) error {
		calls++
		if ctx != replacement {
			t.Fatalf("handler context = %v, want replacement", ctx)
		}
		return nil
	}, config.Middlewares, nil)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("replacement completion = %v, calls = %d", err, calls)
	}
}

func TestObservabilityDefaultsRejectTypedNilPanicReporter(t *testing.T) {
	var reporter *natsPanicRecorder
	config := HandlerConfig{PanicReporter: reporter}
	if err := applyObservabilityDefaults(&config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("typed-nil panic reporter = %v", err)
	}
	config = HandlerConfig{}
	if err := applyObservabilityDefaults(&config); err != nil {
		t.Fatalf("default observability: %v", err)
	}
	if got := sanitizeFailure(config.FailureSanitizer, errors.New("secret")); got != operationFailureText {
		t.Fatalf("default sanitizer = %q", got)
	}
}
