package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type kafkaLogEntry struct {
	level   messenger.LogLevel
	message string
	attrs   []messenger.LogAttr
}

type kafkaLogRecorder struct {
	mu      sync.Mutex
	entries []kafkaLogEntry
}

type nilKafkaExtractPropagator struct{}

func (nilKafkaExtractPropagator) Inject(context.Context, map[string]string) {}

func (nilKafkaExtractPropagator) Extract(context.Context, map[string]string) context.Context {
	return nil
}

type kafkaPanicRecorder struct {
	handlerID string
	value     any
	stack     []byte
}

func (recorder *kafkaPanicRecorder) ReportPanic(
	_ context.Context,
	handlerID string,
	recovered any,
	stack []byte,
) {
	recorder.handlerID = handlerID
	recorder.value = recovered
	recorder.stack = stack
}

func (recorder *kafkaLogRecorder) Log(
	_ context.Context,
	level messenger.LogLevel,
	message string,
	attrs ...messenger.LogAttr,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.entries = append(recorder.entries, kafkaLogEntry{
		level: level, message: message, attrs: append([]messenger.LogAttr(nil), attrs...),
	})
}

func (recorder *kafkaLogRecorder) snapshot() []kafkaLogEntry {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	entries := make([]kafkaLogEntry, len(recorder.entries))
	copy(entries, recorder.entries)
	return entries
}

func TestTransportLoggerRecordsSafeInfrastructureFailures(t *testing.T) {
	logger := &kafkaLogRecorder{}
	transport, err := NewTransport(TransportConfig{
		Name: testTransportName, Brokers: []string{testBrokerAddress}, ClientID: testTransportClientID,
		InstanceID: testTransportInstance, OperationTimeout: 100 * time.Millisecond, Logger: logger,
	})
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(transport.closeClient)

	fencedErr := errors.New("transactional producer fenced")
	consumer := &Consumer{transport: transport, config: HandlerConfig{ConsumerID: testConsumerID}}
	consumer.recordWorkerError(t.Context(), fencedErr)
	consumer.recordWorkerError(t.Context(), errors.New("duplicate worker failure"))
	abortErr := errors.New("abort timed out")
	transport.logTransactionFailure(t.Context(), "publish_commit", fencedErr, abortErr,
		messenger.LogAttr{Key: logAttrRoute, Value: "events"},
		messenger.LogAttr{Key: logAttrTopic, Value: testSourceTopic})
	invalidTopology := validTestTopology()
	invalidTopology.SpecVersion = "invalid"
	if _, err := ApplyTopology(t.Context(), transport, invalidTopology); err == nil {
		t.Fatal("ApplyTopology accepted an invalid spec version")
	}

	entries := logger.snapshot()
	if len(entries) != 3 {
		t.Fatalf("log entries = %d, want 3", len(entries))
	}
	if entries[0].message != "Kafka consumer worker failed" || entries[0].level != messenger.LogError {
		t.Fatalf("worker log = %#v", entries[0])
	}
	if entries[1].message != "Kafka transaction failed" || entries[1].level != messenger.LogError {
		t.Fatalf("transaction log = %#v", entries[1])
	}
	assertSafeKafkaLogAttrs(t, entries)
	assertKafkaLogAttr(t, entries[1], logAttrOperation, "publish_commit")
	assertKafkaLogAttr(t, entries[1], logAttrAbortAttempted, true)
	assertSafeKafkaLogErrorAttr(t, entries[1], logAttrAbortError, abortErr)
	if entries[2].message != "Kafka topology apply failed" || entries[2].level != messenger.LogError {
		t.Fatalf("topology log = %#v", entries[2])
	}
	assertKafkaLogAttr(t, entries[2], logAttrOperation, "topology_apply")
}

func TestDeliveryContextFallbackPreservesParentCancellation(t *testing.T) {
	logger := &kafkaLogRecorder{}
	parent, cancel := context.WithCancel(t.Context())
	config := HandlerConfig{
		ConsumerID: testConsumerID,
		Logger:     logger,
		Propagator: nilKafkaExtractPropagator{},
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
	reporter := &kafkaPanicRecorder{}
	err := invokeMiddlewares(
		t.Context(),
		messenger.Metadata{},
		testConsumerID,
		func(context.Context) error { panic("secret Kafka panic") },
		nil,
		reporter,
	)
	if err == nil || !strings.Contains(err.Error(), "handler "+testConsumerID+" panicked") ||
		strings.Contains(err.Error(), "secret Kafka panic") {
		t.Fatalf("panic error = %v", err)
	}
	var panicErr interface {
		error
		HandlerPanicID() string
	}
	if !errors.As(err, &panicErr) || panicErr.HandlerPanicID() != testConsumerID {
		t.Fatalf("panic classification = %#v, %v", panicErr, err)
	}
	if reporter.handlerID != testConsumerID || reporter.value != "secret Kafka panic" || len(reporter.stack) == 0 {
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
	err := invokeMiddlewares(t.Context(), messenger.Metadata{}, testConsumerID, func(ctx context.Context) error {
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
	var reporter *kafkaPanicRecorder
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

func assertSafeKafkaLogAttrs(t *testing.T, entries []kafkaLogEntry) {
	t.Helper()
	for _, entry := range entries {
		for _, attr := range entry.attrs {
			key := strings.ToLower(attr.Key)
			if strings.Contains(key, "payload") || strings.Contains(key, "header") || key == "key" ||
				strings.HasSuffix(key, "_key") {
				t.Fatalf("unsafe Kafka log attribute %q in %q", attr.Key, entry.message)
			}
		}
	}
}

func assertKafkaLogAttr(t *testing.T, entry kafkaLogEntry, key string, want any) {
	t.Helper()
	for _, attr := range entry.attrs {
		if attr.Key == key {
			if attr.Value != want {
				t.Fatalf("log attribute %q = %#v, want %#v", key, attr.Value, want)
			}
			return
		}
	}
	t.Fatalf("log attribute %q is missing from %#v", key, entry)
}

func assertSafeKafkaLogErrorAttr(t *testing.T, entry kafkaLogEntry, key string, cause error) {
	t.Helper()
	for _, attr := range entry.attrs {
		if attr.Key != key {
			continue
		}
		err, ok := attr.Value.(error)
		if !ok || err.Error() != operationFailureText || !errors.Is(err, cause) {
			t.Fatalf("log attribute %q = %#v, want sanitized wrapper for %v", key, attr.Value, cause)
		}
		return
	}
	t.Fatalf("log attribute %q is missing from %#v", key, entry)
}
