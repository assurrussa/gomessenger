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
	assertKafkaLogAttr(t, entries[1], logAttrAbortError, abortErr)
	if entries[2].message != "Kafka topology apply failed" || entries[2].level != messenger.LogError {
		t.Fatalf("topology log = %#v", entries[2])
	}
	assertKafkaLogAttr(t, entries[2], logAttrOperation, "topology_apply")
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
