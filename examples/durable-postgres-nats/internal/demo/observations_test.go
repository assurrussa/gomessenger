//nolint:testpackage // Tests exercise the package-local benchmark observer index.
package demo

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestBenchmarkObservationRecorderUsesTransactionRegisteredLabels(t *testing.T) {
	t.Parallel()
	recorder := newBenchmarkObservationRecorder()
	messageID := mustMessageID(t)
	labels := BenchmarkLabels{RunID: testRunID, StageID: testStageID}
	recorder.register(messageID.String(), labels)
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: messageID, Duration: 2 * time.Millisecond,
	})
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationBrokerAck, MessageID: messageID, Duration: 500 * time.Microsecond,
	})
	stats := recorder.stats(labels)
	if stats.InboxHandle.Count != 1 || stats.InboxHandle.P95Millis != 2 ||
		stats.BrokerAck.Count != 1 || stats.BrokerAck.P95Millis != 0.5 {
		t.Fatalf("stats = %#v", stats)
	}
	// A successful ACK releases the message index, so later observations cannot
	// leak into the completed stage.
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: messageID, Duration: time.Second,
	})
	if got := recorder.stats(labels).InboxHandle.Count; got != 1 {
		t.Fatalf("handle count after ACK = %d, want 1", got)
	}
}

func TestBenchmarkObservationRecorderUnregistersRolledBackProducer(t *testing.T) {
	t.Parallel()
	recorder := newBenchmarkObservationRecorder()
	messageID := mustMessageID(t)
	labels := BenchmarkLabels{RunID: testRunID, StageID: testStageID}
	recorder.register(messageID.String(), labels)
	recorder.unregister(messageID.String())
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: messageID, Duration: time.Millisecond,
		Err: errors.New("must be ignored"),
	})
	if got := recorder.stats(labels); got != (ConsumerObservationStats{}) {
		t.Fatalf("rolled-back observation stats = %#v", got)
	}
}
