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

func TestBenchmarkObservationRecorderHandlesBatchAckBeforeItemObservation(t *testing.T) {
	t.Parallel()
	recorder := newBenchmarkObservationRecorder()
	messageID := mustMessageID(t)
	labels := BenchmarkLabels{RunID: testRunID, StageID: testStageID}
	recorder.register(messageID.String(), labels)
	recorder.recordAccepted(labels, 1)

	// Batch finalization observes the confirmed ACK before the per-item handle
	// outcome. The label must remain available for the latter observation.
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationBrokerAck, MessageID: messageID, Duration: time.Millisecond,
	})
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: messageID, Duration: 2 * time.Millisecond,
	})

	stats := recorder.stats(labels)
	progress := recorder.progress(labels)
	if stats.InboxHandle.Count != 1 || stats.BrokerAck.Count != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	if progress != (BenchmarkProgressStats{Accepted: 1, Staged: 1, Committed: 1}) {
		t.Fatalf("progress = %#v", progress)
	}
	recorder.Observe(context.Background(), messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: messageID, Duration: time.Second,
	})
	if got := recorder.progress(labels).Committed; got != 1 {
		t.Fatalf("committed after completed observation = %d, want 1", got)
	}
}

func TestBenchmarkObservationRecorderExcludesFailedAndDuplicateCommits(t *testing.T) {
	t.Parallel()
	recorder := newBenchmarkObservationRecorder()
	labels := BenchmarkLabels{RunID: testRunID, StageID: testStageID}
	recorder.recordAccepted(labels, 3)

	for index, observation := range []messenger.Observation{
		{Operation: messenger.OperationHandle, Err: errors.New("retry")},
		{Operation: messenger.OperationHandle, Duplicate: true},
	} {
		messageID := mustMessageID(t)
		recorder.register(messageID.String(), labels)
		observation.MessageID = messageID
		recorder.Observe(context.Background(), observation)
		if index == 1 {
			recorder.Observe(context.Background(), messenger.Observation{
				Operation: messenger.OperationBrokerAck, MessageID: messageID,
			})
		}
	}

	progress := recorder.progress(labels)
	if progress.Accepted != 3 || progress.Staged != 3 || progress.Committed != 0 {
		t.Fatalf("progress = %#v", progress)
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

func TestBenchmarkObservationRecorderReportsActualBatchInvocations(t *testing.T) {
	t.Parallel()
	recorder := newBenchmarkObservationRecorder()
	labels := BenchmarkLabels{RunID: testRunID, StageID: testStageID}
	recorder.recordBatch(labels, 1, 2*time.Millisecond, nil)
	recorder.recordBatch(labels, 100, 8*time.Millisecond, errors.New("rolled back"))

	stats := recorder.stats(labels).Batch
	if stats.Invocations != 2 || stats.Messages != 101 || stats.AverageMessages != 50.5 ||
		stats.MaxMessages != 100 || stats.Handler.Count != 2 || stats.Handler.Errors != 1 ||
		stats.Handler.P95Millis != 7.7 {
		t.Fatalf("batch stats = %#v", stats)
	}
}
