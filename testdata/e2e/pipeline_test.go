package e2e_test

import (
	"context"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTransactionalOutboxRollbackPublishesNothing(t *testing.T) {
	harness := newTestHarness(t)
	rollback := errors.New("force producer rollback")
	receipt, err := harness.stage(t, 41, rollback)
	if !errors.Is(err, rollback) {
		t.Fatalf("stage rollback = %v", err)
	}
	if receipt.State != messenger.ReceiptStaged {
		t.Fatalf("provisional receipt = %#v", receipt)
	}
	if rows := harness.producerRows(t); rows != 0 {
		t.Fatalf("producer rows after rollback = %d", rows)
	}
	if jobs := harness.outboxTotal(t); jobs != 0 {
		t.Fatalf("outbox rows after rollback = %d", jobs)
	}

	outboxRunner := harness.startOutbox(t)
	if messages := harness.streamMessages(t); messages != 0 {
		t.Fatalf("stream messages after rollback = %d", messages)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalPipelineCommitsAndAcknowledges(t *testing.T) {
	harness := newTestHarness(t)
	receipt, err := harness.stage(t, 42, nil)
	if err != nil {
		t.Fatalf("stage command: %v", err)
	}
	if receipt.State != messenger.ReceiptStaged || receipt.MessageID.IsZero() {
		t.Fatalf("staged receipt = %#v", receipt)
	}

	connection := connectNATS(t, harness.server.ClientURL())
	var calls atomic.Int32
	consumer := harness.newConsumer(
		t,
		connection,
		"happy-worker",
		func(ctx context.Context, message messenger.Message[processPayload]) error {
			if message.Metadata.ID != receipt.MessageID || message.Payload.JobID != 42 {
				return errors.New("unexpected delivered message")
			}
			calls.Add(1)
			return harness.incrementBusiness(ctx)
		},
		nil,
	)
	consumerRunner := startConsumer(t, consumer, false)
	outboxRunner := harness.startOutbox(t)

	harness.waitConsumerEmpty(t, "happy-worker")
	harness.waitOutboxEmpty(t)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if count := harness.businessCount(t); count != 1 {
		t.Fatalf("business counter = %d, want 1", count)
	}
	if rows := harness.producerRows(t); rows != 1 {
		t.Fatalf("producer rows = %d, want 1", rows)
	}
	if duplicates := harness.duplicates.Load(); duplicates != 0 {
		t.Fatalf("duplicate inbox deliveries = %d, want 0", duplicates)
	}
	if err := stopService(t, consumerRunner); err != nil {
		t.Fatalf("stop consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalBatchPipelineCommitsOneSharedTransaction(t *testing.T) {
	harness := newTestHarness(t)
	for _, jobID := range []int64{51, 52, 53} {
		if _, err := harness.stage(t, jobID, nil); err != nil {
			t.Fatalf("stage command %d: %v", jobID, err)
		}
	}

	connection := connectNATS(t, harness.server.ClientURL())
	var calls atomic.Int32
	consumer := harness.newBatchConsumer(
		t,
		connection,
		"batch-worker",
		func(ctx context.Context, messages []messenger.Message[processPayload]) (messenger.BatchResult, error) {
			calls.Add(1)
			if len(messages) != 3 {
				return messenger.BatchResult{}, errors.New("expected one three-message batch")
			}
			seen := make(map[int64]struct{}, len(messages))
			result := messenger.BatchResult{Items: make([]messenger.BatchItemResult, len(messages))}
			for index, message := range messages {
				if message.Payload.JobID < 51 || message.Payload.JobID > 53 {
					return messenger.BatchResult{}, errors.New("unexpected batch payload")
				}
				if _, exists := seen[message.Payload.JobID]; exists {
					return messenger.BatchResult{}, errors.New("duplicate batch payload")
				}
				seen[message.Payload.JobID] = struct{}{}
				if err := harness.incrementBusiness(ctx); err != nil {
					return messenger.BatchResult{}, err
				}
				result.Items[index] = messenger.BatchItemResult{Key: messenger.BatchItemKey{
					Source: message.Metadata.Source, MessageID: message.Metadata.ID,
				}}
			}
			return result, nil
		},
		messenger.BatchConfig{MaxMessages: 3, MaxWait: 500 * time.Millisecond},
	)
	consumerRunner := startConsumer(t, consumer, false)
	outboxRunner := harness.startOutbox(t)

	harness.waitConsumerEmpty(t, "batch-worker")
	harness.waitOutboxEmpty(t)
	if got := calls.Load(); got != 1 {
		t.Fatalf("batch handler calls = %d, want 1", got)
	}
	if count := harness.businessCount(t); count != 3 {
		t.Fatalf("business counter = %d, want 3", count)
	}
	if rows := harness.producerRows(t); rows != 3 {
		t.Fatalf("producer rows = %d, want 3", rows)
	}
	if duplicates := harness.duplicates.Load(); duplicates != 0 {
		t.Fatalf("duplicate inbox deliveries = %d, want 0", duplicates)
	}
	if err := stopService(t, consumerRunner); err != nil {
		t.Fatalf("stop batch consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalPipelineSurvivesLostAck(t *testing.T) {
	harness := newTestHarness(t)
	receipt, err := harness.stage(t, 42, nil)
	if err != nil {
		t.Fatalf("stage command: %v", err)
	}
	if receipt.State != messenger.ReceiptStaged || receipt.MessageID.IsZero() {
		t.Fatalf("staged receipt = %#v", receipt)
	}

	firstConnection := connectNATS(t, harness.server.ClientURL())
	var handlerCalls atomic.Int32
	first := harness.newConsumer(
		t,
		firstConnection,
		"lost-ack-worker",
		func(ctx context.Context, message messenger.Message[processPayload]) error {
			if message.Metadata.ID != receipt.MessageID || message.Payload.JobID != 42 {
				return errors.New("unexpected delivered message")
			}
			if ctx.Value(traceContextKey{}) !=
				"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01|vendor=value" {
				return errors.New("trace context did not survive outbox delivery")
			}
			if err := harness.incrementBusiness(ctx); err != nil {
				return err
			}
			handlerCalls.Add(1)
			firstConnection.Close()
			return nil
		},
		nil,
	)
	firstRunner := startConsumer(t, first, true)
	outboxRunner := harness.startOutbox(t)

	eventually(t, func() (bool, error) {
		return harness.businessCount(t) == 1, nil
	})
	_ = waitService(t, firstRunner)

	secondConnection := connectNATS(t, harness.server.ClientURL())
	second := harness.newConsumer(
		t,
		secondConnection,
		"lost-ack-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			handlerCalls.Add(1)
			return harness.incrementBusiness(ctx)
		},
		nil,
	)
	secondRunner := startConsumer(t, second, false)
	harness.waitConsumerEmpty(t, "lost-ack-worker")
	harness.waitOutboxEmpty(t)

	if calls := handlerCalls.Load(); calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	if count := harness.businessCount(t); count != 1 {
		t.Fatalf("business counter = %d, want 1", count)
	}
	if rows := harness.producerRows(t); rows != 1 {
		t.Fatalf("producer rows = %d, want 1", rows)
	}
	if duplicates := harness.duplicates.Load(); duplicates != 1 {
		t.Fatalf("duplicate inbox deliveries = %d, want 1", duplicates)
	}
	if err := stopService(t, secondRunner); err != nil {
		t.Fatalf("stop replacement consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalPipelineRetriesThenCommitsOnce(t *testing.T) {
	harness := newTestHarness(t)
	if _, err := harness.stage(t, 43, nil); err != nil {
		t.Fatalf("stage command: %v", err)
	}

	connection := connectNATS(t, harness.server.ClientURL())
	attemptTimes := make(chan time.Time, 2)
	var attempts atomic.Int32
	consumer := harness.newConsumer(
		t,
		connection,
		"retry-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			attemptTimes <- time.Now()
			if attempts.Add(1) == 1 {
				return messenger.RetryAfter(errors.New("dependency busy"), 150*time.Millisecond)
			}
			return harness.incrementBusiness(ctx)
		},
		nil,
	)
	consumerRunner := startConsumer(t, consumer, false)
	outboxRunner := harness.startOutbox(t)

	firstAttempt := receiveTime(t, attemptTimes)
	secondAttempt := receiveTime(t, attemptTimes)
	if delay := secondAttempt.Sub(firstAttempt); delay < 120*time.Millisecond {
		t.Fatalf("retry delay = %s, want at least 120ms", delay)
	}
	harness.waitConsumerEmpty(t, "retry-worker")
	harness.waitOutboxEmpty(t)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if count := harness.businessCount(t); count != 1 {
		t.Fatalf("business counter = %d, want 1", count)
	}
	if err := stopService(t, consumerRunner); err != nil {
		t.Fatalf("stop consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalPipelinePublishesPermanentFailureToDLQ(t *testing.T) {
	harness := newTestHarness(t)
	receipt, err := harness.stage(t, 44, nil)
	if err != nil {
		t.Fatalf("stage command: %v", err)
	}
	dlqSubscription, err := harness.relayConn.SubscribeSync(testDLQ)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := harness.relayConn.Flush(); err != nil {
		t.Fatalf("flush DLQ subscription: %v", err)
	}

	connection := connectNATS(t, harness.server.ClientURL())
	var calls atomic.Int32
	consumer := harness.newConsumer(
		t,
		connection,
		"dlq-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			calls.Add(1)
			if err := harness.incrementBusiness(ctx); err != nil {
				return err
			}
			return messenger.Permanent(errors.New("unsupported job"))
		},
		nil,
	)
	consumerRunner := startConsumer(t, consumer, false)
	outboxRunner := harness.startOutbox(t)

	dlqMessage, err := dlqSubscription.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	record, err := natsadapter.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ record: %v", err)
	}
	if record.ConsumerID != "dlq-worker" || record.FailureKind != permanentFailureKind || record.Attempt != 1 {
		t.Fatalf("DLQ record = %#v", record)
	}
	envelope, err := messenger.UnmarshalEnvelope(record.Envelope)
	if err != nil {
		t.Fatalf("decode DLQ envelope: %v", err)
	}
	if envelope.ID != receipt.MessageID {
		t.Fatalf("DLQ message ID = %s, want %s", envelope.ID, receipt.MessageID)
	}
	harness.waitConsumerEmpty(t, "dlq-worker")
	harness.waitOutboxEmpty(t)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if count := harness.businessCount(t); count != 0 {
		t.Fatalf("business counter after permanent rollback = %d, want 0", count)
	}
	if err := stopService(t, consumerRunner); err != nil {
		t.Fatalf("stop consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestDLQReplayUsesBrokerDeduplicationAndInboxSuppression(t *testing.T) {
	harness := newTestHarness(t)
	if _, err := harness.stage(t, 46, nil); err != nil {
		t.Fatalf("stage command: %v", err)
	}
	dlqSubscription, err := harness.relayConn.SubscribeSync(testDLQ)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := harness.relayConn.Flush(); err != nil {
		t.Fatalf("flush DLQ subscription: %v", err)
	}

	failingConnection := connectNATS(t, harness.server.ClientURL())
	failing := harness.newConsumer(
		t,
		failingConnection,
		"dlq-replay-worker",
		func(context.Context, messenger.Message[processPayload]) error {
			return messenger.Permanent(errors.New("unsupported job"))
		},
		nil,
	)
	failingRunner := startConsumer(t, failing, false)
	outboxRunner := harness.startOutbox(t)
	dlqMessage, err := dlqSubscription.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	record, err := natsadapter.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ record: %v", err)
	}
	harness.waitConsumerEmpty(t, "dlq-replay-worker")
	if err := stopService(t, failingRunner); err != nil {
		t.Fatalf("stop failing consumer: %v", err)
	}

	replacementConnection := connectNATS(t, harness.server.ClientURL())
	var calls atomic.Int32
	replacement := harness.newConsumer(
		t,
		replacementConnection,
		"dlq-replay-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			calls.Add(1)
			return harness.incrementBusiness(ctx)
		},
		nil,
	)
	replacementRunner := startConsumer(t, replacement, false)
	js, err := jetstream.New(harness.relayConn)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	first, err := natsadapter.ReplayDLQ(t.Context(), js, record)
	if err != nil || first.Duplicate {
		t.Fatalf("first replay = %#v, %v", first, err)
	}
	second, err := natsadapter.ReplayDLQ(t.Context(), js, record)
	if err != nil || !second.Duplicate || second.Plan.ReplayID != first.Plan.ReplayID {
		t.Fatalf("second replay = %#v, %v", second, err)
	}
	harness.waitConsumerEmpty(t, "dlq-replay-worker")

	original, err := base64.StdEncoding.DecodeString(record.OriginalBase64)
	if err != nil {
		t.Fatalf("decode original wire data: %v", err)
	}
	headers := natsio.Header(record.OriginalHeaders)
	headers.Del(natsio.MsgIdHdr)
	if _, err := js.PublishMsg(t.Context(), &natsio.Msg{
		Subject: record.Subject,
		Header:  headers,
		Data:    original,
	}, jetstream.WithMsgID("distinct-broker-delivery")); err != nil {
		t.Fatalf("publish distinct delivery: %v", err)
	}
	harness.waitConsumerEmpty(t, "dlq-replay-worker")
	if got := calls.Load(); got != 1 {
		t.Fatalf("replay handler calls = %d, want 1", got)
	}
	if count := harness.businessCount(t); count != 1 {
		t.Fatalf("business counter = %d, want 1", count)
	}
	if duplicates := harness.duplicates.Load(); duplicates != 1 {
		t.Fatalf("inbox duplicates = %d, want 1", duplicates)
	}
	if err := stopService(t, replacementRunner); err != nil {
		t.Fatalf("stop replacement consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func TestTransactionalPipelineDrainRedeliversUncommittedWork(t *testing.T) {
	harness := newTestHarness(t)
	if _, err := harness.stage(t, 45, nil); err != nil {
		t.Fatalf("stage command: %v", err)
	}

	firstConnection := connectNATS(t, harness.server.ClientURL())
	started := make(chan struct{}, 1)
	var calls atomic.Int32
	first := harness.newConsumer(
		t,
		firstConnection,
		"drain-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			calls.Add(1)
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
		func(config *natsadapter.HandlerConfig) {
			config.Timeout = 2 * time.Second
			config.AckWait = 200 * time.Millisecond
		},
	)
	firstRunner := startConsumer(t, first, false)
	outboxRunner := harness.startOutbox(t)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not start")
	}
	first.BeginDrain()
	if err := first.Readiness(t.Context()); err == nil {
		t.Fatal("draining consumer remained ready")
	}
	shutdownContext, cancelShutdown := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err := first.Shutdown(shutdownContext)
	cancelShutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded shutdown = %v, want deadline exceeded", err)
	}
	if err := stopService(t, firstRunner); err != nil {
		t.Fatalf("stop first consumer: %v", err)
	}

	secondConnection := connectNATS(t, harness.server.ClientURL())
	second := harness.newConsumer(
		t,
		secondConnection,
		"drain-worker",
		func(ctx context.Context, _ messenger.Message[processPayload]) error {
			calls.Add(1)
			return harness.incrementBusiness(ctx)
		},
		func(config *natsadapter.HandlerConfig) {
			config.Timeout = 2 * time.Second
			config.AckWait = 200 * time.Millisecond
		},
	)
	secondRunner := startConsumer(t, second, false)
	harness.waitConsumerEmpty(t, "drain-worker")
	harness.waitOutboxEmpty(t)
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
	if count := harness.businessCount(t); count != 1 {
		t.Fatalf("business counter = %d, want 1", count)
	}
	if err := stopService(t, secondRunner); err != nil {
		t.Fatalf("stop replacement consumer: %v", err)
	}
	if err := stopService(t, outboxRunner); err != nil {
		t.Fatalf("stop outbox: %v", err)
	}
}

func receiveTime(t *testing.T, values <-chan time.Time) time.Time {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler attempt")
		return time.Time{}
	}
}
