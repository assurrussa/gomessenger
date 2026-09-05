package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testBatchSource          = "urn:test"
	testBatchName            = "batch.test"
	testBatchTopic           = "test.topic"
	testBatchContentType     = "application/json"
	testKafkaTxFailedMessage = "Kafka transaction failed"
)

type kafkaBatchTestBackend struct {
	topLevelErr error
}

func (b *kafkaBatchTestBackend) Process(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, handler(ctx)
}

func (b *kafkaBatchTestBackend) ProcessAttempt(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	_ uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{Attempt: 1}, handler(ctx)
}

func (*kafkaBatchTestBackend) ForgetAttempt(context.Context, inbox.Key, inbox.Fingerprint) error {
	return nil
}

func (*kafkaBatchTestBackend) Prune(context.Context, time.Time, int) (int64, error) { return 0, nil }

func (b *kafkaBatchTestBackend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	_ uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	if b.topLevelErr != nil {
		return inbox.BatchProcessResult{}, b.topLevelErr
	}
	result, err := handler(ctx, items)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	expected := make([]messenger.BatchItemKey, len(items))
	for index, item := range items {
		expected[index] = messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
	}
	itemErrors, err := batchruntime.ValidateResult(expected, result)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	report := inbox.BatchProcessResult{Items: make([]inbox.BatchItemOutcome, len(items)), HandlerMessages: len(items)}
	for index, item := range items {
		kind, delay := batchruntime.Classify(itemErrors[index])
		outcome := inbox.BatchItemOutcome{
			Key: item.Key, Fingerprint: item.Fingerprint,
			Outcome: inbox.BatchRetry, Attempt: 1, Delay: delay, Err: itemErrors[index],
		}
		switch kind {
		case batchruntime.FailureSuccess:
			outcome.Outcome = inbox.BatchACK
		case batchruntime.FailureDefer:
			outcome.Outcome = inbox.BatchDefer
			outcome.Attempt = 0
		case batchruntime.FailurePermanent:
			outcome.Outcome = inbox.BatchDLQ
			outcome.FailureKind = inbox.FailurePermanent
		case batchruntime.FailureRetryAfter, batchruntime.FailureOrdinary:
		}
		report.Items[index] = outcome
	}
	return report, nil
}

type kafkaBatchSessionRecorder struct {
	consumerSessionRecorder
	produced          []*kgo.Record
	beginErr          error
	produceErr        error
	abortErr          error
	endErr            error
	uncommittedResult bool
	endHook           func()
}

func (s *kafkaBatchSessionRecorder) Begin() error {
	if s.beginErr != nil {
		s.beginCalls.Add(1)
		return s.beginErr
	}
	return s.consumerSessionRecorder.Begin()
}

func (s *kafkaBatchSessionRecorder) End(ctx context.Context, try kgo.TransactionEndTry) (bool, error) {
	if s.endHook != nil {
		s.endHook()
	}
	if try == kgo.TryAbort {
		if s.abortErr != nil {
			return false, s.abortErr
		}
		return false, nil
	}
	if s.endErr != nil {
		s.endCalls.Add(1)
		return false, s.endErr
	}
	if s.uncommittedResult {
		s.endCalls.Add(1)
		return false, nil
	}
	return s.consumerSessionRecorder.End(ctx, try)
}

func (s *kafkaBatchSessionRecorder) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	s.produced = append(s.produced, records...)
	if s.produceErr != nil {
		return kgo.ProduceResults{{Err: s.produceErr}}
	}
	return nil
}

func TestKafkaBatchCommitsOnlySelectedPartitionRange(t *testing.T) {
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	first := kafkaBatchTestRecord(t, consumer, 0, 7, "01991387-6880-7000-8000-000000000081")
	second := kafkaBatchTestRecord(t, consumer, 0, 8, "01991387-6880-7000-8000-000000000082")
	other := kafkaBatchTestRecord(t, consumer, 1, 3, "01991387-6880-7000-8000-000000000083")
	consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		if len(decoded) != 2 || decoded[0].metadata.ID != first.decoded.metadata.ID ||
			decoded[1].metadata.ID != second.decoded.metadata.ID {
			t.Fatalf("handler order = %#v", decoded)
		}
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: messenger.BatchItemKey{
				Source:    second.decoded.metadata.Source,
				MessageID: second.decoded.metadata.ID,
			}, Err: errors.New("retry")},
			{Key: messenger.BatchItemKey{
				Source:    first.decoded.metadata.Source,
				MessageID: first.decoded.metadata.ID,
			}},
		}}, nil
	}
	batch := &kafkaPolledBatch{
		records:         []kafkaBatchRecord{first, second},
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{first.record, other.record, second.record}),
		partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:           first.record, bytes: first.bytes + second.bytes, fillStarted: time.Now(),
	}
	session := &kafkaBatchSessionRecorder{}
	streak := uint64(0)
	if err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak); err != nil {
		t.Fatalf("processKafkaBatch() error = %v", err)
	}
	if session.beginCalls.Load() != 1 || session.endCalls.Load() != 1 {
		t.Fatalf("transaction calls = begin:%d end:%d", session.beginCalls.Load(), session.endCalls.Load())
	}
	if len(session.produced) != 1 || session.produced[0].Topic != consumer.retryTopics[0] {
		t.Fatalf("produced = %#v", session.produced)
	}
	if got := session.committed[consumer.sourceTopic][0].Offset; got != 9 {
		t.Fatalf("selected partition offset = %d, want 9", got)
	}
	if got := session.committed[consumer.sourceTopic][1].Offset; got != 3 {
		t.Fatalf("other partition offset = %d, want rewind 3", got)
	}
	if session.allowRebalanceCalls.Load() != 1 {
		t.Fatalf("AllowRebalance calls after commit = %d, want 1", session.allowRebalanceCalls.Load())
	}
}

func TestKafkaBatchTopLevelErrorPersistsDurableRetryScheduleAcrossSessions(t *testing.T) {
	topErr := messenger.RetryAfter(errors.New("whole batch"), 10*time.Minute)
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{topLevelErr: topErr})
	first := kafkaBatchTestRecord(t, consumer, 0, 17, "01991387-6880-7000-8000-000000000084")
	other := kafkaBatchTestRecord(t, consumer, 1, 4, "01991387-6880-7000-8000-000000000085")
	batch := &kafkaPolledBatch{
		records: []kafkaBatchRecord{first}, earliestOffsets: earliestKafkaOffsets([]*kgo.Record{first.record, other.record}),
		partition: topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:     first.record, bytes: first.bytes, fillStarted: time.Now(),
	}
	session1 := &kafkaBatchSessionRecorder{}
	streak := uint64(0)
	if err := consumer.processKafkaBatch(t.Context(), session1, newRetryPartitionScheduler(), batch, &streak); err != nil {
		t.Fatalf("processKafkaBatch() error = %v", err)
	}
	if session1.beginCalls.Load() != 1 || session1.endCalls.Load() != 1 {
		t.Fatalf("top-level error transaction calls = begin:%d end:%d, want 1 each",
			session1.beginCalls.Load(), session1.endCalls.Load())
	}
	if len(session1.produced) != 1 || session1.produced[0].Topic != consumer.retryTopics[0] {
		t.Fatalf("produced = %#v, want 1 record on retry topic", session1.produced)
	}
	if got := session1.committed[consumer.sourceTopic][0].Offset; got != 18 {
		t.Fatalf("selected partition committed offset = %d, want 18", got)
	}
	if got := session1.committed[consumer.sourceTopic][1].Offset; got != 4 {
		t.Fatalf("other partition rewind = %d, want 4", got)
	}
	if streak != 1 {
		t.Fatalf("top-level streak = %d, want 1", streak)
	}

	retryRecord := session1.produced[0]
	// Verify that when session1 and its scheduler are lost, a new session and scheduler
	// with the persisted retry record observe the future not-before, pause the partition, and release when due.
	scheduler2 := newRetryPartitionScheduler()
	polledRetryRecord := &kgo.Record{
		Topic: retryRecord.Topic, Partition: 0, Offset: 0,
		Key: retryRecord.Key, Value: retryRecord.Value,
		Headers: retryRecord.Headers, Timestamp: retryRecord.Timestamp,
	}
	batch2 := &kafkaPolledBatch{
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{polledRetryRecord}), first: polledRetryRecord,
		partition:   topicPartition{topic: polledRetryRecord.Topic, partition: 0},
		fillStarted: time.Now(),
	}
	consumer.selectKafkaBatchRecords(batch2, []*kgo.Record{polledRetryRecord})
	if len(batch2.records) != 0 {
		t.Fatalf("new session admitted %d records, want 0 (deferred)", len(batch2.records))
	}
	if batch2.firstDeferred == nil {
		t.Fatal("new session did not mark firstDeferred")
	}
	if !batch2.deferUntil.After(time.Now().Add(9 * time.Minute)) {
		t.Fatalf("new session deferUntil = %v, want > 9m from now", batch2.deferUntil)
	}
	session2 := &kafkaBatchSessionRecorder{}
	if err := consumer.pauseKafkaBatchPartition(session2, scheduler2,
		batch2.firstDeferred, batch2.deferUntil); err != nil {
		t.Fatalf("pauseKafkaBatchPartition() error = %v", err)
	}
	if len(session2.pausedPartitions[retryRecord.Topic]) != 1 {
		t.Fatalf("session2 paused partitions = %#v, want partition 0 paused", session2.pausedPartitions)
	}
	if due := scheduler2.releaseDue(batch2.deferUntil.Add(-time.Second)); len(due) != 0 {
		t.Fatalf("releaseDue before deadline = %#v, want empty", due)
	}
	due := scheduler2.releaseDue(batch2.deferUntil.Add(time.Second))
	if len(due) != 1 || len(due[retryRecord.Topic]) != 1 || due[retryRecord.Topic][0] != 0 {
		t.Fatalf("releaseDue after deadline = %#v, want partition 0", due)
	}
}

func TestKafkaBatchDecodedMessagesMatchActiveFingerprint(t *testing.T) {
	messageID, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000086")
	if err != nil {
		t.Fatal(err)
	}
	key := inbox.Key{ConsumerID: testConsumerID, Source: testBatchSource, MessageID: messageID}
	first := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("first"))}
	active := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("active"))}
	decoded, err := kafkaBatchDecodedMessages([]inbox.BatchItem{active}, map[inbox.BatchItem]decodedMessage{
		first:  {canonical: []byte("first")},
		active: {canonical: []byte("active")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(decoded[0].canonical); got != "active" {
		t.Fatalf("decoded payload = %q, want active fingerprint payload", got)
	}
}

func newKafkaBatchTestConsumer(t *testing.T, backend inbox.Backend) *Consumer {
	t.Helper()
	store, err := inbox.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	return &Consumer{
		transport: &Transport{config: TransportConfig{OperationTimeout: time.Second}},
		store:     store,
		config: HandlerConfig{
			ConsumerID: testConsumerID, Timeout: time.Second,
			FinalizationTimeout: time.Second, MaxAttempts: 3, BaseRetry: time.Millisecond,
			MaxRetry: time.Second, RetryTiers: []time.Duration{time.Second},
			Propagator: messenger.NoopContextPropagator(),
		},
		descriptor: messenger.DescriptorInfo{
			Kind: messenger.KindEvent, Name: testBatchName, SchemaVersion: 1,
			ContentType: testBatchContentType, DataEncoding: messenger.DataJSON,
		},
		sourceTopic: testSourceTopic, replayTopic: testSourceTopic + ".replay",
		retryTopics: []string{testSourceTopic + ".retry"},
		retrySet:    map[string]struct{}{testSourceTopic + ".retry": {}},
		dlqTopic:    testSourceTopic + ".dlq", clock: time.Now,
		decode: func(pe preparedEnvelope) (decodedMessage, error) {
			metadata := pe.envelope.Metadata()
			payloadBytes, encoding, err := pe.envelope.Payload()
			if err != nil {
				return decodedMessage{}, err
			}
			canonical, err := messenger.MarshalEnvelope(metadata, payloadBytes, encoding)
			if err != nil {
				return decodedMessage{}, err
			}
			return decodedMessage{
				metadata:  metadata,
				canonical: canonical,
				value:     messenger.Message[any]{Metadata: metadata, Payload: payloadBytes},
			}, nil
		},
		batch: &kafkaBatchConsumer{config: messenger.BatchConfig{
			MaxMessages: 100, MaxBytes: messenger.DefaultBatchMaxBytes, MaxWait: time.Millisecond,
		}},
		drain: make(chan struct{}),
	}
}

func kafkaBatchTestRecord(
	t *testing.T,
	consumer *Consumer,
	partition int32,
	offset int64,
	id string,
) kafkaBatchRecord {
	t.Helper()
	messageID, err := messenger.ParseMessageID(id)
	if err != nil {
		t.Fatal(err)
	}
	metadata := messenger.Metadata{
		ID: messageID, CorrelationID: messageID, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testBatchContentType,
	}
	canonical, err := messenger.MarshalEnvelope(metadata, []byte(`"test"`), messenger.DataJSON)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{
		Topic: consumer.sourceTopic, Partition: partition, Offset: offset,
		LeaderEpoch: 1, Key: []byte(id), Value: canonical,
	}
	return kafkaBatchRecord{
		record:   record,
		prepared: preparedRecord{control: sourceControl(record), observedAt: time.Now()},
		decoded: decodedMessage{
			metadata: metadata, canonical: canonical,
			value: messenger.Message[int]{Metadata: metadata, Payload: int(offset)},
		},
		bytes: len(canonical),
	}
}

func TestKafkaBatchSelectionSplitsIndependentAttemptGenerations(t *testing.T) {
	t.Parallel()
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	messageID, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	metadata := messenger.Metadata{
		ID: messageID, CorrelationID: messageID, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testBatchContentType,
	}
	native, err := messenger.MarshalEnvelope(metadata, []byte("1"), messenger.DataJSON)
	if err != nil {
		t.Fatal(err)
	}

	consumer.decode = func(_ preparedEnvelope) (decodedMessage, error) {
		return decodedMessage{
			metadata:  metadata,
			canonical: native,
			value:     messenger.Message[int]{Metadata: metadata, Payload: 1},
		}, nil
	}

	ctrl1 := controlMetadata{
		source:            sourcePosition{topic: consumer.sourceTopic, partition: 0, offset: 10},
		attempt:           1,
		attemptGeneration: replayIDPrefix + "gen1",
	}
	ctrl2 := controlMetadata{
		source:            sourcePosition{topic: consumer.sourceTopic, partition: 0, offset: 10},
		attempt:           1,
		attemptGeneration: replayIDPrefix + "gen2",
	}

	rec1 := &kgo.Record{
		Topic: consumer.replayTopic, Partition: 0, Offset: 10,
		Key: []byte(messageID.String()), Value: native,
		Headers: controlHeaders(ctrl1),
	}
	rec2 := &kgo.Record{
		Topic: consumer.replayTopic, Partition: 0, Offset: 11,
		Key: []byte(messageID.String()), Value: native,
		Headers: controlHeaders(ctrl2),
	}

	batch := &kafkaPolledBatch{
		partition: topicPartition{topic: consumer.replayTopic, partition: 0},
	}
	consumer.selectKafkaBatchRecords(batch, []*kgo.Record{rec1, rec2})

	if len(batch.records) != 1 {
		t.Fatalf("selected records count = %d, want 1", len(batch.records))
	}
	if !batch.selectionStopped {
		t.Fatal("batch.selectionStopped is false, want true")
	}
	if batch.firstUnprocessed != rec2 {
		t.Fatalf("firstUnprocessed = %v, want rec2", batch.firstUnprocessed)
	}
}

func TestKafkaBatchReadinessReportsNotReadyDuringWorkerBackoff(t *testing.T) {
	consumer := &Consumer{
		config: HandlerConfig{Concurrency: 2},
		state:  consumerRunning,
	}
	consumer.setWorkerReady(0, true)
	consumer.setWorkerReady(1, true)
	if err := consumer.Readiness(t.Context()); err != nil {
		t.Fatalf("Readiness initially = %v, want nil", err)
	}

	// Worker 0 enters backoff
	backoffErr := errors.New("recoverable session error")
	consumer.setKafkaBatchBackoff(0, backoffErr, true)
	if err := consumer.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("Readiness during backoff = %v, want ErrRuntimeNotRunning", err)
	}

	// Worker 0 exits backoff and re-establishes readiness
	consumer.setKafkaBatchBackoff(0, nil, false)
	consumer.setWorkerReady(0, true)
	if err := consumer.Readiness(t.Context()); err != nil {
		t.Fatalf("Readiness after backoff recovered = %v, want nil", err)
	}
}

func TestKafkaBatchRewindAllPartitionsOnDeferred(t *testing.T) {
	recordP0 := &kgo.Record{Topic: testBatchTopic, Partition: 0, Offset: 10, LeaderEpoch: 1}
	recordP1 := &kgo.Record{Topic: testBatchTopic, Partition: 1, Offset: 20, LeaderEpoch: 1}
	recordP0Later := &kgo.Record{Topic: testBatchTopic, Partition: 0, Offset: 15, LeaderEpoch: 1}

	batch := &kafkaPolledBatch{
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{recordP0, recordP1, recordP0Later}),
	}
	offsets := cloneKafkaOffsets(batch.earliestOffsets)
	if offsets[testBatchTopic][0].Offset != 10 {
		t.Fatalf("p0 offset = %d, want 10", offsets[testBatchTopic][0].Offset)
	}
	if offsets[testBatchTopic][1].Offset != 20 {
		t.Fatalf("p1 offset = %d, want 20", offsets[testBatchTopic][1].Offset)
	}
}

type testKafkaBatchObserver struct {
	mu            sync.Mutex
	observations  []messenger.Observation
	onObservation func(messenger.Observation)
}

func (o *testKafkaBatchObserver) Observe(_ context.Context, obs messenger.Observation) {
	o.mu.Lock()
	o.observations = append(o.observations, obs)
	cb := o.onObservation
	o.mu.Unlock()
	if cb != nil {
		cb(obs)
	}
}

func (o *testKafkaBatchObserver) last() messenger.Observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.observations) == 0 {
		return messenger.Observation{}
	}
	return o.observations[len(o.observations)-1]
}

//nolint:gocognit // The Kafka batch observability suite tests all top-level error scenarios.
func TestProcessKafkaBatchObservability(t *testing.T) {
	setup := func(
		t *testing.T,
		topLevelErr error,
	) (*Consumer, *kafkaBatchSessionRecorder, *kafkaPolledBatch, *testKafkaBatchObserver) {
		t.Helper()
		backend := &kafkaBatchTestBackend{topLevelErr: topLevelErr}
		consumer := newKafkaBatchTestConsumer(t, backend)
		obs := &testKafkaBatchObserver{}
		consumer.config.Observers = []messenger.Observer{obs}
		rec1 := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000091")
		rec2 := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-000000000092")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{rec1, rec2},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{rec1.record, rec2.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           rec1.record,
			bytes:           rec1.bytes + rec2.bytes,
			fillStarted:     time.Now(),
		}
		session := &kafkaBatchSessionRecorder{}
		return consumer, session, batch, obs
	}

	t.Run("top-level ordinary error", func(t *testing.T) {
		consumer, session, batch, obs := setup(t, errors.New("database failure"))
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err != nil {
			t.Fatalf("expected nil error on durable retry commit, got %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchRetries != 2 {
			t.Fatalf("BatchRetries = %d, want 2", o.BatchRetries)
		}
		if o.RetryDelay <= 0 {
			t.Fatalf("RetryDelay = %v, want > 0", o.RetryDelay)
		}
		if o.BatchACKs != 0 || o.BatchDeferrals != 0 {
			t.Fatalf("unexpected ACKs=%d or Deferrals=%d", o.BatchACKs, o.BatchDeferrals)
		}
	})

	t.Run("top-level RetryAfter", func(t *testing.T) {
		consumer, session, batch, obs := setup(t, messenger.RetryAfter(errors.New("retry error"), 5*time.Second))
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err != nil {
			t.Fatalf("expected nil error on durable retry commit, got %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchRetries != 2 {
			t.Fatalf("BatchRetries = %d, want 2", o.BatchRetries)
		}
		if o.RetryDelay != 5*time.Second {
			t.Fatalf("RetryDelay = %v, want 5s", o.RetryDelay)
		}
	})

	t.Run("top-level DeferAfter", func(t *testing.T) {
		consumer, session, batch, obs := setup(t, messenger.DeferAfter(errors.New("defer error"), 3*time.Second))
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err != nil {
			t.Fatalf("expected nil error on durable retry commit, got %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchDeferrals != 2 {
			t.Fatalf("BatchDeferrals = %d, want 2", o.BatchDeferrals)
		}
		if o.BatchRetries != 0 {
			t.Fatalf("BatchRetries = %d, want 0", o.BatchRetries)
		}
		if o.RetryDelay != 3*time.Second {
			t.Fatalf("RetryDelay = %v, want 3s", o.RetryDelay)
		}
	})

	t.Run("failed scheduling of retry", func(t *testing.T) {
		consumer, session, batch, obs := setup(t, errors.New("handler error"))
		session.endErr = errors.New("commit failure")
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected error on failed transaction commit, got nil")
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if !errors.Is(o.Err, err) {
			t.Fatalf("expected observation Err to contain finalization error %v, got %v", err, o.Err)
		}
	})
}

func TestKafkaBatchCollectStopsPollingWhenSelectionStopped(t *testing.T) {
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	messageID1, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	messageID2, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000002")
	if err != nil {
		t.Fatal(err)
	}
	meta1 := messenger.Metadata{
		ID: messageID1, CorrelationID: messageID1, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testBatchContentType,
	}
	meta2 := messenger.Metadata{
		ID: messageID2, CorrelationID: messageID2, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testBatchContentType,
	}
	native1, err := messenger.MarshalEnvelope(meta1, []byte(`"large payload 1"`), messenger.DataJSON)
	if err != nil {
		t.Fatal(err)
	}
	native2, err := messenger.MarshalEnvelope(meta2, []byte(`"large payload 2"`), messenger.DataJSON)
	if err != nil {
		t.Fatal(err)
	}
	consumer.decode = func(pe preparedEnvelope) (decodedMessage, error) {
		meta := pe.envelope.Metadata()
		canonical := native1
		if meta.ID == messageID2 {
			canonical = native2
		}
		return decodedMessage{
			metadata:  meta,
			canonical: canonical,
			value:     messenger.Message[int]{Metadata: meta, Payload: 1},
		}, nil
	}

	consumer.batch.config.MaxBytes = len(native1) + 5
	consumer.batch.config.MaxMessages = 10
	consumer.batch.config.MaxWait = 5 * time.Second

	rec1 := &kgo.Record{
		Topic: consumer.sourceTopic, Partition: 0, Offset: 10,
		Key: []byte(messageID1.String()), Value: native1,
	}
	rec2 := &kgo.Record{
		Topic: consumer.sourceTopic, Partition: 0, Offset: 11,
		Key: []byte(messageID2.String()), Value: native2,
	}

	var pollCalls atomic.Int32
	session := &consumerSessionRecorder{
		poll: func(_ context.Context, _ int) kgo.Fetches {
			calls := pollCalls.Add(1)
			if calls > 1 {
				t.Fatalf("unexpected PollRecords call %d after batch selection stopped", calls)
			}
			fetch := kgo.FetchTopic{
				Topic: consumer.sourceTopic,
				Partitions: []kgo.FetchPartition{
					{
						Partition: 0,
						Records:   []*kgo.Record{rec1, rec2},
					},
				},
			}
			return kgo.Fetches{{Topics: []kgo.FetchTopic{fetch}}}
		},
	}

	batch, err := consumer.collectKafkaBatch(t.Context(), session, time.Second)
	if err != nil {
		t.Fatalf("collectKafkaBatch() error = %v", err)
	}
	if calls := pollCalls.Load(); calls != 1 {
		t.Fatalf("poll calls = %d, want 1", calls)
	}
	if len(batch.records) != 1 {
		t.Fatalf("selected records = %d, want 1", len(batch.records))
	}
	if !batch.selectionStopped {
		t.Fatal("batch.selectionStopped is false, want true")
	}
	if batch.firstUnprocessed != rec2 {
		t.Fatalf("firstUnprocessed = %v, want rec2", batch.firstUnprocessed)
	}
}

func TestKafkaBatchFinalizationLatencyIncludesTransactionDuration(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	consumer.clock = func() time.Time { return now }
	consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
		}}, nil
	}

	obs := &testKafkaBatchObserver{}
	consumer.config.Observers = []messenger.Observer{obs}

	session := &kafkaBatchSessionRecorder{
		endHook: func() {
			now = now.Add(150 * time.Millisecond)
		},
	}

	rec := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000095")
	batch := &kafkaPolledBatch{
		records:         []kafkaBatchRecord{rec},
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{rec.record}),
		partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:           rec.record,
		bytes:           rec.bytes,
		fillStarted:     now,
	}

	streak := uint64(0)
	err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
	if err != nil {
		t.Fatalf("processKafkaBatch() error = %v", err)
	}

	var commitObs *messenger.Observation
	obs.mu.Lock()
	for i := range obs.observations {
		if obs.observations[i].Operation == messenger.Operation("offset_commit") {
			commitObs = &obs.observations[i]
			break
		}
	}
	obs.mu.Unlock()

	if commitObs == nil {
		t.Fatal("expected offset_commit observation")
	}
	if commitObs.Duration != 150*time.Millisecond {
		t.Fatalf("offset_commit Duration = %v, want 150ms", commitObs.Duration)
	}
}

func TestKafkaBatchObservationErrPopulatedOnHandoffError(t *testing.T) {
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	obs := &testKafkaBatchObserver{}
	consumer.config.Observers = []messenger.Observer{obs}

	rec := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000098")
	rec.decoded.canonical = nil
	consumer.batch.invoke = func(_ context.Context, _ []decodedMessage) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{
				Key: messenger.BatchItemKey{Source: rec.decoded.metadata.Source, MessageID: rec.decoded.metadata.ID},
				Err: errors.New("retry me"),
			},
		}}, nil
	}
	batch := &kafkaPolledBatch{
		records:         []kafkaBatchRecord{rec},
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{rec.record}),
		partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:           rec.record,
		bytes:           rec.bytes,
		fillStarted:     time.Now(),
	}

	session := &kafkaBatchSessionRecorder{}
	streak := uint64(0)
	err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
	if err == nil {
		t.Fatal("expected error due to empty canonical envelope in retry")
	}
	o := obs.last()
	if o.Err == nil {
		t.Fatal("expected non-nil observation Err")
	}
	if !errors.Is(o.Err, err) {
		t.Fatalf("observation Err = %v, want %v", o.Err, err)
	}
}

func TestKafkaBatchCollectBoundsMemoryAndPollsOnAlienPartitions(t *testing.T) {
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	consumer.batch.config.MaxMessages = 100
	consumer.batch.config.MaxBytes = 10 * 1024 * 1024
	consumer.batch.config.MaxWait = 5 * time.Second

	msgID0, _ := messenger.ParseMessageID("01991387-6880-7000-8000-000000000010")
	meta0 := messenger.Metadata{
		ID: msgID0, CorrelationID: msgID0, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testBatchContentType,
	}
	val0, _ := messenger.MarshalEnvelope(meta0, []byte(`"p0"`), messenger.DataJSON)
	recP0 := &kgo.Record{
		Topic: consumer.sourceTopic, Partition: 0, Offset: 10,
		Key: []byte(msgID0.String()), Value: val0, LeaderEpoch: 1,
	}

	makeAlienRecords := func(startOffset int64, count int) []*kgo.Record {
		records := make([]*kgo.Record, count)
		for i := 0; i < count; i++ {
			records[i] = &kgo.Record{
				Topic: consumer.sourceTopic, Partition: 1, Offset: startOffset + int64(i),
				Key: []byte(fmt.Sprintf("alien-%d", i)), Value: []byte(`"alien"`), LeaderEpoch: 1,
			}
		}
		return records
	}

	var pollCalls atomic.Int32
	session := &consumerSessionRecorder{
		poll: func(_ context.Context, _ int) kgo.Fetches {
			call := pollCalls.Add(1)
			switch call {
			case 1:
				// Poll 1: 1 record from partition 0, 99 records from partition 1
				alien := makeAlienRecords(100, 99)
				return kgo.Fetches{{Topics: []kgo.FetchTopic{{
					Topic: consumer.sourceTopic,
					Partitions: []kgo.FetchPartition{
						{Partition: 0, Records: []*kgo.Record{recP0}},
						{Partition: 1, Records: alien},
					},
				}}}}
			case 2:
				// Poll 2: 99 records from partition 1 only (no partition 0 records)
				alien := makeAlienRecords(200, 99)
				return kgo.Fetches{{Topics: []kgo.FetchTopic{{
					Topic: consumer.sourceTopic,
					Partitions: []kgo.FetchPartition{
						{Partition: 1, Records: alien},
					},
				}}}}
			default:
				t.Fatalf("unexpected PollRecords call %d; fill loop did not terminate when alien poll arrived", call)
				return nil
			}
		},
	}

	batch, err := consumer.collectKafkaBatch(t.Context(), session, time.Second)
	if err != nil {
		t.Fatalf("collectKafkaBatch() error = %v", err)
	}
	if calls := pollCalls.Load(); calls != 2 {
		t.Fatalf("poll calls = %d, want exactly 2", calls)
	}
	if len(batch.records) != 1 {
		t.Fatalf("batch.records count = %d, want 1", len(batch.records))
	}
	if batch.partition.partition != 0 {
		t.Fatalf("batch partition = %d, want 0", batch.partition.partition)
	}
	if batch.earliestOffsets[consumer.sourceTopic][0].Offset != 10 {
		t.Fatalf("earliest offset p0 = %d, want 10", batch.earliestOffsets[consumer.sourceTopic][0].Offset)
	}
	if batch.earliestOffsets[consumer.sourceTopic][1].Offset != 100 {
		t.Fatalf("earliest offset p1 = %d, want 100", batch.earliestOffsets[consumer.sourceTopic][1].Offset)
	}
}

func TestKafkaBatchConsumerRebalanceTimeout(t *testing.T) {
	t.Run("calculates full worst-case budget with safety margin", func(t *testing.T) {
		got, err := batchConsumerRebalanceTimeout(
			5*time.Second,
			90*time.Second,
			20*time.Second,
			10*time.Second,
		)
		if err != nil {
			t.Fatalf("unexpected error = %v", err)
		}
		// 5s fill + 90s handler + 20s sql + 2*10s broker op + 5s margin = 140s
		want := 140 * time.Second
		if got != want {
			t.Fatalf("rebalanceTimeout = %v, want %v", got, want)
		}
	})

	t.Run("clamps to defaultConsumerRebalanceTimeout when small", func(t *testing.T) {
		got, err := batchConsumerRebalanceTimeout(
			100*time.Millisecond,
			time.Second,
			time.Second,
			time.Second,
		)
		if err != nil {
			t.Fatalf("unexpected error = %v", err)
		}
		if got != defaultConsumerRebalanceTimeout {
			t.Fatalf("rebalanceTimeout = %v, want %v", got, defaultConsumerRebalanceTimeout)
		}
	})

	t.Run("rejects negative durations", func(t *testing.T) {
		_, err := batchConsumerRebalanceTimeout(-time.Second, time.Second, time.Second, time.Second)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("expected ErrInvalidConfig on negative duration, got %v", err)
		}
	})

	t.Run("rejects durations exceeding Kafka wire limit", func(t *testing.T) {
		_, err := batchConsumerRebalanceTimeout(
			maxKafkaRebalanceTimeout+time.Second,
			time.Second,
			time.Second,
			time.Second,
		)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("expected ErrInvalidConfig on wire limit exceeded, got %v", err)
		}
	})

	t.Run("rejects sum overflow exceeding Kafka wire limit", func(t *testing.T) {
		_, err := batchConsumerRebalanceTimeout(
			maxKafkaRebalanceTimeout/2,
			maxKafkaRebalanceTimeout/2+time.Hour,
			time.Second,
			time.Second,
		)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("expected ErrInvalidConfig on sum overflow, got %v", err)
		}
	})
}

//nolint:gocognit,gocyclo // The suite comprehensively tests all transaction failure branches.
func TestProcessKafkaBatchTransactionFailures(t *testing.T) {
	setup := func(t *testing.T) (
		*Consumer,
		*kafkaBatchSessionRecorder,
		*kafkaPolledBatch,
		*testKafkaBatchObserver,
		*kafkaLogRecorder,
	) {
		t.Helper()
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		logger := &kafkaLogRecorder{}
		consumer.transport.config.Logger = logger
		obs := &testKafkaBatchObserver{}
		consumer.config.Observers = []messenger.Observer{obs}

		rec1 := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-0000000000a1")
		rec2 := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-0000000000a2")

		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{Items: []messenger.BatchItemResult{
				{Key: messenger.BatchItemKey{
					Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID,
				}},
				{
					Key: messenger.BatchItemKey{
						Source: decoded[1].metadata.Source, MessageID: decoded[1].metadata.ID,
					},
					Err: errors.New("retry error"),
				},
			}}, nil
		}

		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{rec1, rec2},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{rec1.record, rec2.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           rec1.record,
			bytes:           rec1.bytes + rec2.bytes,
			fillStarted:     time.Now(),
		}
		session := &kafkaBatchSessionRecorder{}
		return consumer, session, batch, obs, logger
	}

	findObservations := func(obs *testKafkaBatchObserver, op messenger.Operation) []messenger.Observation {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		var found []messenger.Observation
		for _, o := range obs.observations {
			if o.Operation == op {
				found = append(found, o)
			}
		}
		return found
	}

	t.Run("Begin error", func(t *testing.T) {
		consumer, session, batch, obs, logger := setup(t)
		beginErr := errors.New("kafka broker begin failed")
		session.beginErr = beginErr

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected error from processKafkaBatch on begin error, got nil")
		}

		handles := findObservations(obs, messenger.OperationHandle)
		if len(handles) != 2 {
			t.Fatalf("OperationHandle observations = %d, want 2", len(handles))
		}
		if handles[0].Err != nil {
			t.Fatalf("handle[0] Err = %v, want nil (ACK)", handles[0].Err)
		}
		if handles[1].Err == nil {
			t.Fatal("handle[1] Err is nil, want handler error")
		}

		commits := findObservations(obs, messenger.Operation("offset_commit"))
		if len(commits) != 1 || commits[0].Err == nil {
			t.Fatalf("offset_commit observations = %#v, want 1 with error", commits)
		}

		retries := findObservations(obs, messenger.Operation("retry_handoff"))
		if len(retries) != 1 || retries[0].Err == nil {
			t.Fatalf("retry_handoff observations = %#v, want 1 with error", retries)
		}

		batchHandles := findObservations(obs, messenger.OperationBatchHandle)
		if len(batchHandles) != 1 || batchHandles[0].Err == nil {
			t.Fatalf("batch_handle observations = %#v, want 1 with error", batchHandles)
		}

		entries := logger.snapshot()
		if len(entries) == 0 {
			t.Fatal("expected transport log entries, got none")
		}
		var foundLog bool
		for _, entry := range entries {
			if entry.level == messenger.LogError && entry.message == testKafkaTxFailedMessage {
				for _, attr := range entry.attrs {
					if attr.Key == logAttrOperation && attr.Value == "consumer_begin" {
						foundLog = true
					}
				}
			}
		}
		if !foundLog {
			t.Fatalf("expected consumer_begin log error, got logs: %#v", entries)
		}
	})

	t.Run("ProduceSync error with successful abort", func(t *testing.T) {
		consumer, session, batch, obs, logger := setup(t)
		produceErr := errors.New("produce sync failed")
		session.produceErr = produceErr

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected error from processKafkaBatch on produce error, got nil")
		}

		handles := findObservations(obs, messenger.OperationHandle)
		if len(handles) != 2 {
			t.Fatalf("OperationHandle observations = %d, want 2", len(handles))
		}

		commits := findObservations(obs, messenger.Operation("offset_commit"))
		if len(commits) != 1 || commits[0].Err == nil {
			t.Fatalf("offset_commit observations = %#v, want 1 with error", commits)
		}

		retries := findObservations(obs, messenger.Operation("retry_handoff"))
		if len(retries) != 1 || retries[0].Err == nil {
			t.Fatalf("retry_handoff observations = %#v, want 1 with error", retries)
		}

		entries := logger.snapshot()
		var foundLog bool
		for _, entry := range entries {
			if entry.level == messenger.LogError && entry.message == testKafkaTxFailedMessage {
				for _, attr := range entry.attrs {
					if attr.Key == logAttrOperation && attr.Value == "consumer_handoff" {
						foundLog = true
					}
				}
			}
		}
		if !foundLog {
			t.Fatalf("expected consumer_handoff log error, got logs: %#v", entries)
		}
	})

	t.Run("ProduceSync error with failed abort", func(t *testing.T) {
		consumer, session, batch, obs, logger := setup(t)
		session.produceErr = errors.New("produce sync failed")
		session.abortErr = errors.New("abort failed")

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected error from processKafkaBatch on produce+abort error, got nil")
		}

		handles := findObservations(obs, messenger.OperationHandle)
		if len(handles) != 2 {
			t.Fatalf("OperationHandle observations = %d, want 2", len(handles))
		}

		commits := findObservations(obs, messenger.Operation("offset_commit"))
		if len(commits) != 1 || commits[0].Err == nil {
			t.Fatalf("offset_commit observations = %#v, want 1 with error", commits)
		}

		entries := logger.snapshot()
		var foundLog bool
		for _, entry := range entries {
			if entry.level == messenger.LogError && entry.message == testKafkaTxFailedMessage {
				for _, attr := range entry.attrs {
					if attr.Key == logAttrOperation && attr.Value == "consumer_handoff" {
						foundLog = true
					}
				}
			}
		}
		if !foundLog {
			t.Fatalf("expected consumer_handoff log error, got logs: %#v", entries)
		}
	})

	t.Run("End(TryCommit) error", func(t *testing.T) {
		consumer, session, batch, obs, logger := setup(t)
		session.endErr = errors.New("end commit failed")

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected error from processKafkaBatch on commit error, got nil")
		}

		handles := findObservations(obs, messenger.OperationHandle)
		if len(handles) != 2 {
			t.Fatalf("OperationHandle observations = %d, want 2", len(handles))
		}

		commits := findObservations(obs, messenger.Operation("offset_commit"))
		if len(commits) != 1 || commits[0].Err == nil {
			t.Fatalf("offset_commit observations = %#v, want 1 with error", commits)
		}

		entries := logger.snapshot()
		var foundLog bool
		for _, entry := range entries {
			if entry.level == messenger.LogError && entry.message == testKafkaTxFailedMessage {
				for _, attr := range entry.attrs {
					if attr.Key == logAttrOperation && attr.Value == "consumer_commit" {
						foundLog = true
					}
				}
			}
		}
		if !foundLog {
			t.Fatalf("expected consumer_commit log error, got logs: %#v", entries)
		}
	})

	t.Run("End(TryCommit) == false, nil (uncommitted)", func(t *testing.T) {
		consumer, session, batch, obs, logger := setup(t)
		session.uncommittedResult = true

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if !errors.Is(err, errTransactionNotCommitted) {
			t.Fatalf("expected errTransactionNotCommitted, got %v", err)
		}

		handles := findObservations(obs, messenger.OperationHandle)
		if len(handles) != 2 {
			t.Fatalf("OperationHandle observations = %d, want 2", len(handles))
		}

		commits := findObservations(obs, messenger.Operation("offset_commit"))
		if len(commits) != 1 || !errors.Is(commits[0].Err, errTransactionNotCommitted) {
			t.Fatalf("offset_commit observations = %#v, want 1 with errTransactionNotCommitted", commits)
		}

		entries := logger.snapshot()
		var foundLog bool
		for _, entry := range entries {
			if entry.level == messenger.LogWarn && entry.message == "Kafka consumer transaction aborted" {
				for _, attr := range entry.attrs {
					if attr.Key == logAttrOperation && attr.Value == "consumer_commit" {
						foundLog = true
					}
				}
			}
		}
		if !foundLog {
			t.Fatalf("expected consumer_commit log warning for uncommitted transaction, got logs: %#v", entries)
		}
	})
}

type testAdvancingClockObserver struct {
	mu           sync.Mutex
	observations []messenger.Observation
	onObserve    func()
}

func (o *testAdvancingClockObserver) Observe(_ context.Context, obs messenger.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observations = append(o.observations, obs)
	if o.onObserve != nil {
		o.onObserve()
	}
}

func TestKafkaBatchHandoffObservationsUseFixedTransactionDuration(t *testing.T) {
	startTime := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	var currentNanos atomic.Int64
	currentNanos.Store(startTime.UnixNano())
	nowFunc := func() time.Time {
		return time.Unix(0, currentNanos.Load()).UTC()
	}

	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	consumer.clock = nowFunc

	rec1 := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-0000000000b1")
	rec2 := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-0000000000b2")
	rec3 := kafkaBatchTestRecord(t, consumer, 0, 12, "01991387-6880-7000-8000-0000000000b3")

	consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: messenger.BatchItemKey{
				Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID,
			}},
			{
				Key: messenger.BatchItemKey{
					Source: decoded[1].metadata.Source, MessageID: decoded[1].metadata.ID,
				},
				Err: errors.New("retry me"),
			},
			{
				Key: messenger.BatchItemKey{
					Source: decoded[2].metadata.Source, MessageID: decoded[2].metadata.ID,
				},
				Err: messenger.Permanent(errors.New("dlq me")),
			},
		}}, nil
	}

	batch := &kafkaPolledBatch{
		records:         []kafkaBatchRecord{rec1, rec2, rec3},
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{rec1.record, rec2.record, rec3.record}),
		partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:           rec1.record,
		bytes:           rec1.bytes + rec2.bytes + rec3.bytes,
		fillStarted:     nowFunc(),
	}

	// Session will take 100ms during commit
	session := &kafkaBatchSessionRecorder{
		endHook: func() {
			currentNanos.Add(int64(100 * time.Millisecond))
		},
	}

	// Observer will advance clock by 50ms on each observation
	obs := &testAdvancingClockObserver{
		onObserve: func() {
			currentNanos.Add(int64(50 * time.Millisecond))
		},
	}
	consumer.config.Observers = []messenger.Observer{obs}

	streak := uint64(0)
	err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
	if err != nil {
		t.Fatalf("processKafkaBatch() error = %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()

	var commitObs, retryObs, dlqObs *messenger.Observation
	for i := range obs.observations {
		switch obs.observations[i].Operation {
		case messenger.Operation("offset_commit"):
			commitObs = &obs.observations[i]
		case messenger.Operation("retry_handoff"):
			retryObs = &obs.observations[i]
		case messenger.Operation("dlq_handoff"):
			dlqObs = &obs.observations[i]
		default:
		}
	}

	if commitObs == nil || retryObs == nil || dlqObs == nil {
		t.Fatalf("missing observations: commit=%v, retry=%v, dlq=%v", commitObs, retryObs, dlqObs)
	}

	// All 3 boundaries must have EXACTLY the 100ms transaction duration, unaffected by observer clock skew!
	const expectedDuration = 100 * time.Millisecond
	if commitObs.Duration != expectedDuration {
		t.Fatalf("offset_commit Duration = %v, want %v", commitObs.Duration, expectedDuration)
	}
	if retryObs.Duration != expectedDuration {
		t.Fatalf("retry_handoff Duration = %v, want %v", retryObs.Duration, expectedDuration)
	}
	if dlqObs.Duration != expectedDuration {
		t.Fatalf("dlq_handoff Duration = %v, want %v", dlqObs.Duration, expectedDuration)
	}
}

type testKafkaTxMarkerKey struct{}

type delayedBatchTestBackend struct {
	kafkaBatchTestBackend
	delay time.Duration
}

func (b *delayedBatchTestBackend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	maxAttempts uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return inbox.BatchProcessResult{}, ctx.Err()
		}
	}
	txCtx := context.WithValue(ctx, testKafkaTxMarkerKey{}, "sentinel-kafka-tx-marker")
	return b.kafkaBatchTestBackend.ProcessBatchAttempt(txCtx, items, maxAttempts, handler)
}

func TestKafkaBatchHandlerContextInheritsTransactionContextDeadlineAndCancellation(t *testing.T) {
	t.Run("handler inherits transaction context sentinel, deadline, and cancellation", func(t *testing.T) {
		backend := &delayedBatchTestBackend{delay: 90 * time.Millisecond}
		consumer := newKafkaBatchTestConsumer(t, backend)
		consumer.config.Timeout = 80 * time.Millisecond
		consumer.config.FinalizationTimeout = 40 * time.Millisecond

		record := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000091")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{record},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{record.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           record.record,
			bytes:           record.bytes,
			fillStarted:     time.Now(),
		}

		var (
			sawSentinel       bool
			sawDeadline       bool
			deadlineInherited bool
			sawCancellation   bool
		)

		consumer.batch.invoke = func(ctx context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			if val, ok := ctx.Value(testKafkaTxMarkerKey{}).(string); ok && val == "sentinel-kafka-tx-marker" {
				sawSentinel = true
			}
			handlerDeadline, hasDeadline := ctx.Deadline()
			sawDeadline = hasDeadline
			if hasDeadline {
				deadlineInherited = time.Until(handlerDeadline) <= 45*time.Millisecond
			}
			select {
			case <-ctx.Done():
				sawCancellation = true
				return messenger.BatchResult{}, ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			}
		}

		session := &kafkaBatchSessionRecorder{}
		streak := uint64(0)
		_ = consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if !sawSentinel {
			t.Fatal("expected handler context to inherit sentinel value from transactionHandlerCtx")
		}
		if !sawDeadline {
			t.Fatal("expected handler context to have deadline")
		}
		if !deadlineInherited {
			t.Fatal("expected handler deadline to be inherited/bounded by transaction deadline")
		}
		if !sawCancellation {
			t.Fatal("expected transaction cancellation to be visible in handler")
		}
	})
}

type trackingConfirmBackend struct {
	kafkaBatchTestBackend
	confirmCalls atomic.Int32
	mu           sync.Mutex
	confirmKeys  []inbox.Key
	onConfirm    func()
}

func (b *trackingConfirmBackend) ConfirmTerminalHandoff(_ context.Context, key inbox.Key, _ inbox.Fingerprint) error {
	b.confirmCalls.Add(1)
	b.mu.Lock()
	b.confirmKeys = append(b.confirmKeys, key)
	b.mu.Unlock()
	if b.onConfirm != nil {
		b.onConfirm()
	}
	return nil
}

func TestKafkaBatchAllowRebalanceCalledBeforeConfirmTerminalHandoff(t *testing.T) {
	t.Run("AllowRebalance called before ConfirmTerminalHandoff and cleanup deduplicated", func(t *testing.T) {
		backend := &trackingConfirmBackend{}
		consumer := newKafkaBatchTestConsumer(t, backend)
		first := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000092")
		second := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-000000000093")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{first, second},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{first.record, second.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           first.record,
			bytes:           first.bytes + second.bytes,
			fillStarted:     time.Now(),
		}

		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{
						Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID},
						Err: messenger.Permanent(errors.New("terminal 1")),
					},
					{
						Key: messenger.BatchItemKey{Source: decoded[1].metadata.Source, MessageID: decoded[1].metadata.ID},
						Err: messenger.Permanent(errors.New("terminal 2")),
					},
				},
			}, nil
		}

		session := &kafkaBatchSessionRecorder{}
		var allowRebalanceBeforeConfirm bool
		backend.onConfirm = func() {
			if session.allowRebalanceCalls.Load() > 0 {
				allowRebalanceBeforeConfirm = true
			}
		}

		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err != nil {
			t.Fatalf("processKafkaBatch failed: %v", err)
		}
		if !allowRebalanceBeforeConfirm {
			t.Fatal("expected AllowRebalance to be called BEFORE ConfirmTerminalHandoff")
		}
		if backend.confirmCalls.Load() != 2 {
			t.Fatalf("confirmCalls = %d, want 2", backend.confirmCalls.Load())
		}
	})
}

func TestKafkaBatchOperationHandleFilteredForUninvokedItems(t *testing.T) {
	t.Run("backend error before callback produces 0 OperationHandle", func(t *testing.T) {
		obs := &testKafkaBatchObserver{}
		backend := &kafkaBatchTestBackend{topLevelErr: errors.New("backend failed before callback")}
		consumer := newKafkaBatchTestConsumer(t, backend)
		consumer.config.Observers = []messenger.Observer{obs}

		record := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000093")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{record},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{record.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           record.record,
			bytes:           record.bytes,
			fillStarted:     time.Now(),
		}

		session := &kafkaBatchSessionRecorder{}
		streak := uint64(0)
		_ = consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
			}
		}
		if handleCount != 0 {
			t.Fatalf("expected 0 OperationHandle observations on pre-callback error, got %d", handleCount)
		}
	})

	t.Run("mixed expired and active item produces 1 OperationHandle", func(t *testing.T) {
		obs := &testKafkaBatchObserver{}
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		consumer.config.Observers = []messenger.Observer{obs}

		active := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000094")
		expired := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-000000000095")
		expired.decoded.metadata.ExpiresAt = time.Now().Add(-time.Hour)

		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{expired, active},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{expired.record, active.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           expired.record,
			bytes:           expired.bytes + active.bytes,
			fillStarted:     time.Now(),
		}

		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			if len(decoded) != 1 {
				t.Fatalf("expected 1 decoded message in invoke, got %d", len(decoded))
			}
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
				},
			}, nil
		}

		session := &kafkaBatchSessionRecorder{}
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err != nil {
			t.Fatalf("processKafkaBatch failed: %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
			}
		}
		if handleCount != 1 {
			t.Fatalf("expected 1 OperationHandle observation for active item, got %d", handleCount)
		}
	})
}

type watchdogBlockingProduceSession struct {
	kafkaBatchSessionRecorder
	produceStarted chan struct{}
	closed         chan struct{}
	closeCalls     atomic.Int32
}

func newWatchdogBlockingProduceSession() *watchdogBlockingProduceSession {
	return &watchdogBlockingProduceSession{
		produceStarted: make(chan struct{}),
		closed:         make(chan struct{}),
	}
}

func (s *watchdogBlockingProduceSession) CloseAllowingRebalance() {
	s.closeCalls.Add(1)
	s.allowRebalanceCalls.Add(1)
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}

func (s *watchdogBlockingProduceSession) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	s.produced = append(s.produced, records...)
	select {
	case <-s.produceStarted:
	default:
		close(s.produceStarted)
	}
	// Deliberately block and ignore record context (simulating in-flight franz-go idempotent produce retry)
	<-s.closed
	return kgo.ProduceResults{{Err: errors.New("franz-go: client closed")}}
}

func TestKafkaBatchProduceSyncWatchdogBoundsProduceAndAllowsRebalance(t *testing.T) {
	t.Run("ProduceSync ignoring record context is unblocked by watchdog upon OperationTimeout", func(t *testing.T) {
		session := newWatchdogBlockingProduceSession()
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		consumer.transport.config.OperationTimeout = 50 * time.Millisecond

		record := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000096")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{record},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{record.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           record.record,
			bytes:           record.bytes,
			fillStarted:     time.Now(),
		}
		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{
						Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID},
						Err: errors.New("transient error triggering retry produce"),
					},
				},
			}, nil
		}

		errCh := make(chan error, 1)
		streak := uint64(0)
		start := time.Now()
		go func() {
			errCh <- consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		}()

		select {
		case <-session.produceStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ProduceSync to start")
		}

		select {
		case err := <-errCh:
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("expected processKafkaBatch to return error after produce failure")
			}
			if session.closeCalls.Load() == 0 {
				t.Fatal("expected watchdog to call CloseAllowingRebalance")
			}
			if session.allowRebalanceCalls.Load() == 0 {
				t.Fatal("expected AllowRebalance to be called")
			}
			if elapsed > 2*time.Second {
				t.Fatalf("processKafkaBatch took too long: %v, want bounded by OperationTimeout", elapsed)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("processKafkaBatch remained hung in ProduceSync; watchdog failed")
		}
	})

	t.Run("produceWatchdog completed before cancellation does not close session", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		session := &watchdogRaceProduceSession{}
		watchdog := startProduceWatchdog(ctx, session)
		watchdog.Complete()
		cancel()
		if calls := session.closeCalls.Load(); calls != 0 {
			t.Fatalf("expected 0 CloseAllowingRebalance calls for completed watchdog, got %d", calls)
		}
	})

	t.Run("produceWatchdog cancellation before completion closes session", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		session := &watchdogRaceProduceSession{}
		watchdog := startProduceWatchdog(ctx, session)
		cancel()
		<-watchdog.wait
		watchdog.Complete()
		if calls := session.closeCalls.Load(); calls != 1 {
			t.Fatalf("expected 1 CloseAllowingRebalance call for cancelled watchdog, got %d", calls)
		}
	})
}

type watchdogRaceProduceSession struct {
	kafkaBatchSessionRecorder
	closeCalls      atomic.Int32
	cancelOnProduce context.CancelFunc
}

func (s *watchdogRaceProduceSession) CloseAllowingRebalance() {
	s.closeCalls.Add(1)
	s.allowRebalanceCalls.Add(1)
}

func (s *watchdogRaceProduceSession) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	s.produced = append(s.produced, records...)
	if s.cancelOnProduce != nil {
		s.cancelOnProduce()
	}
	return kgo.ProduceResults{{Record: records[0]}}
}

type testRebalanceCheckingPropagator struct {
	session   *kafkaBatchSessionRecorder
	premature *atomic.Int32
}

func (p *testRebalanceCheckingPropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	if p.session.allowRebalanceCalls.Load() == 0 {
		p.premature.Add(1)
	}
	return ctx
}

func (p *testRebalanceCheckingPropagator) Inject(_ context.Context, _ map[string]string) {}

type testRebalanceCheckingSanitizer struct {
	session   *kafkaBatchSessionRecorder
	premature *atomic.Int32
}

func (s *testRebalanceCheckingSanitizer) SanitizeFailure(err error) string {
	if s.session.allowRebalanceCalls.Load() == 0 {
		s.premature.Add(1)
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

type testRebalanceCheckingLogger struct {
	session   *kafkaBatchSessionRecorder
	premature *atomic.Int32
}

func (l *testRebalanceCheckingLogger) Log(_ context.Context, _ messenger.LogLevel, _ string, _ ...messenger.LogAttr) {
	if l.session.allowRebalanceCalls.Load() == 0 {
		l.premature.Add(1)
	}
}

func TestKafkaBatchObserversCalledOnlyAfterAllowRebalance(t *testing.T) {
	checkAllowRebalance := func(t *testing.T, session *kafkaBatchSessionRecorder, scenario string) {
		t.Helper()
		obs := &testKafkaBatchObserver{}
		var prematureObservations atomic.Int32
		obs.onObservation = func(_ messenger.Observation) {
			if session.allowRebalanceCalls.Load() == 0 {
				prematureObservations.Add(1)
			}
		}

		var prematurePropagator atomic.Int32
		var prematureSanitizer atomic.Int32
		var prematureLogger atomic.Int32

		logger := &testRebalanceCheckingLogger{session: session, premature: &prematureLogger}
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		consumer.config.Observers = []messenger.Observer{obs}
		consumer.config.Propagator = &testRebalanceCheckingPropagator{session: session, premature: &prematurePropagator}
		consumer.config.FailureSanitizer = &testRebalanceCheckingSanitizer{session: session, premature: &prematureSanitizer}
		consumer.config.Logger = logger
		consumer.transport.config.Logger = logger

		record := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000097")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{record},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{record.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           record.record,
			bytes:           record.bytes,
			fillStarted:     time.Now(),
		}

		switch scenario {
		case "success":
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			}
		case "fail_closed_permanent":
			consumer.batch.invoke = func(_ context.Context, _ []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, messenger.Permanent(errors.New("terminal poison pill"))
			}
		case "fail_closed_invalid_result":
			consumer.batch.invoke = func(_ context.Context, _ []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, nil
			}
		case "begin_error":
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			}
			session.beginErr = errors.New("kafka begin transaction failed")
		case "produce_error":
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{
							Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID},
							Err: errors.New("retry"),
						},
					},
				}, nil
			}
			session.produceErr = errors.New("produce failed")
		case "commit_error":
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			}
			session.endErr = errors.New("commit failed")
		case "item_level_permanent_dlq":
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{
							Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID},
							Err: messenger.Permanent(errors.New("poison pill item")),
						},
					},
				}, nil
			}
		case "preflight_dlq":
			batch.records[0].prepared.failure = errors.New("preflight failure")
			batch.records[0].prepared.failureKind = "preflight"
		case "deferred_partition":
			batch.firstDeferred = batch.records[0].record
			batch.deferUntil = time.Now().Add(time.Minute)
			consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			}
		}

		streak := uint64(0)
		_ = consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)

		if premature := prematureObservations.Load(); premature > 0 {
			t.Fatalf("[%s] %d observations were invoked BEFORE AllowRebalance was called", scenario, premature)
		}
		if premature := prematurePropagator.Load(); premature > 0 {
			t.Fatalf("[%s] %d propagator extractions were invoked BEFORE AllowRebalance was called", scenario, premature)
		}
		if premature := prematureSanitizer.Load(); premature > 0 {
			t.Fatalf("[%s] %d failure sanitizations were invoked BEFORE AllowRebalance was called", scenario, premature)
		}
		if premature := prematureLogger.Load(); premature > 0 {
			t.Fatalf("[%s] %d logger calls were invoked BEFORE AllowRebalance was called", scenario, premature)
		}
	}

	scenarios := []string{
		"success",
		"fail_closed_permanent",
		"fail_closed_invalid_result",
		"begin_error",
		"produce_error",
		"commit_error",
		"item_level_permanent_dlq",
		"preflight_dlq",
		"deferred_partition",
	}
	for _, sc := range scenarios {
		t.Run(sc, func(t *testing.T) {
			checkAllowRebalance(t, &kafkaBatchSessionRecorder{}, sc)
		})
	}
}

func TestKafkaBatchAggregateDurationExcludesObserverFanOut(t *testing.T) {
	currentTime := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	fakeClock := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	var batchObsDuration time.Duration
	obs := &testKafkaBatchObserver{
		onObservation: func(o messenger.Observation) {
			if o.Operation == messenger.OperationBatchHandle {
				batchObsDuration = o.Duration
			}
			timeMu.Lock()
			currentTime = currentTime.Add(100 * time.Millisecond)
			timeMu.Unlock()
		},
	}

	session := &kafkaBatchSessionRecorder{}
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
	consumer.clock = fakeClock
	consumer.config.Observers = []messenger.Observer{obs}
	record := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-000000000098")
	batch := &kafkaPolledBatch{
		records:         []kafkaBatchRecord{record},
		earliestOffsets: earliestKafkaOffsets([]*kgo.Record{record.record}),
		partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:           record.record,
		bytes:           record.bytes,
		fillStarted:     currentTime,
	}
	consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		timeMu.Lock()
		currentTime = currentTime.Add(25 * time.Millisecond)
		timeMu.Unlock()
		return messenger.BatchResult{
			Items: []messenger.BatchItemResult{
				{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
			},
		}, nil
	}

	streak := uint64(0)
	_ = consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)

	if batchObsDuration > 50*time.Millisecond {
		t.Fatalf("OperationBatchHandle.Duration (%v) included observer fan-out; expected ~25ms", batchObsDuration)
	}
}

func TestKafkaBatchFailClosedPermanentEmitsOperationHandle(t *testing.T) {
	t.Run("top-level Permanent after handler invocation emits OperationHandle for active items", func(t *testing.T) {
		obs := &testKafkaBatchObserver{}
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		consumer.config.Observers = []messenger.Observer{obs}

		first := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-0000000000a1")
		second := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-0000000000a2")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{first, second},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{first.record, second.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           first.record,
			bytes:           first.bytes + second.bytes,
			fillStarted:     time.Now(),
		}

		permErr := errors.New("permanent failure in batch")
		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			if len(decoded) != 2 {
				t.Fatalf("expected 2 active messages, got %d", len(decoded))
			}
			return messenger.BatchResult{}, messenger.Permanent(permErr)
		}

		session := &kafkaBatchSessionRecorder{}
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected processKafkaBatch to return fail-closed error")
		}
		var failClosed *kafkaBatchFailClosedError
		if !errors.As(err, &failClosed) {
			t.Fatalf("expected kafkaBatchFailClosedError, got %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
				if !errors.Is(o.Err, permErr) {
					t.Fatalf("OperationHandle Err = %v, want %v", o.Err, permErr)
				}
			}
		}
		if handleCount != 2 {
			t.Fatalf("expected 2 OperationHandle observations, got %d", handleCount)
		}
		var retries, deferrals, retryHandoffs int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationRetryHandoff {
				retryHandoffs++
			}
			retries += o.BatchRetries
			deferrals += o.BatchDeferrals
		}
		if retries != 0 {
			t.Fatalf("BatchRetries = %d, want 0 on fail-closed", retries)
		}
		if deferrals != 0 {
			t.Fatalf("BatchDeferrals = %d, want 0 on fail-closed", deferrals)
		}
		if retryHandoffs != 0 {
			t.Fatalf("OperationRetryHandoff count = %d, want 0 on fail-closed", retryHandoffs)
		}
	})
}

func TestKafkaBatchFailClosedInvalidResultEmitsOperationHandle(t *testing.T) {
	t.Run("invalid exact-cover result emits OperationHandle with ErrInvalidBatchResult", func(t *testing.T) {
		obs := &testKafkaBatchObserver{}
		consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{})
		consumer.config.Observers = []messenger.Observer{obs}

		first := kafkaBatchTestRecord(t, consumer, 0, 10, "01991387-6880-7000-8000-0000000000a3")
		second := kafkaBatchTestRecord(t, consumer, 0, 11, "01991387-6880-7000-8000-0000000000a4")
		batch := &kafkaPolledBatch{
			records:         []kafkaBatchRecord{first, second},
			earliestOffsets: earliestKafkaOffsets([]*kgo.Record{first.record, second.record}),
			partition:       topicPartition{topic: consumer.sourceTopic, partition: 0},
			first:           first.record,
			bytes:           first.bytes + second.bytes,
			fillStarted:     time.Now(),
		}

		consumer.batch.invoke = func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
				},
			}, nil
		}

		session := &kafkaBatchSessionRecorder{}
		streak := uint64(0)
		err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak)
		if err == nil {
			t.Fatal("expected processKafkaBatch to return fail-closed error")
		}
		var failClosed *kafkaBatchFailClosedError
		if !errors.As(err, &failClosed) {
			t.Fatalf("expected kafkaBatchFailClosedError, got %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
				if !errors.Is(o.Err, messenger.ErrInvalidBatchResult) {
					t.Fatalf("OperationHandle Err = %v, want ErrInvalidBatchResult", o.Err)
				}
			}
		}
		if handleCount != 2 {
			t.Fatalf("expected 2 OperationHandle observations, got %d", handleCount)
		}
		var retries, deferrals, retryHandoffs int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationRetryHandoff {
				retryHandoffs++
			}
			retries += o.BatchRetries
			deferrals += o.BatchDeferrals
		}
		if retries != 0 {
			t.Fatalf("BatchRetries = %d, want 0 on fail-closed", retries)
		}
		if deferrals != 0 {
			t.Fatalf("BatchDeferrals = %d, want 0 on fail-closed", deferrals)
		}
		if retryHandoffs != 0 {
			t.Fatalf("OperationRetryHandoff count = %d, want 0 on fail-closed", retryHandoffs)
		}
	})
}

func (*trackingConfirmBackend) PruneTerminalAttempts(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}
