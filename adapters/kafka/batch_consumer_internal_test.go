package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testBatchSource = "urn:test"
	testBatchName   = "batch.test"
	testBatchTopic  = "test.topic"
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
	produced []*kgo.Record
}

func (s *kafkaBatchSessionRecorder) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	s.produced = append(s.produced, records...)
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
		records:   []kafkaBatchRecord{first, second},
		all:       []*kgo.Record{first.record, other.record, second.record},
		partition: topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:     first.record, bytes: first.bytes + second.bytes, fillStarted: time.Now(),
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
	if session.allowRebalanceCalls.Load() != 0 {
		t.Fatalf("AllowRebalance during processing = %d", session.allowRebalanceCalls.Load())
	}
}

func TestKafkaBatchTopLevelErrorRewindsWithoutKafkaTransaction(t *testing.T) {
	topErr := messenger.RetryAfter(errors.New("whole batch"), 10*time.Millisecond)
	consumer := newKafkaBatchTestConsumer(t, &kafkaBatchTestBackend{topLevelErr: topErr})
	first := kafkaBatchTestRecord(t, consumer, 0, 17, "01991387-6880-7000-8000-000000000084")
	other := kafkaBatchTestRecord(t, consumer, 1, 4, "01991387-6880-7000-8000-000000000085")
	batch := &kafkaPolledBatch{
		records: []kafkaBatchRecord{first}, all: []*kgo.Record{first.record, other.record},
		partition: topicPartition{topic: consumer.sourceTopic, partition: 0},
		first:     first.record, bytes: first.bytes, fillStarted: time.Now(),
	}
	session := &kafkaBatchSessionRecorder{}
	streak := uint64(0)
	if err := consumer.processKafkaBatch(t.Context(), session, newRetryPartitionScheduler(), batch, &streak); err != nil {
		t.Fatalf("processKafkaBatch() error = %v", err)
	}
	if session.beginCalls.Load() != 0 || session.endCalls.Load() != 0 {
		t.Fatalf("top-level error started transaction: begin=%d end=%d",
			session.beginCalls.Load(), session.endCalls.Load())
	}
	if got := session.committed[consumer.sourceTopic][0].Offset; got != 17 {
		t.Fatalf("selected rewind = %d, want 17", got)
	}
	if got := session.committed[consumer.sourceTopic][1].Offset; got != 4 {
		t.Fatalf("other rewind = %d, want 4", got)
	}
	if streak != 1 {
		t.Fatalf("top-level streak = %d, want 1", streak)
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
		},
		descriptor: messenger.DescriptorInfo{
			Kind: messenger.KindEvent, Name: testBatchName, SchemaVersion: 1,
			ContentType: "application/json", DataEncoding: messenger.DataJSON,
		},
		sourceTopic: testSourceTopic, replayTopic: testSourceTopic + ".replay",
		retryTopics: []string{testSourceTopic + ".retry"},
		dlqTopic:    testSourceTopic + ".dlq", clock: time.Now,
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
		ID: messageID, Source: testBatchSource, Kind: messenger.KindEvent,
		Name: testBatchName, SchemaVersion: 1,
	}
	canonical := []byte(`{"specVersion":"1.0"}`)
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
		Name: testBatchName, SchemaVersion: 1, Time: time.Now().UTC(), ContentType: "application/json",
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
		all: []*kgo.Record{recordP0, recordP1, recordP0Later},
	}
	offsets := earliestKafkaOffsets(batch.all)
	if offsets[testBatchTopic][0].Offset != 10 {
		t.Fatalf("p0 offset = %d, want 10", offsets[testBatchTopic][0].Offset)
	}
	if offsets[testBatchTopic][1].Offset != 20 {
		t.Fatalf("p1 offset = %d, want 20", offsets[testBatchTopic][1].Offset)
	}
}
