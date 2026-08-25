package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testWorkerMemberID = "worker-1"

type consumerGroupMetadataRecorder struct {
	memberID   string
	generation int32
}

type consumerSessionRecorder struct {
	mu                  sync.Mutex
	pausedPartitions    map[string]map[int32]struct{}
	committed           map[string]map[int32]kgo.EpochOffset
	uncommitted         map[string]map[int32]kgo.EpochOffset
	resumedPartitions   []map[string][]int32
	ignoreSetOffsets    bool
	poll                func(context.Context, int) kgo.Fetches
	pollStarted         chan struct{}
	releasePoll         chan struct{}
	pollOnce            sync.Once
	fetches             kgo.Fetches
	pollCalls           atomic.Int32
	allowRebalanceCalls atomic.Int32
	beginCalls          atomic.Int32
	endCalls            atomic.Int32
}

type forceCancellationSession struct {
	consumerSessionRecorder
	endStarted chan struct{}
}

type passthroughAttemptBackend struct{}

func (passthroughAttemptBackend) Process(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, handler(ctx)
}

func (passthroughAttemptBackend) ProcessAttempt(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	_ uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{Attempt: 1}, handler(ctx)
}

func (passthroughAttemptBackend) ForgetAttempt(context.Context, inbox.Key, inbox.Fingerprint) error {
	return nil
}

func (passthroughAttemptBackend) Prune(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

type rebalanceCodecPayload struct {
	OrderID string `json:"orderId"`
}

func (recorder *consumerSessionRecorder) GroupMetadata() (string, int32) {
	return testWorkerMemberID, 0
}

func (recorder *consumerSessionRecorder) AllowRebalance() {
	recorder.allowRebalanceCalls.Add(1)
}

func (recorder *consumerSessionRecorder) PauseFetchPartitions(
	topicPartitions map[string][]int32,
) map[string][]int32 {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.pausedPartitions == nil {
		recorder.pausedPartitions = make(map[string]map[int32]struct{})
	}
	for topic, partitions := range topicPartitions {
		if recorder.pausedPartitions[topic] == nil {
			recorder.pausedPartitions[topic] = make(map[int32]struct{})
		}
		for _, partition := range partitions {
			recorder.pausedPartitions[topic][partition] = struct{}{}
		}
	}
	return clonePausedPartitions(recorder.pausedPartitions)
}

func (recorder *consumerSessionRecorder) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.resumedPartitions = append(recorder.resumedPartitions, clonePartitionLists(topicPartitions))
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			delete(recorder.pausedPartitions[topic], partition)
		}
		if len(recorder.pausedPartitions[topic]) == 0 {
			delete(recorder.pausedPartitions, topic)
		}
	}
}

func (recorder *consumerSessionRecorder) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.ignoreSetOffsets {
		return
	}
	if recorder.uncommitted == nil {
		recorder.uncommitted = make(map[string]map[int32]kgo.EpochOffset)
	}
	if recorder.committed == nil {
		recorder.committed = make(map[string]map[int32]kgo.EpochOffset)
	}
	for topic, partitions := range offsets {
		if recorder.uncommitted[topic] == nil {
			recorder.uncommitted[topic] = make(map[int32]kgo.EpochOffset)
		}
		if recorder.committed[topic] == nil {
			recorder.committed[topic] = make(map[int32]kgo.EpochOffset)
		}
		for partition, offset := range partitions {
			recorder.uncommitted[topic][partition] = offset
			recorder.committed[topic][partition] = offset
		}
	}
}

func (recorder *consumerSessionRecorder) UncommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	result := cloneEpochOffsets(recorder.uncommitted)
	for topic, partitions := range result {
		for partition, offset := range partitions {
			committed, committedFound := recorder.committed[topic][partition]
			if committedFound && offset == committed {
				delete(partitions, partition)
			}
		}
		if len(partitions) == 0 {
			delete(result, topic)
		}
	}
	return result
}

func (recorder *consumerSessionRecorder) CommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return cloneEpochOffsets(recorder.committed)
}

func (recorder *consumerSessionRecorder) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	recorder.pollCalls.Add(1)
	if recorder.pollStarted != nil {
		recorder.pollOnce.Do(func() { close(recorder.pollStarted) })
	}
	if recorder.releasePoll != nil {
		select {
		case <-recorder.releasePoll:
		case <-ctx.Done():
			return kgo.NewErrFetch(ctx.Err())
		}
	}
	fetches := recorder.fetches
	if recorder.poll != nil {
		fetches = recorder.poll(ctx, 1)
	}
	recorder.recordPolledOffsets(fetches)
	return fetches
}

func (recorder *consumerSessionRecorder) Begin() error {
	recorder.beginCalls.Add(1)
	return nil
}

func (*consumerSessionRecorder) ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults {
	return nil
}

func (recorder *consumerSessionRecorder) End(context.Context, kgo.TransactionEndTry) (bool, error) {
	recorder.endCalls.Add(1)
	return true, nil
}

func (session *forceCancellationSession) End(ctx context.Context, _ kgo.TransactionEndTry) (bool, error) {
	close(session.endStarted)
	<-ctx.Done()
	return false, ctx.Err()
}

func (recorder *consumerSessionRecorder) recordPolledOffsets(fetches kgo.Fetches) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.uncommitted == nil {
		recorder.uncommitted = make(map[string]map[int32]kgo.EpochOffset)
	}
	iterator := fetches.RecordIter()
	for !iterator.Done() {
		record := iterator.Next()
		if recorder.uncommitted[record.Topic] == nil {
			recorder.uncommitted[record.Topic] = make(map[int32]kgo.EpochOffset)
		}
		recorder.uncommitted[record.Topic][record.Partition] = kgo.EpochOffset{
			Epoch: record.LeaderEpoch, Offset: record.Offset + 1,
		}
	}
}

func clonePausedPartitions(source map[string]map[int32]struct{}) map[string][]int32 {
	cloned := make(map[string][]int32, len(source))
	for topic, partitions := range source {
		for partition := range partitions {
			cloned[topic] = append(cloned[topic], partition)
		}
	}
	return cloned
}

func clonePartitionLists(source map[string][]int32) map[string][]int32 {
	cloned := make(map[string][]int32, len(source))
	for topic, partitions := range source {
		cloned[topic] = append([]int32(nil), partitions...)
	}
	return cloned
}

func cloneEpochOffsets(source map[string]map[int32]kgo.EpochOffset) map[string]map[int32]kgo.EpochOffset {
	cloned := make(map[string]map[int32]kgo.EpochOffset, len(source))
	for topic, partitions := range source {
		cloned[topic] = make(map[int32]kgo.EpochOffset, len(partitions))
		for partition, offset := range partitions {
			cloned[topic][partition] = offset
		}
	}
	return cloned
}

func (recorder consumerGroupMetadataRecorder) GroupMetadata() (string, int32) {
	return recorder.memberID, recorder.generation
}

func TestConsumerRunChecksTopologyBeforeStartingWorkers(t *testing.T) {
	topologyErr := errors.New("topology drift")
	var workersStarted atomic.Int32
	consumer := &Consumer{
		config: HandlerConfig{Concurrency: 3},
		state:  consumerNew,
		drain:  make(chan struct{}),
		done:   make(chan struct{}),
		startupCheck: func(context.Context) error {
			return topologyErr
		},
		workerRun: func(context.Context, int, func()) error {
			workersStarted.Add(1)
			return nil
		},
	}

	err := consumer.Run(t.Context())
	if !errors.Is(err, topologyErr) {
		t.Fatalf("Run error = %v, want topology drift", err)
	}
	if got := workersStarted.Load(); got != 0 {
		t.Fatalf("workers started = %d, want 0", got)
	}
	consumer.mu.Lock()
	state := consumer.state
	consumer.mu.Unlock()
	if state != consumerClosed {
		t.Fatalf("consumer state = %v, want closed", state)
	}
	select {
	case <-consumer.done:
	default:
		t.Fatal("consumer completion was not closed after startup failure")
	}
}

func TestConsumerReadinessRequiresAllTransactionalWorkers(t *testing.T) {
	consumer := &Consumer{config: HandlerConfig{Concurrency: 2}, state: consumerRunning}
	if err := consumer.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("Readiness before worker startup = %v, want ErrRuntimeNotRunning", err)
	}
	consumer.markWorkerReady()
	if err := consumer.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("Readiness with one worker = %v, want ErrRuntimeNotRunning", err)
	}
	consumer.markWorkerReady()
	consumer.mu.Lock()
	ready := consumer.workersReady
	consumer.mu.Unlock()
	if !ready {
		t.Fatal("consumer did not become worker-ready")
	}

	workerErr := errors.New("transactional producer fenced")
	consumer.recordWorkerError(t.Context(), workerErr)
	if err := consumer.Readiness(t.Context()); !errors.Is(err, workerErr) {
		t.Fatalf("Readiness after worker failure = %v, want worker error", err)
	}
}

func TestConsumerGroupJoinedRequiresMemberAndGeneration(t *testing.T) {
	tests := []struct {
		name       string
		memberID   string
		generation int32
		want       bool
	}{
		{name: "not joined", generation: -1},
		{name: "member without generation", memberID: testWorkerMemberID, generation: -1},
		{name: "generation without member", generation: 1},
		{name: "joined", memberID: testWorkerMemberID, generation: 0, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := consumerGroupMetadataRecorder{memberID: test.memberID, generation: test.generation}
			if got := consumerGroupJoined(client); got != test.want {
				t.Fatalf("consumerGroupJoined() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestConsumerRebalanceTimeoutTracksBrokerFinalization(t *testing.T) {
	tests := []struct {
		name      string
		operation time.Duration
		want      time.Duration
	}{
		{name: "short operation keeps franz default", operation: 5 * time.Second, want: time.Minute},
		{name: "long operation extends rebalance", operation: 2 * time.Minute, want: 2 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := consumerRebalanceTimeout(test.operation); got != test.want {
				t.Fatalf("consumerRebalanceTimeout(%s) = %s, want %s", test.operation, got, test.want)
			}
		})
	}
}

func TestConsumerPollTimeoutAllowsRebalance(t *testing.T) {
	session := &consumerSessionRecorder{
		poll: func(ctx context.Context, _ int) kgo.Fetches {
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		},
	}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID},
		drain:  make(chan struct{}),
	}

	poll, err := consumer.pollWorkerRecord(t.Context(), session, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("pollWorkerRecord timeout: %v", err)
	}
	if poll != (workerPoll{}) {
		t.Fatalf("pollWorkerRecord timeout = %#v, want empty poll", poll)
	}
	if got := session.allowRebalanceCalls.Load(); got != 1 {
		t.Fatalf("AllowRebalance calls after timeout = %d, want 1", got)
	}
}

func TestConsumerAllowsRebalanceBeforeBlockingCustomCodec(t *testing.T) {
	native, key := testNativeEnvelope(t)
	session := &consumerSessionRecorder{}
	decodeStarted := make(chan struct{})
	releaseDecode := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseDecode) })
	var decodeCalls atomic.Int32
	var allowAtDecode atomic.Int32
	var consumer *Consumer
	consumer = newRebalanceCodecConsumer(t, func(data []byte) (rebalanceCodecPayload, error) {
		decodeCalls.Add(1)
		allowAtDecode.Store(session.allowRebalanceCalls.Load())
		close(decodeStarted)
		<-releaseDecode
		var payload rebalanceCodecPayload
		return payload, json.Unmarshal(data, &payload)
	}, func(context.Context, messenger.Message[rebalanceCodecPayload]) error {
		consumer.BeginDrain()
		return nil
	})
	record := &kgo.Record{
		Topic: testSourceTopic, Partition: 0, LeaderEpoch: 2, Offset: 7, Key: key, Value: native,
	}
	session.fetches = fetchesWithRecord(record)
	done := make(chan error, 1)
	go func() {
		done <- consumer.runWorkerSession(
			t.Context(), session, nil, consumer.prepareRecord, consumer.processPreparedRecord,
		)
	}()

	select {
	case <-decodeStarted:
	case <-time.After(time.Second):
		t.Fatal("custom codec did not start")
	}
	if got := allowAtDecode.Load(); got != 1 {
		t.Fatalf("AllowRebalance calls when custom codec started = %d, want 1", got)
	}
	if session.beginCalls.Load() != 0 {
		t.Fatalf("Kafka transaction began while custom codec was blocked")
	}
	releaseOnce.Do(func() { close(releaseDecode) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWorkerSession: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after custom codec was released")
	}
	if got := decodeCalls.Load(); got != 1 {
		t.Fatalf("custom codec calls = %d, want 1", got)
	}
	if session.beginCalls.Load() != 1 || session.endCalls.Load() != 1 {
		t.Fatalf("transaction calls = begin:%d end:%d, want 1 each",
			session.beginCalls.Load(), session.endCalls.Load())
	}
}

func TestConsumerEarlyRetryDefersCodecUntilRefetch(t *testing.T) {
	native, key := testNativeEnvelope(t)
	session := &consumerSessionRecorder{}
	var decodeCalls atomic.Int32
	var allowAtDecode atomic.Int32
	var consumer *Consumer
	consumer = newRebalanceCodecConsumer(t, func(data []byte) (rebalanceCodecPayload, error) {
		decodeCalls.Add(1)
		allowAtDecode.Store(session.allowRebalanceCalls.Load())
		var payload rebalanceCodecPayload
		return payload, json.Unmarshal(data, &payload)
	}, func(context.Context, messenger.Message[rebalanceCodecPayload]) error {
		consumer.BeginDrain()
		return nil
	})
	due := time.Now().UTC().Add(50 * time.Millisecond)
	record := &kgo.Record{
		Topic: consumer.retryTopics[0], Partition: 0, LeaderEpoch: 4, Offset: 12, Key: key, Value: native,
		Headers: controlHeaders(controlMetadata{
			source:  sourcePosition{topic: testSourceTopic, partition: 0, offset: 3},
			attempt: 1, notBefore: due,
		}),
	}
	var pollIndex atomic.Int32
	session.poll = func(ctx context.Context, _ int) kgo.Fetches {
		switch pollIndex.Add(1) {
		case 1:
			return fetchesWithRecord(record)
		case 2:
			if got := decodeCalls.Load(); got != 0 {
				t.Fatalf("custom codec ran during early retry deferral: %d calls", got)
			}
			if got := session.allowRebalanceCalls.Load(); got != 1 {
				t.Fatalf("AllowRebalance calls after early retry deferral = %d, want 1", got)
			}
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		case 3:
			return fetchesWithRecord(record)
		default:
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		}
	}

	if err := consumer.runWorkerSession(
		t.Context(), session, nil, consumer.prepareRecord, consumer.processPreparedRecord,
	); err != nil {
		t.Fatalf("runWorkerSession: %v", err)
	}
	if got := decodeCalls.Load(); got != 1 {
		t.Fatalf("custom codec calls after refetch = %d, want 1", got)
	}
	if got := allowAtDecode.Load(); got != 3 {
		t.Fatalf("AllowRebalance calls when refetched codec ran = %d, want 3", got)
	}
	if got := pollIndex.Load(); got != 3 {
		t.Fatalf("poll calls = %d, want 3", got)
	}
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	if containsPartition(session.PauseFetchPartitions(nil), partition) {
		t.Fatalf("retry partition %v remained paused after deadline", partition)
	}
}

func TestConsumerPreflightRejectsRetryAtOrBeyondExpiryWithoutCustomCodec(t *testing.T) {
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	native, key := testNativeEnvelope(t)
	envelope, err := messenger.UnmarshalEnvelope(native)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	payload, encoding, err := envelope.Payload()
	if err != nil {
		t.Fatalf("Payload: %v", err)
	}
	metadata := envelope.Metadata()
	metadata.ExpiresAt = now.Add(time.Minute)
	native, err = messenger.MarshalEnvelope(metadata, payload, encoding)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	var decodeCalls atomic.Int32
	consumer := newRebalanceCodecConsumer(t, func([]byte) (rebalanceCodecPayload, error) {
		decodeCalls.Add(1)
		return rebalanceCodecPayload{}, nil
	}, func(context.Context, messenger.Message[rebalanceCodecPayload]) error {
		t.Fatal("handler ran for an undeliverable retry")
		return nil
	})
	consumer.clock = func() time.Time { return now }
	record := &kgo.Record{
		Topic: consumer.retryTopics[0], Partition: 0, Offset: 12, Key: key, Value: native,
		Headers: controlHeaders(controlMetadata{
			source:  sourcePosition{topic: testSourceTopic, partition: 0, offset: 3},
			attempt: 1, notBefore: now.Add(2 * time.Minute),
		}),
	}

	prepared := consumer.prepareRecord(record)
	if !errors.Is(prepared.failure, ErrMessageExpired) || prepared.failureKind != "expired" {
		t.Fatalf("preflight failure = %q, %v, want expired", prepared.failureKind, prepared.failure)
	}
	if !prepared.retryAt.IsZero() {
		t.Fatalf("preflight retry deadline = %s, want none", prepared.retryAt)
	}
	if prepared.messageID != metadata.ID.String() {
		t.Fatalf("preflight message ID = %q, want %q", prepared.messageID, metadata.ID.String())
	}
	if got := decodeCalls.Load(); got != 0 {
		t.Fatalf("custom codec calls during expiry preflight = %d, want 0", got)
	}
}

func TestConsumerPreflightRejectsMalformedEnvelopeBeforeRetryDeferral(t *testing.T) {
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	consumer := &Consumer{
		descriptor: messenger.MustEvent("orders.created", 1, messenger.JSON[rebalanceCodecPayload]()).Info(),
		clock:      func() time.Time { return now }, sourceTopic: testSourceTopic,
		retrySet: map[string]struct{}{testSourceTopic + ".gm.worker.retry.0": {}},
	}
	retryTopic := testSourceTopic + ".gm.worker.retry.0"
	record := &kgo.Record{
		Topic: retryTopic, Partition: 0, Offset: 1, Key: []byte(testDomainKey), Value: []byte(`{"invalid":`),
		Headers: controlHeaders(controlMetadata{
			source:  sourcePosition{topic: testSourceTopic, partition: 0, offset: 0},
			attempt: 1, notBefore: now.Add(time.Minute),
		}),
	}

	prepared := consumer.prepareRecord(record)
	if prepared.failure == nil || prepared.failureKind != failureKindDecode {
		t.Fatalf("preflight failure = %q, %v, want decode", prepared.failureKind, prepared.failure)
	}
	if !prepared.retryAt.IsZero() {
		t.Fatalf("preflight retry deadline = %s, want none", prepared.retryAt)
	}
}

func newRebalanceCodecConsumer(
	t *testing.T,
	decode func([]byte) (rebalanceCodecPayload, error),
	handler messenger.Handler[rebalanceCodecPayload],
) *Consumer {
	t.Helper()
	codec, err := messenger.CustomCodec(
		"application/json",
		messenger.DataJSON,
		func(payload rebalanceCodecPayload) ([]byte, error) { return json.Marshal(payload) },
		decode,
	)
	if err != nil {
		t.Fatalf("custom codec: %v", err)
	}
	store, err := inbox.New(passthroughAttemptBackend{})
	if err != nil {
		t.Fatalf("new passthrough Inbox: %v", err)
	}
	consumer, err := NewEventConsumer(
		&Transport{config: TransportConfig{OperationTimeout: time.Second}},
		store,
		messenger.MustEvent("orders.created", 1, codec),
		handler,
		HandlerConfig{Namespace: testNamespace, ConsumerID: testConsumerID},
	)
	if err != nil {
		t.Fatalf("new event consumer: %v", err)
	}
	return consumer
}

func TestConsumerDrainAfterPollDoesNotProcessFetchedRecord(t *testing.T) {
	record := &kgo.Record{Topic: testSourceTopic, Partition: 0, Offset: 12}
	session := &consumerSessionRecorder{
		pollStarted: make(chan struct{}),
		releasePoll: make(chan struct{}),
		fetches: kgo.Fetches{{Topics: []kgo.FetchTopic{{
			Topic: testSourceTopic,
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Records:   []*kgo.Record{record},
			}},
		}}}},
	}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID},
		state:  consumerRunning,
		drain:  make(chan struct{}),
	}
	var processed atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- consumer.runWorkerSession(t.Context(), session, nil,
			func(*kgo.Record) preparedRecord { return preparedRecord{} },
			func(context.Context, transactionalConsumerSession, *kgo.Record, preparedRecord) error {
				processed.Add(1)
				return nil
			})
	}()

	select {
	case <-session.pollStarted:
	case <-time.After(time.Second):
		t.Fatal("consumer did not enter PollRecords")
	}
	consumer.BeginDrain()
	close(session.releasePoll)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWorkerSession after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after drain")
	}
	if got := processed.Load(); got != 0 {
		t.Fatalf("records processed after drain = %d, want 0", got)
	}
	if session.beginCalls.Load() != 0 || session.endCalls.Load() != 0 {
		t.Fatalf("transaction calls after drain = begin:%d end:%d, want 0",
			session.beginCalls.Load(), session.endCalls.Load())
	}
	if got := session.allowRebalanceCalls.Load(); got != 1 {
		t.Fatalf("AllowRebalance calls after drain = %d, want 1", got)
	}
}

func TestConsumerShutdownForceCancelsTransactionFinalization(t *testing.T) {
	session := &forceCancellationSession{endStarted: make(chan struct{})}
	consumer := &Consumer{
		transport:   &Transport{config: TransportConfig{OperationTimeout: time.Minute}},
		config:      HandlerConfig{ConsumerID: testConsumerID, Concurrency: 1},
		sourceTopic: testSourceTopic,
		state:       consumerNew,
		drain:       make(chan struct{}),
		done:        make(chan struct{}),
	}
	consumer.startupCheck = func(context.Context) error { return nil }
	consumer.workerRun = func(ctx context.Context, _ int, ready func()) error {
		ready()
		_, err := consumer.commitRecord(ctx, session, nil)
		return err
	}
	runResult := make(chan error, 1)
	go func() {
		runResult <- consumer.Run(t.Context())
	}()

	select {
	case <-session.endStarted:
	case <-time.After(time.Second):
		t.Fatal("consumer did not begin transaction finalization")
	}
	shutdownContext, cancelShutdown := context.WithCancel(t.Context())
	cancelShutdown()
	if err := consumer.Shutdown(shutdownContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("forced Shutdown error = %v, want context canceled", err)
	}
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run after forced transaction cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer Run outlived the forced shutdown deadline")
	}
}
