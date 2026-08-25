package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRetryPartitionSchedulerOrdersDeadlinesAndPreservesForeignPause(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	scheduler := newRetryPartitionScheduler()
	retryTopicName := string(TopicRoleRetry)
	late := topicPartition{topic: retryTopicName, partition: 2}
	foreign := topicPartition{topic: retryTopicName, partition: 0}
	early := topicPartition{topic: retryTopicName, partition: 1}
	scheduler.schedule(late, now.Add(2*time.Second), true)
	scheduler.schedule(foreign, now.Add(time.Second), false)
	scheduler.schedule(early, now.Add(time.Second), true)

	if got := scheduler.pollTimeout(now, 5*time.Second); got != time.Second {
		t.Fatalf("poll timeout = %s, want 1s", got)
	}
	first := scheduler.releaseDue(now.Add(time.Second))
	if got := first[retryTopicName]; len(got) != 1 || got[0] != early.partition {
		t.Fatalf("first resumed partitions = %v, want [%d]", got, early.partition)
	}
	if containsPartition(first, foreign) {
		t.Fatalf("foreign pause %v was resumed", foreign)
	}
	if got := scheduler.pollTimeout(now.Add(time.Second), 5*time.Second); got != time.Second {
		t.Fatalf("second poll timeout = %s, want 1s", got)
	}
	second := scheduler.releaseDue(now.Add(2 * time.Second))
	if got := second[retryTopicName]; len(got) != 1 || got[0] != late.partition {
		t.Fatalf("second resumed partitions = %v, want [%d]", got, late.partition)
	}
	if len(scheduler.deadlines) != 0 || len(scheduler.partitions) != 0 || len(scheduler.ownedPause) != 0 {
		t.Fatalf("scheduler retained state: deadlines=%d partitions=%d owned=%d",
			len(scheduler.deadlines), len(scheduler.partitions), len(scheduler.ownedPause))
	}
}

func TestPauseAndRewindRetryPartitionUsesExactEpochOffset(t *testing.T) {
	t.Parallel()
	record := &kgo.Record{
		Topic: testSourceTopic + ".retry", Partition: 3, LeaderEpoch: 7, Offset: 41,
	}
	session := &consumerSessionRecorder{}
	session.recordPolledOffsets(fetchesWithRecord(record))

	partition, ownsPause, err := pauseAndRewindRetryPartition(session, record)
	if err != nil {
		t.Fatalf("pauseAndRewindRetryPartition: %v", err)
	}
	if partition != (topicPartition{topic: record.Topic, partition: record.Partition}) {
		t.Fatalf("partition = %#v", partition)
	}
	if !ownsPause {
		t.Fatal("new partition pause was not owned by scheduler")
	}
	if !containsPartition(session.PauseFetchPartitions(nil), partition) {
		t.Fatalf("partition %v is not paused", partition)
	}
	want := kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
	if got := session.CommittedOffsets()[record.Topic][record.Partition]; got != want {
		t.Fatalf("rewound committed cursor = %#v, want %#v", got, want)
	}
	if _, found := session.UncommittedOffsets()[record.Topic][record.Partition]; found {
		t.Fatal("rewound cursor remained ahead in UncommittedOffsets")
	}
}

func TestPauseAndRewindRetryPartitionKeepsForeignPauseOwnership(t *testing.T) {
	t.Parallel()
	record := &kgo.Record{Topic: testSourceTopic + ".retry", Partition: 1, LeaderEpoch: 2, Offset: 9}
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	session := &consumerSessionRecorder{
		pausedPartitions: map[string]map[int32]struct{}{record.Topic: {record.Partition: {}}},
	}
	session.recordPolledOffsets(fetchesWithRecord(record))

	_, ownsPause, err := pauseAndRewindRetryPartition(session, record)
	if err != nil {
		t.Fatalf("pauseAndRewindRetryPartition: %v", err)
	}
	if ownsPause {
		t.Fatal("scheduler claimed a pre-existing partition pause")
	}
	scheduler := newRetryPartitionScheduler()
	scheduler.schedule(partition, time.Now().UTC(), ownsPause)
	if due := scheduler.releaseDue(time.Now().UTC().Add(time.Second)); len(due) != 0 {
		t.Fatalf("foreign pause release = %v, want none", due)
	}
	if !containsPartition(session.PauseFetchPartitions(nil), partition) {
		t.Fatal("foreign partition pause was removed")
	}
}

func TestConsumerDeferredRetryProcessesAnotherPartitionBeforeDeadline(t *testing.T) {
	due := time.Now().UTC().Add(5 * time.Second)
	delayed := &kgo.Record{
		Topic: testSourceTopic + ".retry", Partition: 0, LeaderEpoch: 4, Offset: 12,
	}
	barrier := &kgo.Record{Topic: testSourceTopic, Partition: 1, LeaderEpoch: 5, Offset: 21}
	var pollIndex atomic.Int32
	session := &consumerSessionRecorder{}
	session.poll = func(ctx context.Context, _ int) kgo.Fetches {
		switch pollIndex.Add(1) {
		case 1:
			return fetchesWithRecord(delayed)
		case 2:
			return fetchesWithRecord(barrier)
		default:
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		}
	}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID},
		state:  consumerRunning,
		drain:  make(chan struct{}),
		clock:  time.Now,
	}
	var processed atomic.Int32
	err := consumer.runWorkerSession(
		t.Context(),
		session,
		nil,
		func(record *kgo.Record) preparedRecord {
			if record == delayed {
				return preparedRecord{retryAt: due}
			}
			return preparedRecord{}
		},
		func(_ context.Context, _ transactionalConsumerSession, record *kgo.Record, _ preparedRecord) error {
			if record != barrier {
				t.Fatalf("processed record = %#v, want barrier", record)
			}
			if got := session.allowRebalanceCalls.Load(); got != 2 {
				t.Fatalf("AllowRebalance calls before barrier handler = %d, want 2", got)
			}
			processed.Add(1)
			consumer.BeginDrain()
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runWorkerSession: %v", err)
	}
	if got := processed.Load(); got != 1 {
		t.Fatalf("processed records = %d, want 1", got)
	}
	delayedPartition := topicPartition{topic: delayed.Topic, partition: delayed.Partition}
	if !containsPartition(session.PauseFetchPartitions(nil), delayedPartition) {
		t.Fatalf("delayed partition %v is not paused", delayedPartition)
	}
	wantDelayed := kgo.EpochOffset{Epoch: delayed.LeaderEpoch, Offset: delayed.Offset}
	if got := session.CommittedOffsets()[delayed.Topic][delayed.Partition]; got != wantDelayed {
		t.Fatalf("delayed cursor after barrier = %#v, want %#v", got, wantDelayed)
	}
	if got, found := session.UncommittedOffsets()[delayed.Topic][delayed.Partition]; found && got != wantDelayed {
		t.Fatalf("delayed uncommitted offset after barrier = %#v, want absent or %#v", got, wantDelayed)
	}
	if got := session.UncommittedOffsets()[barrier.Topic][barrier.Partition].Offset; got != barrier.Offset+1 {
		t.Fatalf("barrier uncommitted offset = %d, want %d", got, barrier.Offset+1)
	}
}

func TestConsumerDeferredRetryIsReadAgainAfterResume(t *testing.T) {
	due := time.Now().UTC().Add(40 * time.Millisecond)
	record := &kgo.Record{
		Topic: testSourceTopic + ".retry", Partition: 0, LeaderEpoch: 3, Offset: 8,
	}
	var pollIndex atomic.Int32
	session := &consumerSessionRecorder{}
	session.poll = func(ctx context.Context, _ int) kgo.Fetches {
		switch pollIndex.Add(1) {
		case 1:
			return fetchesWithRecord(record)
		case 2:
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		case 3:
			return fetchesWithRecord(record)
		default:
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		}
	}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID},
		state:  consumerRunning,
		drain:  make(chan struct{}),
		clock:  time.Now,
	}
	var processed atomic.Int32
	err := consumer.runWorkerSession(
		t.Context(),
		session,
		nil,
		func(*kgo.Record) preparedRecord {
			if time.Now().UTC().Before(due) {
				return preparedRecord{retryAt: due}
			}
			return preparedRecord{}
		},
		func(context.Context, transactionalConsumerSession, *kgo.Record, preparedRecord) error {
			processed.Add(1)
			consumer.BeginDrain()
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runWorkerSession: %v", err)
	}
	if got := processed.Load(); got != 1 {
		t.Fatalf("processed records = %d, want 1", got)
	}
	if got := pollIndex.Load(); got != 3 {
		t.Fatalf("poll calls = %d, want 3", got)
	}
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	if containsPartition(session.PauseFetchPartitions(nil), partition) {
		t.Fatalf("partition %v remained paused after deadline", partition)
	}
	session.mu.Lock()
	resumeCalls := append([]map[string][]int32(nil), session.resumedPartitions...)
	session.mu.Unlock()
	if len(resumeCalls) != 1 || !containsPartition(resumeCalls[0], partition) {
		t.Fatalf("resumed partitions = %v, want %v exactly once", resumeCalls, partition)
	}
}

func TestConsumerDeferredRetryCancellationStopsPoll(t *testing.T) {
	record := &kgo.Record{Topic: testSourceTopic + ".retry", Partition: 0, Offset: 5}
	waiting := make(chan struct{})
	var pollIndex atomic.Int32
	session := &consumerSessionRecorder{}
	session.poll = func(ctx context.Context, _ int) kgo.Fetches {
		if pollIndex.Add(1) == 1 {
			return fetchesWithRecord(record)
		}
		close(waiting)
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID}, state: consumerRunning,
		drain: make(chan struct{}), clock: time.Now,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- consumer.runWorkerSession(
			ctx,
			session,
			nil,
			func(*kgo.Record) preparedRecord {
				return preparedRecord{retryAt: time.Now().UTC().Add(time.Minute)}
			},
			func(context.Context, transactionalConsumerSession, *kgo.Record, preparedRecord) error {
				t.Error("deferred record was processed before cancellation")
				return nil
			},
		)
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("worker did not continue polling after deferral")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runWorkerSession cancellation = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestConsumerDeferredRetryFailsClosedWhenRewindIsNotConfirmed(t *testing.T) {
	record := &kgo.Record{Topic: testSourceTopic + ".retry", Partition: 0, LeaderEpoch: 6, Offset: 18}
	session := &consumerSessionRecorder{fetches: fetchesWithRecord(record), ignoreSetOffsets: true}
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: testConsumerID}, state: consumerRunning,
		drain: make(chan struct{}), clock: time.Now,
	}
	var processed atomic.Int32
	err := consumer.runWorkerSession(
		t.Context(),
		session,
		nil,
		func(*kgo.Record) preparedRecord {
			return preparedRecord{retryAt: time.Now().UTC().Add(time.Minute)}
		},
		func(context.Context, transactionalConsumerSession, *kgo.Record, preparedRecord) error {
			processed.Add(1)
			return nil
		},
	)
	if !errors.Is(err, errRetryOffsetRewindNotConfirmed) {
		t.Fatalf("runWorkerSession error = %v, want rewind confirmation failure", err)
	}
	if got := processed.Load(); got != 0 {
		t.Fatalf("processed records = %d, want 0", got)
	}
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	if !containsPartition(session.PauseFetchPartitions(nil), partition) {
		t.Fatalf("failed partition %v was resumed", partition)
	}
	if got := session.allowRebalanceCalls.Load(); got != 1 {
		t.Fatalf("AllowRebalance calls after fail-closed rewind = %d, want 1", got)
	}
}

func fetchesWithRecord(record *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: record.Topic,
		Partitions: []kgo.FetchPartition{{
			Partition: record.Partition,
			Records:   []*kgo.Record{record},
		}},
	}}}}
}
