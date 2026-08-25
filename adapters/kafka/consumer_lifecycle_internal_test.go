package kafka

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testWorkerMemberID = "worker-1"

type consumerGroupMetadataRecorder struct {
	memberID   string
	generation int32
}

type consumerSessionRecorder struct {
	mu          sync.Mutex
	paused      map[string]struct{}
	pollStarted chan struct{}
	releasePoll chan struct{}
	pollOnce    sync.Once
	fetches     kgo.Fetches
	pollCalls   atomic.Int32
	beginCalls  atomic.Int32
	endCalls    atomic.Int32
}

type forceCancellationSession struct {
	consumerSessionRecorder
	endStarted chan struct{}
}

func (recorder *consumerSessionRecorder) GroupMetadata() (string, int32) {
	return testWorkerMemberID, 0
}

func (recorder *consumerSessionRecorder) PauseFetchTopics(topics ...string) []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.paused == nil {
		recorder.paused = make(map[string]struct{})
	}
	for _, topic := range topics {
		recorder.paused[topic] = struct{}{}
	}
	return sortedTopics(recorder.paused)
}

func (recorder *consumerSessionRecorder) ResumeFetchTopics(topics ...string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, topic := range topics {
		delete(recorder.paused, topic)
	}
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
	return recorder.fetches
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

func (recorder *consumerSessionRecorder) pausedTopics() []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return sortedTopics(recorder.paused)
}

func sortedTopics(topics map[string]struct{}) []string {
	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}
	sort.Strings(result)
	return result
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

func TestConsumerDelayedRetryDoesNotPollBufferedRecordsAndRestoresPauseState(t *testing.T) {
	due := time.Now().UTC().Add(20 * time.Millisecond)
	session := &consumerSessionRecorder{
		paused: map[string]struct{}{"external": {}, testSourceTopic: {}},
		fetches: kgo.Fetches{{Topics: []kgo.FetchTopic{{
			Topic: testSourceTopic,
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Records:   []*kgo.Record{{Topic: testSourceTopic, Partition: 0, Offset: 12}},
			}},
		}}}},
	}
	consumer := &Consumer{
		topics: []string{testSourceTopic, testSourceTopic + ".retry"},
		drain:  make(chan struct{}),
		clock:  time.Now,
	}

	if err := consumer.waitUntilDue(t.Context(), session, due); err != nil {
		t.Fatalf("waitUntilDue: %v", err)
	}
	if got := session.pollCalls.Load(); got != 0 {
		t.Fatalf("PollRecords calls during delayed retry = %d, want 0", got)
	}
	want := []string{"external", testSourceTopic}
	if got := session.pausedTopics(); !slices.Equal(got, want) {
		t.Fatalf("paused topics after delayed retry = %v, want %v", got, want)
	}
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
		config: HandlerConfig{ConsumerID: "worker"},
		state:  consumerRunning,
		drain:  make(chan struct{}),
	}
	var processed atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- consumer.runWorkerSession(t.Context(), session, nil,
			func(context.Context, transactionalConsumerSession, *kgo.Record) error {
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
}

func TestConsumerShutdownForceCancelsTransactionFinalization(t *testing.T) {
	session := &forceCancellationSession{endStarted: make(chan struct{})}
	consumer := &Consumer{
		transport:   &Transport{config: TransportConfig{OperationTimeout: time.Minute}},
		config:      HandlerConfig{ConsumerID: "worker", Concurrency: 1},
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
