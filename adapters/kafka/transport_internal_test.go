package kafka

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testTransportName     = "kafka.test"
	testTransportClientID = "kafka-test"
	testTransportInstance = "host-a"
	testBrokerAddress     = "127.0.0.1:1"
)

type transactionEndRecorder struct {
	contextErr  error
	hasDeadline bool
	operation   kgo.TransactionEndTry
}

type transactionalReadinessRecorder struct {
	pingErr             error
	producerIDErr       error
	pingCalls           int
	producerCalls       int
	pingHasDeadline     bool
	producerHasDeadline bool
}

type produceRecordHook struct{}

func (produceRecordHook) OnProduceRecordBuffered(*kgo.Record) {}

type fetchRecordHook struct{}

func (fetchRecordHook) OnFetchRecordBuffered(*kgo.Record) {}

type fetchRecordUnbufferedHook struct{}

func (fetchRecordUnbufferedHook) OnFetchRecordUnbuffered(*kgo.Record, bool) {}

type newClientHook struct{}

func (newClientHook) OnNewClient(*kgo.Client) {}

type produceRecordPartitionedHook struct{}

func (produceRecordPartitionedHook) OnProduceRecordPartitioned(*kgo.Record, int32) {}

type produceRecordUnbufferedHook struct{}

func (produceRecordUnbufferedHook) OnProduceRecordUnbuffered(*kgo.Record, error) {}

type groupManageHook struct{}

func (groupManageHook) OnGroupManageError(error) {}

func (recorder *transactionalReadinessRecorder) Ping(ctx context.Context) error {
	recorder.pingCalls++
	_, recorder.pingHasDeadline = ctx.Deadline()
	return recorder.pingErr
}

func (recorder *transactionalReadinessRecorder) ProducerID(ctx context.Context) (int64, int16, error) {
	recorder.producerCalls++
	_, recorder.producerHasDeadline = ctx.Deadline()
	return 1, 0, recorder.producerIDErr
}

func (recorder *transactionEndRecorder) EndTransaction(
	ctx context.Context,
	operation kgo.TransactionEndTry,
) error {
	recorder.contextErr = ctx.Err()
	_, recorder.hasDeadline = ctx.Deadline()
	recorder.operation = operation
	return nil
}

func TestRequiredProducerBatchLimitsMatchManagedTopology(t *testing.T) {
	retry, err := retryTopic(testSourceTopic, testConsumerID, 0)
	if err != nil {
		t.Fatalf("retryTopic: %v", err)
	}
	replay, err := replayTopic(testSourceTopic, testConsumerID)
	if err != nil {
		t.Fatalf("replayTopic: %v", err)
	}
	dlq, err := dlqTopic(testSourceTopic, testConsumerID)
	if err != nil {
		t.Fatalf("dlqTopic: %v", err)
	}
	tests := []struct {
		topic string
		want  int32
	}{
		{topic: testSourceTopic, want: int32(DefaultMaxSourceMessageBytes)},
		{topic: retry, want: int32(DefaultMaxSourceMessageBytes)},
		{topic: replay, want: int32(DefaultMaxSourceMessageBytes)},
		{topic: dlq, want: int32(DefaultMaxDLQMessageBytes)},
	}
	for _, test := range tests {
		if got := producerBatchMaxBytes(test.topic); got != test.want {
			t.Errorf("producer batch max for %q = %d, want %d", test.topic, got, test.want)
		}
	}
}

func TestTransactionalReadinessInitializesProducerID(t *testing.T) {
	producerErr := errors.New("transactional ID denied")
	client := &transactionalReadinessRecorder{producerIDErr: producerErr}
	if err := checkTransactionalReadiness(t.Context(), client); !errors.Is(err, producerErr) {
		t.Fatalf("checkTransactionalReadiness error = %v, want producer ID error", err)
	}
	if client.pingCalls != 1 || client.producerCalls != 1 {
		t.Fatalf("readiness calls = ping:%d producer:%d, want 1 each", client.pingCalls, client.producerCalls)
	}

	pingErr := errors.New("broker unavailable")
	client = &transactionalReadinessRecorder{pingErr: pingErr}
	if err := checkTransactionalReadiness(t.Context(), client); !errors.Is(err, pingErr) {
		t.Fatalf("checkTransactionalReadiness error = %v, want ping error", err)
	}
	if client.producerCalls != 0 {
		t.Fatalf("producer ID calls after ping failure = %d, want 0", client.producerCalls)
	}
}

func TestTransactionalStartupAppliesOperationDeadline(t *testing.T) {
	client := &transactionalReadinessRecorder{}
	if err := checkTransactionalStartup(context.Background(), time.Second, client); err != nil {
		t.Fatalf("checkTransactionalStartup: %v", err)
	}
	if !client.pingHasDeadline || !client.producerHasDeadline {
		t.Fatalf("startup deadlines = ping:%t producer:%t, want both true",
			client.pingHasDeadline, client.producerHasDeadline)
	}
}

func TestDefaultMaxSourceMessageBytesCoversEnvelopeAndRecordKey(t *testing.T) {
	t.Parallel()
	want := 2*messenger.DefaultMaxEnvelopeBytes + messenger.DefaultMaxHeaderBytes
	if DefaultMaxSourceMessageBytes < want {
		t.Fatalf("DefaultMaxSourceMessageBytes = %d, want at least %d", DefaultMaxSourceMessageBytes, want)
	}
}

func TestTransportRunAfterPreDrainClosesWithoutBrokerAccess(t *testing.T) {
	transport := newTestTransport(t)
	transport.BeginDrain()
	if err := transport.Run(t.Context()); err != nil {
		t.Fatalf("Run after BeginDrain: %v", err)
	}
	select {
	case <-transport.done:
	default:
		t.Fatal("pre-drained transport did not close")
	}
	if err := transport.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestTransportShutdownWaitsForAdmittedTransaction(t *testing.T) {
	transport := newTestTransport(t)
	transport.mu.Lock()
	transport.state = transportRunning
	transport.mu.Unlock()
	if err := transport.acquireTransaction(t.Context()); err != nil {
		t.Fatalf("acquireTransaction: %v", err)
	}

	shutdownContext, cancel := context.WithCancel(t.Context())
	cancel()
	if err := transport.Shutdown(shutdownContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown with active transaction error = %v, want context canceled", err)
	}
	transport.releaseTransaction()
	if err := transport.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown after transaction completion: %v", err)
	}
}

func TestTransportTransactionAdmissionHonorsCancellation(t *testing.T) {
	transport := newTestTransport(t)
	transport.mu.Lock()
	transport.state = transportRunning
	transport.mu.Unlock()
	if err := transport.acquireTransaction(t.Context()); err != nil {
		t.Fatalf("acquire first transaction: %v", err)
	}
	defer transport.releaseTransaction()

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- transport.acquireTransaction(ctx) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued transaction error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued transaction did not stop after context cancellation")
	}

	transport.mu.Lock()
	active := transport.txActive
	transport.mu.Unlock()
	if !active {
		t.Fatal("canceled waiter disturbed the admitted transaction")
	}
}

func TestAbortTransactionUsesFreshBoundedContext(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	recorder := &transactionEndRecorder{}
	if err := abortTransaction(parent, time.Second, recorder); err != nil {
		t.Fatalf("abortTransaction: %v", err)
	}
	if recorder.contextErr != nil {
		t.Fatalf("abort context was already canceled: %v", recorder.contextErr)
	}
	if !recorder.hasDeadline {
		t.Fatal("abort context has no deadline")
	}
	if recorder.operation != kgo.TryAbort {
		t.Fatalf("abort operation = %v, want TryAbort", recorder.operation)
	}
}

func TestNewTransportClonesBrokerInputForWorkers(t *testing.T) {
	brokers := []string{"broker-one:9092"}
	transport, err := NewTransport(TransportConfig{
		Name:             testTransportName,
		Brokers:          brokers,
		ClientID:         testTransportClientID,
		InstanceID:       testTransportInstance,
		OperationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(transport.closeClient)
	brokers[0] = "broker-two:9092"

	worker, err := kgo.NewClient(transport.workerOptions(
		"kafka.test.worker",
		"host-a-worker-1",
		"kafka.test.host-a.worker-1",
		[]string{"kafka.test.event.created.v1"},
	)...)
	if err != nil {
		t.Fatalf("create worker client: %v", err)
	}
	t.Cleanup(worker.Close)
	got, ok := worker.OptValue(kgo.SeedBrokers).([]string)
	if !ok || !slices.Equal(got, []string{"broker-one:9092"}) {
		t.Fatalf("worker seed brokers = %#v, want original broker input", got)
	}
}

func TestConnectionOptionsCannotReplaceWorkerPolicy(t *testing.T) {
	transport, err := NewTransport(TransportConfig{
		Name:              testTransportName,
		Brokers:           []string{testBrokerAddress},
		ClientID:          testTransportClientID,
		InstanceID:        testTransportInstance,
		ConnectionOptions: []ConnectionOption{Rack("rack-a")},
		OperationTimeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(transport.closeClient)

	topics := []string{"kafka.test.event.created.v1"}
	worker, err := kgo.NewClient(transport.workerOptions(
		"kafka.test.worker",
		"host-a-worker-1",
		"kafka.test.host-a.worker-1",
		topics,
	)...)
	if err != nil {
		t.Fatalf("create worker client: %v", err)
	}
	t.Cleanup(worker.Close)
	if got, ok := worker.OptValue(kgo.ConsumeRegex).(bool); !ok || got {
		t.Fatalf("ConsumeRegex = %v, want false", got)
	}
	if got, ok := worker.OptValue(kgo.BlockRebalanceOnPoll).(bool); !ok || !got {
		t.Fatalf("BlockRebalanceOnPoll = %v, want true", got)
	}
	gotTopics, ok := worker.OptValue(kgo.ConsumeTopics).(map[string]*regexp.Regexp)
	if !ok || len(gotTopics) != 1 || gotTopics[topics[0]] != nil {
		t.Fatalf("ConsumeTopics = %#v, want exact topic %#v", gotTopics, topics)
	}
	if got := worker.OptValue(kgo.Rack); got != "rack-a" {
		t.Fatalf("Rack = %v, want rack-a", got)
	}
}

func TestWithHooksRejectsRecordMutatingHooks(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		hook kgo.Hook
	}{
		{name: "produce buffered", hook: produceRecordHook{}},
		{name: "produce partitioned", hook: produceRecordPartitionedHook{}},
		{name: "produce unbuffered", hook: produceRecordUnbufferedHook{}},
		{name: "fetch buffered", hook: fetchRecordHook{}},
		{name: "fetch unbuffered", hook: fetchRecordUnbufferedHook{}},
		{name: "new client", hook: newClientHook{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if option := WithHooks(test.hook); !nilValue(option.option) {
				t.Fatal("record-mutating Kafka hook was accepted")
			}
		})
	}
}

func TestWithHooksAcceptsNonMutatingHook(t *testing.T) {
	t.Parallel()
	if option := WithHooks(groupManageHook{}); nilValue(option.option) {
		t.Fatal("non-mutating Kafka hook was rejected")
	}
}

func TestNewTransportRejectsInvalidConnectionOption(t *testing.T) {
	_, err := NewTransport(TransportConfig{
		Name:              testTransportName,
		Brokers:           []string{testBrokerAddress},
		ClientID:          testTransportClientID,
		InstanceID:        testTransportInstance,
		ConnectionOptions: []ConnectionOption{{}},
		OperationTimeout:  100 * time.Millisecond,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewTransport error = %v, want ErrInvalidConfig", err)
	}
}

func newTestTransport(t *testing.T) *Transport {
	t.Helper()
	transport, err := NewTransport(TransportConfig{
		Name:             testTransportName,
		Brokers:          []string{testBrokerAddress},
		ClientID:         testTransportClientID,
		InstanceID:       testTransportInstance,
		OperationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(transport.closeClient)
	return transport
}
