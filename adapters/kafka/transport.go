package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const defaultBrokerOperationTimeout = 30 * time.Second

// TransportConfig declares host-owned connection input and stable process identity.
type TransportConfig struct {
	Name              string
	Brokers           []string
	ClientID          string
	InstanceID        string
	ConnectionOptions []ConnectionOption
	OperationTimeout  time.Duration
	Logger            messenger.Logger
}

type transportState uint8

const (
	transportNew transportState = iota
	transportRunning
	transportDraining
	transportClosed
)

// Transport owns the shared direct producer and admin client used by routes.
type Transport struct {
	config   TransportConfig
	client   *kgo.Client
	admin    *kadm.Client
	baseOpts []kgo.Opt

	txGate          chan struct{}
	topologyMu      sync.Mutex
	mu              sync.Mutex
	state           transportState
	runStarted      bool
	txActive        bool
	txDone          chan struct{}
	drain           chan struct{}
	drainOnce       sync.Once
	done            chan struct{}
	doneOnce        sync.Once
	clientCloseOnce sync.Once
}

// NewTransport constructs a managed Kafka transport with adapter-enforced
// idempotent, all-ISR, transactional producer settings.
func NewTransport(config TransportConfig) (*Transport, error) {
	config.Brokers = slices.Clone(config.Brokers)
	config.ConnectionOptions = slices.Clone(config.ConnectionOptions)
	applyTransportDefaults(&config)
	if err := validateTransportConfig(config); err != nil {
		return nil, err
	}
	baseOpts := make([]kgo.Opt, len(config.ConnectionOptions))
	for index, option := range config.ConnectionOptions {
		baseOpts[index] = option.option
	}
	producerID, err := transactionalID(config.Name, config.InstanceID, 0)
	if err != nil {
		return nil, err
	}
	opts := appendRequiredOptions(baseOpts, requiredProducerOptions(config, producerID)...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("messenger/kafka: create transport client: %w", err)
	}
	return &Transport{
		config: config, client: client, admin: kadm.NewClient(client), baseOpts: baseOpts,
		txGate: make(chan struct{}, 1), state: transportNew,
		drain: make(chan struct{}), done: make(chan struct{}),
	}, nil
}

// Name returns the stable managed service identifier.
func (t *Transport) Name() string { return t.config.Name }

// Run verifies broker and transactional producer readiness and blocks until drain or cancellation.
func (t *Transport) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil transport context", ErrInvalidConfig)
	}
	t.mu.Lock()
	switch t.state {
	case transportRunning:
		t.mu.Unlock()
		return messenger.ErrRuntimeRunning
	case transportDraining:
		if !t.runStarted {
			t.runStarted = true
			t.state = transportClosed
			t.mu.Unlock()
			t.closeDone()
			return nil
		}
		t.mu.Unlock()
		return messenger.ErrRuntimeRunning
	case transportClosed:
		t.mu.Unlock()
		return ErrTransportClosed
	case transportNew:
		t.runStarted = true
		t.state = transportRunning
	}
	t.mu.Unlock()
	if err := checkTransactionalStartup(ctx, t.config.OperationTimeout, t.client); err != nil {
		t.markClosed()
		failure := fmt.Errorf("messenger/kafka: transport startup: %w", err)
		t.logFailure(ctx, messenger.LogError, "Kafka transport startup failed", "transport_startup", failure)
		return failure
	}
	select {
	case <-ctx.Done():
		t.markClosed()
		return ctx.Err()
	case <-t.drain:
		t.markClosed()
		return nil
	}
}

// Readiness checks managed state, broker connectivity, and transactional producer health.
func (t *Transport) Readiness(ctx context.Context) error {
	t.mu.Lock()
	state := t.state
	t.mu.Unlock()
	if state != transportRunning {
		return messenger.ErrRuntimeNotRunning
	}
	if err := checkTransactionalReadiness(ctx, t.client); err != nil {
		failure := fmt.Errorf("messenger/kafka: transport readiness: %w", err)
		t.logFailure(ctx, messenger.LogWarn, "Kafka transport readiness failed", "transport_readiness", failure)
		return failure
	}
	return nil
}

// BeginDrain synchronously rejects new route deliveries.
func (t *Transport) BeginDrain() {
	t.mu.Lock()
	if t.state == transportNew || t.state == transportRunning {
		t.state = transportDraining
	}
	t.mu.Unlock()
	t.drainOnce.Do(func() { close(t.drain) })
}

// Shutdown drains, waits for Run when started, and closes broker clients.
func (t *Transport) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	started := t.runStarted
	if !started {
		t.state = transportClosed
	}
	t.mu.Unlock()
	t.BeginDrain()
	if started {
		select {
		case <-t.done:
		case <-ctx.Done():
			t.closeClient()
			return ctx.Err()
		}
	} else {
		t.closeDone()
	}
	if err := t.waitForTransactions(ctx); err != nil {
		t.closeClient()
		return err
	}
	t.closeClient()
	return nil
}

func (t *Transport) acquireTransaction(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil transaction admission context", ErrInvalidConfig)
	}
	select {
	case t.txGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-t.txGate
		return err
	}
	t.mu.Lock()
	var err error
	switch t.state {
	case transportRunning:
		t.txActive = true
		t.txDone = make(chan struct{})
	case transportClosed:
		err = ErrTransportClosed
	case transportNew, transportDraining:
		err = messenger.ErrRuntimeNotRunning
	default:
		err = messenger.ErrRuntimeNotRunning
	}
	t.mu.Unlock()
	if err != nil {
		<-t.txGate
	}
	return err
}

func (t *Transport) releaseTransaction() {
	t.mu.Lock()
	t.txActive = false
	close(t.txDone)
	t.txDone = nil
	t.mu.Unlock()
	<-t.txGate
}

func (t *Transport) waitForTransactions(ctx context.Context) error {
	t.mu.Lock()
	if !t.txActive {
		t.mu.Unlock()
		return nil
	}
	done := t.txDone
	t.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Transport) closeClient() {
	t.clientCloseOnce.Do(t.client.Close)
}

func (t *Transport) ensureRunning() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case transportRunning:
		return nil
	case transportClosed:
		return ErrTransportClosed
	case transportNew, transportDraining:
		return messenger.ErrRuntimeNotRunning
	default:
		return messenger.ErrRuntimeNotRunning
	}
}

func (t *Transport) markClosed() {
	t.mu.Lock()
	t.state = transportClosed
	t.mu.Unlock()
	t.closeDone()
}

func (t *Transport) closeDone() {
	t.doneOnce.Do(func() { close(t.done) })
}

func (t *Transport) workerOptions(groupID, instanceID, transactionID string, topics []string) []kgo.Opt {
	required := append(requiredProducerOptions(t.config, transactionID),
		kgo.ConsumerGroup(groupID),
		kgo.InstanceID(instanceID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	)
	return appendRequiredOptions(t.baseOpts, required...)
}

func requiredProducerOptions(config TransportConfig, transactionID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.TransactionalID(transactionID),
		kgo.TransactionTimeout(config.OperationTimeout),
		kgo.ProducerBatchMaxBytesFn(producerBatchMaxBytes),
	}
}

func producerBatchMaxBytes(topic string) int32 {
	if strings.Contains(topic, serviceMarker) && strings.HasSuffix(topic, ".dlq") {
		return int32(DefaultMaxDLQMessageBytes)
	}
	return int32(DefaultMaxSourceMessageBytes)
}

func appendRequiredOptions(base []kgo.Opt, required ...kgo.Opt) []kgo.Opt {
	opts := make([]kgo.Opt, 0, len(base)+len(required))
	opts = append(opts, base...)
	return append(opts, required...)
}

func applyTransportDefaults(config *TransportConfig) {
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultBrokerOperationTimeout
	}
	if nilValue(config.Logger) {
		config.Logger = messenger.AdaptSlog(nil)
	}
}

func validateTransportConfig(config TransportConfig) error {
	if config.Name == "" || config.ClientID == "" || len(config.Brokers) == 0 ||
		config.OperationTimeout <= 0 || len(config.ConnectionOptions) > 64 {
		return fmt.Errorf("%w: transport", ErrInvalidConfig)
	}
	if err := validateKafkaToken("transport name", config.Name); err != nil {
		return err
	}
	if err := validateKafkaToken("client ID", config.ClientID); err != nil {
		return err
	}
	if err := validateInstanceID(config.InstanceID); err != nil {
		return err
	}
	for _, broker := range config.Brokers {
		if broker == "" {
			return fmt.Errorf("%w: empty broker", ErrInvalidConfig)
		}
	}
	for _, option := range config.ConnectionOptions {
		if nilValue(option.option) {
			return fmt.Errorf("%w: invalid Kafka connection option", ErrInvalidConfig)
		}
	}
	return nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (t *Transport) brokerContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(parent), t.config.OperationTimeout)
}

// PublishReplay commits one protected replay-ingress record and returns its
// broker-assigned topic, partition, and offset metadata.
func (t *Transport) PublishReplay(ctx context.Context, record *kgo.Record) (*kgo.Record, error) {
	if ctx == nil || record == nil || record.Topic == "" {
		return nil, fmt.Errorf("%w: replay record", ErrInvalidConfig)
	}
	if err := t.ensureRunning(); err != nil {
		return nil, err
	}
	canonical, err := validateProtectedReplayRecord(record)
	if err != nil {
		return nil, err
	}
	if err := t.acquireTransaction(ctx); err != nil {
		return nil, err
	}
	defer t.releaseTransaction()
	brokerCtx, cancel := t.brokerContext(ctx)
	defer cancel()
	if err := t.client.BeginTransaction(); err != nil {
		failure := fmt.Errorf("messenger/kafka: begin replay transaction: %w", err)
		t.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "replay_begin", failure,
			messenger.LogAttr{Key: logAttrTopic, Value: record.Topic})
		return nil, failure
	}
	cloned := newReplayRecord(record, canonical)
	if err := t.client.ProduceSync(brokerCtx, cloned).FirstErr(); err != nil {
		abortErr := abortTransaction(ctx, t.config.OperationTimeout, t.client)
		failure := fmt.Errorf("messenger/kafka: publish replay: %w", err)
		t.logTransactionFailure(ctx, "replay_produce", failure, abortErr,
			messenger.LogAttr{Key: logAttrTopic, Value: record.Topic})
		return nil, errors.Join(failure, abortErr)
	}
	if err := t.client.EndTransaction(brokerCtx, kgo.TryCommit); err != nil {
		abortErr := abortTransaction(ctx, t.config.OperationTimeout, t.client)
		failure := fmt.Errorf("messenger/kafka: commit replay: %w", err)
		t.logTransactionFailure(ctx, "replay_commit", failure, abortErr,
			messenger.LogAttr{Key: logAttrTopic, Value: record.Topic})
		return nil, errors.Join(failure, abortErr)
	}
	return cloned, nil
}

func newReplayRecord(record *kgo.Record, canonical []byte) *kgo.Record {
	// Protected replay is a new broker publication. Logical creation time stays
	// inside canonical, while a zero timestamp lets franz-go stamp this ingress.
	return &kgo.Record{
		Topic:   record.Topic,
		Key:     append([]byte(nil), record.Key...),
		Value:   append([]byte(nil), canonical...),
		Headers: append([]kgo.RecordHeader(nil), record.Headers...),
	}
}

func validateProtectedReplayRecord(record *kgo.Record) ([]byte, error) {
	control, err := decodeControlHeaders(record.Headers)
	if err != nil || !strings.HasPrefix(control.attemptGeneration, replayIDPrefix) {
		return nil, fmt.Errorf("%w: protected Kafka replay headers", messenger.ErrInvalidMessage)
	}
	prefix := control.source.topic + serviceMarker
	const suffix = ".replay"
	if !strings.HasPrefix(record.Topic, prefix) || !strings.HasSuffix(record.Topic, suffix) {
		return nil, fmt.Errorf("%w: protected Kafka replay topic", messenger.ErrInvalidMessage)
	}
	consumerID := strings.TrimSuffix(strings.TrimPrefix(record.Topic, prefix), suffix)
	expected, err := replayTopic(control.source.topic, consumerID)
	if err != nil || expected != record.Topic {
		return nil, fmt.Errorf("%w: protected Kafka replay topic", messenger.ErrInvalidMessage)
	}
	canonical, err := messenger.CanonicalizeEnvelope(record.Value)
	if err != nil {
		return nil, err
	}
	envelope, err := messenger.UnmarshalEnvelope(canonical)
	if err != nil {
		return nil, err
	}
	if err := sourceTopicMatchesMetadata(control.source.topic, envelope.Metadata()); err != nil {
		return nil, err
	}
	if err := validateRecordKey(record.Key, envelope.Metadata()); err != nil {
		return nil, err
	}
	return canonical, nil
}

type transactionEnder interface {
	EndTransaction(ctx context.Context, operation kgo.TransactionEndTry) error
}

type transactionalReadinessClient interface {
	Ping(ctx context.Context) error
	ProducerID(ctx context.Context) (int64, int16, error)
}

func checkTransactionalStartup(
	parent context.Context,
	timeout time.Duration,
	client transactionalReadinessClient,
) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return checkTransactionalReadiness(ctx, client)
}

func checkTransactionalReadiness(ctx context.Context, client transactionalReadinessClient) error {
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("ping broker: %w", err)
	}
	if _, _, err := client.ProducerID(ctx); err != nil {
		return fmt.Errorf("load transactional producer ID: %w", err)
	}
	return nil
}

func abortTransaction(parent context.Context, timeout time.Duration, client transactionEnder) error {
	if client == nil {
		return errors.New("messenger/kafka: nil transactional client")
	}
	if parent == nil {
		return errors.New("messenger/kafka: nil transaction cleanup context")
	}
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	return client.EndTransaction(cleanupContext, kgo.TryAbort)
}

var (
	_ messenger.Service = (*Transport)(nil)
	_ ReplayPublisher   = (*Transport)(nil)
)
