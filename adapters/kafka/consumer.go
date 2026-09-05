package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultConsumerRebalanceTimeout   = time.Minute
	defaultBatchRebalanceSafetyMargin = 5 * time.Second
	maxKafkaRebalanceTimeout          = time.Duration(math.MaxInt32) * time.Millisecond
)

var errTransactionNotCommitted = errors.New("messenger/kafka: transaction was aborted")

// HandlerConfig declares one durable Kafka consumer and its bounded retry policy.
type HandlerConfig struct {
	Namespace           string
	ConsumerID          string
	Concurrency         int
	Timeout             time.Duration
	FinalizationTimeout time.Duration
	MaxAttempts         int
	BaseRetry           time.Duration
	MaxRetry            time.Duration
	RetryTiers          []time.Duration
	Logger              messenger.Logger
	Observers           []messenger.Observer
	Middlewares         []messenger.Middleware
	Propagator          messenger.ContextPropagator
	PanicReporter       PanicReporter
	FailureSanitizer    FailureSanitizer
}

type decodedMessage struct {
	metadata  messenger.Metadata
	canonical []byte
	value     any
	handle    func(context.Context) error
}

type preparedEnvelope struct {
	envelope messenger.Envelope
}

type decoder func(preparedEnvelope) (decodedMessage, error)

type consumerState uint8

const (
	consumerNew consumerState = iota
	consumerRunning
	consumerDraining
	consumerClosed
)

// Consumer is a native-envelope transactional Kafka consumer implementing messenger.Service.
// Consumer-only hosts must register its shared Transport as a separate managed service.
type Consumer struct {
	transport  *Transport
	store      *inbox.Store
	config     HandlerConfig
	descriptor messenger.DescriptorInfo
	decode     decoder
	clock      func() time.Time
	batch      *kafkaBatchConsumer

	sourceTopic string
	groupID     string
	retryTopics []string
	retrySet    map[string]struct{}
	replayTopic string
	dlqTopic    string
	topics      []string

	mu                  sync.Mutex
	state               consumerState
	runStarted          bool
	forceCancel         context.CancelFunc
	drain               chan struct{}
	drainOnce           sync.Once
	done                chan struct{}
	doneOnce            sync.Once
	readyWorkers        int
	workersReady        bool
	workerErr           error
	batchBackoffWorkers int
	batchLastError      error
	workerReady         map[int]bool

	startupCheck func(context.Context) error
	workerRun    func(context.Context, int, func()) error
}

// NewCommandConsumer constructs a native-envelope durable command consumer.
func NewCommandConsumer[T any](
	transport *Transport,
	store *inbox.Store,
	descriptor messenger.Command[T],
	handler messenger.Handler[T],
	config HandlerConfig,
) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil command handler", ErrInvalidConfig)
	}
	decode := func(prepared preparedEnvelope) (decodedMessage, error) {
		metadata := prepared.envelope.Metadata()
		payloadBytes, encoding, err := prepared.envelope.Payload()
		if err != nil {
			return decodedMessage{}, err
		}
		canonical, err := messenger.MarshalEnvelope(metadata, payloadBytes, encoding)
		if err != nil {
			return decodedMessage{}, err
		}
		payload, err := messenger.DecodeCommandPayload(descriptor, payloadBytes)
		if err != nil {
			return decodedMessage{}, err
		}
		message := messenger.Message[T]{Metadata: metadata, Payload: payload}
		return decodedMessage{
			metadata: message.Metadata, canonical: canonical, value: message,
			handle: func(ctx context.Context) error { return callHandler(ctx, handler, message) },
		}, nil
	}
	return newConsumer(transport, store, descriptor.Info(), config, decode)
}

// NewEventConsumer constructs a native-envelope durable event consumer.
func NewEventConsumer[T any](
	transport *Transport,
	store *inbox.Store,
	descriptor messenger.Event[T],
	handler messenger.Handler[T],
	config HandlerConfig,
) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil event handler", ErrInvalidConfig)
	}
	decode := func(prepared preparedEnvelope) (decodedMessage, error) {
		metadata := prepared.envelope.Metadata()
		payloadBytes, encoding, err := prepared.envelope.Payload()
		if err != nil {
			return decodedMessage{}, err
		}
		canonical, err := messenger.MarshalEnvelope(metadata, payloadBytes, encoding)
		if err != nil {
			return decodedMessage{}, err
		}
		payload, err := messenger.DecodeEventPayload(descriptor, payloadBytes)
		if err != nil {
			return decodedMessage{}, err
		}
		message := messenger.Message[T]{Metadata: metadata, Payload: payload}
		return decodedMessage{
			metadata: message.Metadata, canonical: canonical, value: message,
			handle: func(ctx context.Context) error { return callHandler(ctx, handler, message) },
		}, nil
	}
	return newConsumer(transport, store, descriptor.Info(), config, decode)
}

func newConsumer(
	transport *Transport,
	store *inbox.Store,
	descriptor messenger.DescriptorInfo,
	config HandlerConfig,
	decode decoder,
) (*Consumer, error) {
	applyConsumerDefaults(&config)
	if err := applyObservabilityDefaults(&config); err != nil {
		return nil, err
	}
	if transport == nil || store == nil || decode == nil || !store.SupportsAttempts() ||
		config.Concurrency < 1 || config.Concurrency > 128 || config.Timeout <= 0 ||
		config.FinalizationTimeout <= 0 || config.MaxAttempts <= 0 || config.BaseRetry <= 0 ||
		config.MaxRetry < config.BaseRetry || len(config.RetryTiers) == 0 || len(config.RetryTiers) > 16 {
		return nil, fmt.Errorf("%w: durable consumer", ErrInvalidConfig)
	}
	if err := validateRetryTiers(config.RetryTiers); err != nil {
		return nil, err
	}
	source, err := Topic(config.Namespace, descriptor)
	if err != nil {
		return nil, err
	}
	group, err := consumerGroup(source, config.ConsumerID)
	if err != nil {
		return nil, err
	}
	retries := make([]string, len(config.RetryTiers))
	retrySet := make(map[string]struct{}, len(retries))
	for index := range retries {
		retries[index], err = retryTopic(source, config.ConsumerID, index)
		if err != nil {
			return nil, err
		}
		retrySet[retries[index]] = struct{}{}
	}
	replay, err := replayTopic(source, config.ConsumerID)
	if err != nil {
		return nil, err
	}
	dlq, err := dlqTopic(source, config.ConsumerID)
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, 2+len(retries))
	topics = append(topics, source)
	topics = append(topics, retries...)
	topics = append(topics, replay)
	return &Consumer{
		transport: transport, store: store, config: config, descriptor: descriptor, decode: decode, clock: time.Now,
		sourceTopic: source, groupID: group, retryTopics: retries, retrySet: retrySet,
		replayTopic: replay, dlqTopic: dlq, topics: topics,
		state: consumerNew, drain: make(chan struct{}), done: make(chan struct{}),
	}, nil
}

func applyConsumerDefaults(config *HandlerConfig) {
	if config.Concurrency == 0 {
		config.Concurrency = 1
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.FinalizationTimeout == 0 {
		config.FinalizationTimeout = 5 * time.Second
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 10
	}
	if config.BaseRetry == 0 {
		config.BaseRetry = time.Second
	}
	if config.MaxRetry == 0 {
		config.MaxRetry = 5 * time.Minute
	}
	if len(config.RetryTiers) == 0 {
		config.RetryTiers = []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}
	} else {
		config.RetryTiers = append([]time.Duration(nil), config.RetryTiers...)
	}
}

func validateRetryTiers(tiers []time.Duration) error {
	for index, tier := range tiers {
		if tier <= 0 || index > 0 && tier <= tiers[index-1] {
			return fmt.Errorf("%w: retry tiers must be positive and strictly increasing", ErrInvalidConfig)
		}
	}
	return nil
}

// Run starts transactional workers and blocks until drain, cancellation, or a fatal broker failure.
func (c *Consumer) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil consumer context", ErrInvalidConfig)
	}
	c.mu.Lock()
	switch c.state {
	case consumerRunning, consumerDraining:
		if c.state == consumerDraining && !c.runStarted {
			c.runStarted = true
			c.state = consumerClosed
			c.mu.Unlock()
			c.closeDone()
			return nil
		}
		c.mu.Unlock()
		return messenger.ErrRuntimeRunning
	case consumerClosed:
		c.mu.Unlock()
		return ErrConsumerClosed
	case consumerNew:
		c.runStarted = true
		c.state = consumerRunning
	}
	runContext, cancel := context.WithCancel(ctx)
	c.forceCancel = cancel
	c.mu.Unlock()
	startupCheck := c.startupCheck
	if startupCheck == nil {
		startupCheck = c.ensureTopics
	}
	if err := startupCheck(runContext); err != nil {
		cancel()
		c.markClosed()
		return fmt.Errorf("messenger/kafka: consumer startup: %w", err)
	}

	results := make(chan error, c.config.Concurrency)
	var workers sync.WaitGroup
	workerRun := c.workerRun
	if workerRun == nil {
		workerRun = c.runWorker
	}
	for worker := range c.config.Concurrency {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			err := workerRun(runContext, index, func() {
				c.setWorkerReady(index, true)
			})
			c.setWorkerReady(index, false)
			c.recordWorkerError(runContext, err)
			results <- err
		}(worker)
	}
	var runErr error
	for range c.config.Concurrency {
		err := <-results
		if err != nil && !errors.Is(err, context.Canceled) && runErr == nil {
			runErr = err
			cancel()
		}
	}
	workers.Wait()
	c.markClosed()
	return runErr
}

// Readiness verifies transactional workers and broker connectivity.
// Expensive topology checks are exposed through DeepHealth.
func (c *Consumer) Readiness(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil readiness context", ErrInvalidConfig)
	}
	c.mu.Lock()
	state := c.state
	workersReady := c.workersReady
	workerErr := c.workerErr
	backoffWorkers := c.batchBackoffWorkers
	batchErr := c.batchLastError
	c.mu.Unlock()
	if state != consumerRunning {
		return messenger.ErrRuntimeNotRunning
	}
	if workerErr != nil {
		return fmt.Errorf("messenger/kafka: consumer worker failed: %w", workerErr)
	}
	if !workersReady || backoffWorkers != 0 {
		if batchErr != nil {
			return fmt.Errorf("%w: Kafka consumer workers are backing off: %w", messenger.ErrRuntimeNotRunning, batchErr)
		}
		return fmt.Errorf("%w: Kafka consumer workers are not ready", messenger.ErrRuntimeNotRunning)
	}
	if c.transport != nil && c.transport.client != nil {
		if err := c.transport.client.Ping(ctx); err != nil {
			return fmt.Errorf("messenger/kafka: consumer readiness: %w", err)
		}
	}
	return nil
}

// DeepHealth verifies the full declared Kafka topic contract.
func (c *Consumer) DeepHealth(ctx context.Context) error {
	if err := c.Readiness(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	backoffWorkers, batchErr := c.batchBackoffWorkers, c.batchLastError
	c.mu.Unlock()
	if backoffWorkers != 0 {
		return fmt.Errorf("messenger/kafka: %d batch workers are recreating or backing off: %w",
			backoffWorkers, batchErr)
	}
	return c.ensureTopics(ctx)
}

// BeginDrain stops workers from polling new records while in-flight work completes.
func (c *Consumer) BeginDrain() {
	c.mu.Lock()
	if c.state == consumerNew || c.state == consumerRunning {
		c.state = consumerDraining
	}
	c.mu.Unlock()
	c.drainOnce.Do(func() { close(c.drain) })
}

// Shutdown drains and waits for Run, force-cancelling only when ctx expires.
func (c *Consumer) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil shutdown context", ErrInvalidConfig)
	}
	c.mu.Lock()
	started := c.runStarted
	cancel := c.forceCancel
	if !started {
		c.state = consumerClosed
	}
	c.mu.Unlock()
	c.BeginDrain()
	if !started {
		c.closeDone()
		return nil
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func (c *Consumer) runWorker(ctx context.Context, index int, ready func()) error {
	if c.batch != nil {
		return c.runKafkaBatchWorkerSupervisor(ctx, index, ready)
	}
	transactionID, err := transactionalID(c.groupID, c.transport.config.InstanceID, index+1)
	if err != nil {
		return err
	}
	instanceID, err := groupInstanceID(c.transport.config.InstanceID, index+1)
	if err != nil {
		return err
	}
	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	opts := c.transport.workerOptions(c.groupID, instanceID, transactionID, c.topics)
	opts = append(opts,
		kgo.WithContext(sessionCtx),
		kgo.RebalanceTimeout(consumerRebalanceTimeout(c.transport.config.OperationTimeout)),
	)
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: create consumer worker %d: %w", index, err)
	}
	defer session.CloseAllowingRebalance()
	if err := checkTransactionalStartup(
		sessionCtx, c.transport.config.OperationTimeout, session.Client(),
	); err != nil {
		return fmt.Errorf("messenger/kafka: worker %d startup: %w", index, err)
	}
	return c.runWorkerSession(
		sessionCtx,
		franzConsumerSession{session: session},
		ready,
		c.prepareRecord,
		c.processPreparedRecord,
	)
}

type preparedRecord struct {
	control     controlMetadata
	envelope    preparedEnvelope
	observedAt  time.Time
	retryAt     time.Time
	failureKind string
	failure     error
	attempt     uint64
	messageID   string
}

type consumerRecordPreflight func(*kgo.Record) preparedRecord

type consumerRecordProcessor func(
	context.Context,
	transactionalConsumerSession,
	*kgo.Record,
	preparedRecord,
) error

func (c *Consumer) runWorkerSession(
	ctx context.Context,
	session transactionalConsumerSession,
	ready func(),
	prepare consumerRecordPreflight,
	process consumerRecordProcessor,
) error {
	scheduler := newRetryPartitionScheduler()
	clock := c.clock
	if clock == nil {
		clock = time.Now
	}
	markReady := func() {
		if ready == nil || ctx.Err() != nil || !consumerGroupJoined(session) {
			return
		}
		ready()
		ready = nil
	}
	markReady()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.drain:
			return nil
		default:
		}
		now := clock().UTC()
		if due := scheduler.releaseDue(now); len(due) > 0 {
			session.ResumeFetchPartitions(due)
		}
		pollTimeout := scheduler.pollTimeout(now, time.Second)
		if pollTimeout <= 0 {
			continue
		}
		poll, err := c.pollWorkerRecord(ctx, session, pollTimeout)
		if err != nil {
			return err
		}
		if poll.drained {
			return nil
		}
		markReady()
		if poll.record == nil {
			continue
		}
		record := poll.record
		prepared := prepare(record)
		if !prepared.retryAt.IsZero() {
			if err := c.deferRetryPartition(ctx, session, scheduler, record, prepared.retryAt); err != nil {
				return err
			}
			continue
		}
		session.AllowRebalance()
		err = process(ctx, session, record, prepared)
		if err == nil || errors.Is(err, errTransactionNotCommitted) {
			continue
		}
		return err
	}
}

type workerPoll struct {
	record  *kgo.Record
	drained bool
}

func (c *Consumer) pollWorkerRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	timeout time.Duration,
) (workerPoll, error) {
	pollContext, cancel := context.WithTimeout(ctx, timeout)
	fetches := session.PollRecords(pollContext, 1)
	pollErr := pollContext.Err()
	cancel()
	if err := fetchError(fetches, pollErr); err != nil {
		session.AllowRebalance()
		return workerPoll{}, fmt.Errorf("messenger/kafka: consume %s: %w", c.config.ConsumerID, err)
	}
	if c.drainRequested() {
		session.AllowRebalance()
		return workerPoll{drained: true}, nil
	}
	iterator := fetches.RecordIter()
	if !iterator.Done() {
		return workerPoll{record: iterator.Next()}, nil
	}
	session.AllowRebalance()
	if pollErr != nil && !errors.Is(pollErr, context.DeadlineExceeded) && ctx.Err() != nil {
		return workerPoll{}, ctx.Err()
	}
	return workerPoll{}, nil
}

func (c *Consumer) deferRetryPartition(
	ctx context.Context,
	session transactionalConsumerSession,
	scheduler *retryPartitionScheduler,
	record *kgo.Record,
	deadline time.Time,
) error {
	partition, ownsPause, err := pauseAndRewindRetryPartition(session, record)
	if err != nil {
		session.AllowRebalance()
		return err
	}
	scheduler.schedule(partition, deadline, ownsPause)
	session.AllowRebalance()
	c.logDeferredPartition(ctx, record, deadline)
	return nil
}

func (c *Consumer) drainRequested() bool {
	select {
	case <-c.drain:
		return true
	default:
		return false
	}
}

type consumerGroupMetadataReader interface {
	GroupMetadata() (string, int32)
}

type transactionalConsumerSession interface {
	consumerGroupMetadataReader
	retryPartitionSession
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	Begin() error
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	End(ctx context.Context, operation kgo.TransactionEndTry) (bool, error)
	CloseAllowingRebalance()
}

type franzConsumerSession struct {
	session *kgo.GroupTransactSession
}

func (s franzConsumerSession) GroupMetadata() (string, int32) {
	return s.session.Client().GroupMetadata()
}

func (s franzConsumerSession) AllowRebalance() {
	s.session.AllowRebalance()
}

func (s franzConsumerSession) CloseAllowingRebalance() {
	s.session.CloseAllowingRebalance()
}

func (s franzConsumerSession) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
	return s.session.Client().PauseFetchPartitions(topicPartitions)
}

func (s franzConsumerSession) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	s.session.Client().ResumeFetchPartitions(topicPartitions)
}

func (s franzConsumerSession) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	s.session.Client().SetOffsets(offsets)
}

func (s franzConsumerSession) CommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	return s.session.Client().CommittedOffsets()
}

func (s franzConsumerSession) UncommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	return s.session.Client().UncommittedOffsets()
}

func (s franzConsumerSession) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	return s.session.PollRecords(ctx, maxRecords)
}

func (s franzConsumerSession) Begin() error {
	return s.session.Begin()
}

func (s franzConsumerSession) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	return s.session.ProduceSync(ctx, records...)
}

func (s franzConsumerSession) End(ctx context.Context, operation kgo.TransactionEndTry) (bool, error) {
	return s.session.End(ctx, operation)
}

func consumerGroupJoined(client consumerGroupMetadataReader) bool {
	memberID, generation := client.GroupMetadata()
	return memberID != "" && generation >= 0
}

func (c *Consumer) markWorkerReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != consumerRunning || c.workersReady {
		return
	}
	if c.workerReady == nil {
		c.workerReady = make(map[int]bool)
	}
	c.readyWorkers++
	c.workerReady[c.readyWorkers-1] = true
	if (c.readyWorkers == c.config.Concurrency || len(c.workerReady) == c.config.Concurrency) &&
		c.batchBackoffWorkers == 0 {
		c.workersReady = true
	}
}

func (c *Consumer) setWorkerReady(index int, ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.workerReady == nil {
		c.workerReady = make(map[int]bool)
	}
	if ready {
		c.workerReady[index] = true
	} else {
		delete(c.workerReady, index)
	}
	if c.state != consumerRunning || c.batchBackoffWorkers != 0 {
		c.workersReady = false
		return
	}
	c.workersReady = len(c.workerReady) == c.config.Concurrency
}

func (c *Consumer) recordWorkerError(ctx context.Context, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	c.mu.Lock()
	if c.workerErr != nil {
		c.mu.Unlock()
		return
	}
	c.workerErr = err
	c.mu.Unlock()
	if c.transport != nil {
		c.transport.logFailure(context.WithoutCancel(ctx), messenger.LogError, "Kafka consumer worker failed",
			"consumer_worker", err,
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID})
	}
}

func consumerRebalanceTimeout(operationTimeout time.Duration) time.Duration {
	return max(defaultConsumerRebalanceTimeout, operationTimeout)
}

// batchConsumerRebalanceTimeout sizes rebalance timeout covering MaxWait, handler
// transaction timeout, and up to two broker operation timeouts (to cover ProduceSync
// failure followed by TryAbort), plus safety margin. It assumes application codec
// execution has bounded runtime.
func batchConsumerRebalanceTimeout(
	batchMaxWait time.Duration,
	handlerTimeout time.Duration,
	finalizationTimeout time.Duration,
	operationTimeout time.Duration,
) (time.Duration, error) {
	if batchMaxWait < 0 || handlerTimeout < 0 || finalizationTimeout < 0 || operationTimeout < 0 {
		return 0, fmt.Errorf("%w: negative timeout in batch rebalance calculation", ErrInvalidConfig)
	}
	handlerTx := handlerTransactionTimeout(handlerTimeout, finalizationTimeout)
	if batchMaxWait > maxKafkaRebalanceTimeout {
		return 0, fmt.Errorf("%w: batch MaxWait %s exceeds maximum Kafka rebalance timeout %s",
			ErrInvalidConfig, batchMaxWait, maxKafkaRebalanceTimeout)
	}
	remaining := maxKafkaRebalanceTimeout - batchMaxWait
	if handlerTx > remaining {
		return 0, fmt.Errorf("%w: batch handler transaction timeout %s exceeds maximum Kafka rebalance timeout %s",
			ErrInvalidConfig, handlerTx, maxKafkaRebalanceTimeout)
	}
	remaining -= handlerTx
	totalBrokerTimeout := 2 * operationTimeout
	if totalBrokerTimeout > remaining {
		return 0, fmt.Errorf("%w: batch broker operation timeout %s exceeds maximum Kafka rebalance timeout %s",
			ErrInvalidConfig, totalBrokerTimeout, maxKafkaRebalanceTimeout)
	}
	remaining -= totalBrokerTimeout
	if defaultBatchRebalanceSafetyMargin > remaining {
		return 0, fmt.Errorf("%w: batch rebalance budget exceeds maximum Kafka rebalance timeout %s",
			ErrInvalidConfig, maxKafkaRebalanceTimeout)
	}
	budget := batchMaxWait + handlerTx + totalBrokerTimeout + defaultBatchRebalanceSafetyMargin
	return max(defaultConsumerRebalanceTimeout, budget), nil
}

func (c *Consumer) prepareRecord(record *kgo.Record) preparedRecord {
	control, controlErr := parseControl(record, c.sourceTopic, c.replayTopic, c.retrySet)
	if controlErr != nil {
		return preparedRecord{
			control: sourceControl(record), failureKind: "control", failure: controlErr, attempt: 1,
		}
	}
	if control.source.topic != c.sourceTopic {
		return preparedRecord{
			control: control, failureKind: "control",
			failure: fmt.Errorf("%w: source topic mismatch", messenger.ErrInvalidMessage),
			attempt: max(1, control.attempt),
		}
	}
	now := c.clock().UTC()
	envelope, err := messenger.UnmarshalEnvelope(record.Value)
	if err != nil {
		return preparedRecord{
			control: control, observedAt: now, failureKind: failureKindDecode, failure: err,
			attempt: max(1, control.attempt),
		}
	}
	metadata := envelope.Metadata()
	messageID := metadata.ID.String()
	if err := validateEnvelopeDescriptor(envelope, c.descriptor); err != nil {
		return preparedRecord{
			control: control, observedAt: now, failureKind: failureKindDecode, failure: err,
			attempt: max(1, control.attempt), messageID: messageID,
		}
	}
	if err := validateRecordKey(record.Key, metadata); err != nil {
		return preparedRecord{
			control: control, observedAt: now, failureKind: "identity_conflict", failure: err,
			attempt: max(1, control.attempt), messageID: messageID,
		}
	}
	retryAt, timingErr := retryDue(control.notBefore, metadata.ExpiresAt, now)
	if timingErr != nil {
		return preparedRecord{
			control: control, observedAt: now, failureKind: "expired", failure: timingErr,
			attempt: max(1, control.attempt), messageID: messageID,
		}
	}
	if !retryAt.IsZero() {
		return preparedRecord{control: control, observedAt: now, retryAt: retryAt}
	}
	return preparedRecord{
		control:    control,
		envelope:   preparedEnvelope{envelope: envelope},
		observedAt: now,
	}
}

func validateEnvelopeDescriptor(envelope messenger.Envelope, descriptor messenger.DescriptorInfo) error {
	if envelope.Kind != descriptor.Kind || envelope.Name != descriptor.Name ||
		envelope.SchemaVersion != descriptor.SchemaVersion || envelope.ContentType != descriptor.ContentType ||
		envelope.DataEncoding != descriptor.DataEncoding || envelope.Schema != descriptor.Schema {
		return fmt.Errorf("%w: envelope does not match %s v%d",
			messenger.ErrDescriptorConflict, descriptor.Name, descriptor.SchemaVersion)
	}
	return nil
}

func retryDue(notBefore, expiresAt, now time.Time) (time.Time, error) {
	if !expiresAt.IsZero() && !expiresAt.After(now) {
		return time.Time{}, ErrMessageExpired
	}
	if notBefore.IsZero() || !notBefore.After(now) {
		return time.Time{}, nil
	}
	if !expiresAt.IsZero() && !expiresAt.After(notBefore) {
		return time.Time{}, ErrMessageExpired
	}
	return notBefore, nil
}

func (c *Consumer) processPreparedRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	record *kgo.Record,
	prepared preparedRecord,
) error {
	if prepared.failure != nil {
		return c.deadLetterRecord(
			ctx,
			session,
			record,
			prepared.control,
			prepared.failureKind,
			prepared.failure,
			prepared.attempt,
			prepared.messageID,
		)
	}
	control := prepared.control
	decoded, err := c.decode(prepared.envelope)
	if err != nil {
		return c.deadLetterRecord(ctx, session, record, control, failureKindDecode, err,
			max(1, control.attempt), prepared.envelope.envelope.ID.String())
	}
	now := c.clock().UTC()
	if !decoded.metadata.ExpiresAt.IsZero() && !decoded.metadata.ExpiresAt.After(now) {
		return c.deadLetterRecord(ctx, session, record, control, "expired", ErrMessageExpired,
			max(1, control.attempt), decoded.metadata.ID.String())
	}
	if !decoded.metadata.NotBefore.IsZero() && decoded.metadata.NotBefore.After(now) {
		return c.retryRecord(ctx, session, record, decoded.canonical, control, control.attempt,
			decoded.metadata.NotBefore.Sub(now), decoded.metadata.ID)
	}
	deliveryContext := extractDeliveryContext(ctx, c.config, decoded.metadata.Headers)
	deliveryContext = messenger.ContextWithMetadata(deliveryContext, decoded.metadata)
	processContext, cancelProcess := context.WithTimeout(deliveryContext, c.config.Timeout)
	defer cancelProcess()
	transactionContext, cancelTransaction := context.WithTimeout(
		deliveryContext, handlerTransactionTimeout(c.config.Timeout, c.config.FinalizationTimeout),
	)
	defer cancelTransaction()
	key := inbox.Key{
		ConsumerID: c.config.ConsumerID, Source: decoded.metadata.Source, MessageID: decoded.metadata.ID,
		AttemptGeneration: control.attemptGeneration,
	}
	fingerprint := inbox.FingerprintEnvelope(decoded.canonical)
	startedAt := prepared.observedAt
	if startedAt.IsZero() {
		startedAt = now
	}
	maxAttempts := uint64(c.config.MaxAttempts) //nolint:gosec // Configuration validation requires a positive value.
	result, processErr := c.store.ProcessAttempt(
		transactionContext, key, fingerprint, maxAttempts,
		func(transactionHandlerContext context.Context) error {
			handlerContext := processContext
			if tx, ok := inbox.SQLTxFromContext(transactionHandlerContext); ok {
				handlerContext = inbox.ContextWithSQLTx(processContext, tx)
			}
			return invokeMiddlewares(handlerContext, decoded.metadata, c.config.ConsumerID,
				decoded.handle, c.config.Middlewares, c.config.PanicReporter)
		},
	)
	attempt := result.Attempt
	if processErr == nil {
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, nil)
		commitStarted := c.clock().UTC()
		committed, commitErr := c.commitRecord(ctx, session, nil)
		finalizationErr := commitErr
		if finalizationErr == nil && !committed {
			finalizationErr = errTransactionNotCommitted
		}
		c.observeBoundary(processContext, operationOffsetCommit, decoded.metadata.ID, commitStarted,
			finalizationErr)
		return finalizationErr
	}
	if ctx.Err() != nil {
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return ctx.Err()
	}
	exhausted := attempt >= maxAttempts || errors.Is(processErr, inbox.ErrAttemptsExhausted)
	identityConflict := errors.Is(processErr, inbox.ErrFingerprintConflict)
	if identityConflict || messenger.IsPermanent(processErr) || exhausted {
		failureKind := "permanent"
		if identityConflict {
			failureKind = "identity_conflict"
		} else if exhausted && !messenger.IsPermanent(processErr) {
			failureKind = "attempts_exhausted"
		}
		commitErr := c.deadLetterRecord(ctx, session, record, control, failureKind, processErr,
			max(1, attempt), decoded.metadata.ID.String())
		if commitErr == nil && !identityConflict {
			c.confirmTerminalHandoff(deliveryContext, key, fingerprint, decoded.metadata.ID)
		}
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return commitErr
	}
	delay, ok := messenger.DeferDelay(processErr)
	if !ok {
		delay, ok = messenger.RetryDelay(processErr)
	}
	if !ok {
		delay = retryDelay(c.config.BaseRetry, c.config.MaxRetry, attempt)
	}
	retryErr := c.retryRecord(ctx, session, record, decoded.canonical, control, attempt, delay,
		decoded.metadata.ID)
	c.observeHandle(processContext, decoded, attempt, result, delay, startedAt, processErr)
	return retryErr
}

func (c *Consumer) retryRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	record *kgo.Record,
	canonical []byte,
	control controlMetadata,
	attempt uint64,
	delay time.Duration,
	messageID messenger.MessageID,
) error {
	if delay <= 0 {
		return fmt.Errorf("%w: non-positive durable retry delay", messenger.ErrInvalidMessage)
	}
	tier := retryTier(c.config.RetryTiers, delay)
	control.attempt = attempt
	control.notBefore = c.clock().UTC().Add(delay)
	retry := &kgo.Record{
		Topic: c.retryTopics[tier], Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), canonical...),
		Headers: controlHeaders(control), Timestamp: record.Timestamp,
	}
	startedAt := c.clock().UTC()
	committed, err := c.commitRecord(ctx, session, retry)
	if err == nil && !committed {
		err = errTransactionNotCommitted
	}
	c.observeBoundary(ctx, operationRetryHandoff, messageID, startedAt, err)
	return err
}

func (c *Consumer) deadLetterRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	record *kgo.Record,
	control controlMetadata,
	failureKind string,
	failure error,
	attempt uint64,
	messageID string,
) error {
	safeFailure := sanitizeError(c.config.FailureSanitizer, failure)
	dlq := makeDLQRecord(c.config.ConsumerID, record, control, messageID, attempt, failureKind, safeFailure, c.clock())
	data, err := encodeDLQRecord(dlq)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	startedAt := c.clock().UTC()
	committed, err := c.commitRecord(ctx, session, &kgo.Record{
		Topic: c.dlqTopic, Key: digest[:], Value: data, Timestamp: dlq.FailedAt,
	})
	if err == nil && !committed {
		err = errTransactionNotCommitted
	}
	parsedID, _ := messenger.ParseMessageID(messageID)
	c.observeBoundary(ctx, operationDLQHandoff, parsedID, startedAt, err)
	return err
}

const (
	watchdogRunning int32 = iota
	watchdogCompleted
	watchdogTimedOut
)

type produceWatchdog struct {
	state atomic.Int32
	done  chan struct{}
	wait  chan struct{}
}

func startProduceWatchdog(ctx context.Context, session transactionalConsumerSession) *produceWatchdog {
	w := &produceWatchdog{
		done: make(chan struct{}),
		wait: make(chan struct{}),
	}
	w.state.Store(watchdogRunning)
	go func() {
		defer close(w.wait)
		select {
		case <-w.done:
		case <-ctx.Done():
			if w.state.CompareAndSwap(watchdogRunning, watchdogTimedOut) {
				session.CloseAllowingRebalance()
			}
		}
	}()
	return w
}

func (w *produceWatchdog) Complete() {
	w.state.CompareAndSwap(watchdogRunning, watchdogCompleted)
	close(w.done)
	<-w.wait
}

func (c *Consumer) commitRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	produced *kgo.Record,
) (bool, error) {
	var records []*kgo.Record
	if produced != nil {
		records = []*kgo.Record{produced}
	}
	committed, logAction, err := c.commitTransactionalRecords(ctx, session, records)
	if logAction != nil {
		logAction()
	}
	return committed, err
}

func (c *Consumer) commitTransactionalRecords(
	ctx context.Context,
	session transactionalConsumerSession,
	produced []*kgo.Record,
) (bool, func(), error) {
	topic := c.sourceTopic
	if len(produced) != 0 {
		topic = produced[0].Topic
	}
	logAttrs := []messenger.LogAttr{
		{Key: logAttrConsumerID, Value: c.config.ConsumerID},
		{Key: logAttrTopic, Value: topic},
	}
	if err := session.Begin(); err != nil {
		failure := fmt.Errorf("messenger/kafka: begin consumer transaction: %w", err)
		logAction := func() {
			c.transport.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "consumer_begin", failure,
				logAttrs...)
		}
		return false, logAction, failure
	}
	brokerContext, cancel := c.brokerContext(ctx)
	defer cancel()
	if len(produced) != 0 {
		watchdog := startProduceWatchdog(brokerContext, session)
		produceResults := session.ProduceSync(brokerContext, produced...)
		watchdog.Complete()
		if err := produceResults.FirstErr(); err != nil {
			abortContext, abortCancel := c.brokerContext(ctx)
			_, abortErr := session.End(abortContext, kgo.TryAbort)
			abortCancel()
			failure := fmt.Errorf("messenger/kafka: transactional handoff: %w", err)
			logAction := func() {
				c.transport.logTransactionFailure(ctx, "consumer_handoff", failure, abortErr, logAttrs...)
			}
			return false, logAction, errors.Join(failure, abortErr)
		}
	}
	committed, err := session.End(brokerContext, kgo.TryCommit)
	if err != nil {
		failure := fmt.Errorf("messenger/kafka: commit consumer transaction: %w", err)
		logAction := func() {
			c.transport.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "consumer_commit", failure,
				logAttrs...)
		}
		return false, logAction, failure
	}
	if !committed {
		logAction := func() {
			c.transport.logFailure(ctx, messenger.LogWarn, "Kafka consumer transaction aborted", "consumer_commit",
				errTransactionNotCommitted, logAttrs...)
		}
		return false, logAction, nil
	}
	return committed, nil, nil
}

// brokerContext keeps graceful drain finalization bounded while preserving
// run-context cancellation for forced shutdown and peer-service failure.
func (c *Consumer) brokerContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, c.transport.config.OperationTimeout)
}

func (c *Consumer) logDeferredPartition(ctx context.Context, record *kgo.Record, deadline time.Time) {
	attrs := []messenger.LogAttr{
		{Key: logAttrOperation, Value: "retry_defer"},
		{Key: logAttrConsumerID, Value: c.config.ConsumerID},
		{Key: logAttrTopic, Value: record.Topic},
		{Key: logAttrPartition, Value: record.Partition},
		{Key: logAttrNotBefore, Value: deadline},
	}
	if c.transport != nil {
		c.transport.logInfrastructure(ctx, messenger.LogDebug, "Kafka retry partition deferred", attrs...)
		return
	}
	logInfrastructure(ctx, c.config.Logger, messenger.LogDebug, "Kafka retry partition deferred", attrs...)
}

func handlerTransactionTimeout(handlerTimeout, finalizationTimeout time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if handlerTimeout > maxDuration-finalizationTimeout {
		return maxDuration
	}
	return handlerTimeout + finalizationTimeout
}

func (c *Consumer) confirmTerminalHandoff(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	messageID messenger.MessageID,
) {
	if !c.store.SupportsTerminalRetention() {
		return
	}
	confirmationContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.store.ConfirmTerminalHandoff(confirmationContext, key, fingerprint); err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "confirm terminal handler handoff",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrMessageID, Value: messageID.String()},
			messenger.LogAttr{Key: logAttrError, Value: err})
	}
}

func (c *Consumer) observeHandle(
	ctx context.Context,
	decoded decodedMessage,
	attempt uint64,
	result inbox.Result,
	retry time.Duration,
	startedAt time.Time,
	err error,
) {
	notifyObservers(ctx, c.config, messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: decoded.metadata.ID,
		Kind: decoded.metadata.Kind, Name: decoded.metadata.Name, SchemaVersion: decoded.metadata.SchemaVersion,
		HandlerID: c.config.ConsumerID, ConsumerID: c.config.ConsumerID, Attempt: attempt,
		Duplicate: result.Duplicate, RetryDelay: retry, StartedAt: startedAt,
		Duration: c.clock().UTC().Sub(startedAt),
		Err:      sanitizeError(c.config.FailureSanitizer, err),
	})
}

func (c *Consumer) observeBoundary(
	ctx context.Context,
	operation messenger.Operation,
	messageID messenger.MessageID,
	startedAt time.Time,
	err error,
) {
	c.observeBoundaryWithDuration(ctx, operation, messageID, startedAt, c.clock().UTC().Sub(startedAt), err)
}

func (c *Consumer) observeBoundaryWithDuration(
	ctx context.Context,
	operation messenger.Operation,
	messageID messenger.MessageID,
	startedAt time.Time,
	duration time.Duration,
	err error,
) {
	if len(c.config.Observers) == 0 {
		return
	}
	notifyObservers(ctx, c.config, messenger.Observation{
		Operation: operation, MessageID: messageID, Kind: c.descriptor.Kind,
		Name: c.descriptor.Name, SchemaVersion: c.descriptor.SchemaVersion,
		ConsumerID: c.config.ConsumerID, HandlerID: c.config.ConsumerID,
		StartedAt: startedAt, Duration: duration,
		Err: sanitizeError(c.config.FailureSanitizer, err),
	})
}

func (c *Consumer) ensureTopics(ctx context.Context) error {
	allTopics := append(slices.Clone(c.topics), c.dlqTopic)
	details, err := c.transport.admin.ListTopics(ctx, allTopics...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: list consumer topics: %w", err)
	}
	missing := make([]string, 0)
	partitions := -1
	for _, topic := range allTopics {
		detail, ok := details[topic]
		if !ok || detail.Err != nil {
			missing = append(missing, topic)
			continue
		}
		if partitions == -1 {
			partitions = len(detail.Partitions)
		} else if len(detail.Partitions) != partitions {
			return fmt.Errorf("%w: consumer topic %s has %d partitions, expected %d",
				ErrTopologyDrift, topic, len(detail.Partitions), partitions)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing consumer topics %v", ErrTopologyDrift, missing)
	}
	configs, err := c.transport.admin.DescribeTopicConfigs(ctx, c.retryTopics...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: describe retry topics: %w", err)
	}
	for _, resource := range configs {
		if resource.Err != nil {
			return fmt.Errorf("messenger/kafka: describe retry topic %s: %w", resource.Name, resource.Err)
		}
		values := make(map[string]string, len(resource.Configs))
		for _, config := range resource.Configs {
			values[config.Key] = config.MaybeValue()
		}
		if values[configRetentionMillis] != "-1" || values[configRetentionBytes] != "-1" {
			return fmt.Errorf("%w: retry topic %s must have unlimited retention", ErrTopologyDrift, resource.Name)
		}
	}
	return nil
}

func (c *Consumer) markClosed() {
	c.mu.Lock()
	c.state = consumerClosed
	c.mu.Unlock()
	c.closeDone()
}

func (c *Consumer) closeDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

func callHandler[T any](ctx context.Context, handler messenger.Handler[T], message messenger.Message[T]) error {
	return handler(ctx, message)
}

var _ messenger.Service = (*Consumer)(nil)
