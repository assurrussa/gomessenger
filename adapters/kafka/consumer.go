package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

const defaultConsumerRebalanceTimeout = time.Minute

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
}

type decodedMessage struct {
	metadata  messenger.Metadata
	canonical []byte
	handle    func(context.Context) error
}

type decoder func([]byte) (decodedMessage, error)

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

	sourceTopic string
	groupID     string
	retryTopics []string
	retrySet    map[string]struct{}
	replayTopic string
	dlqTopic    string
	topics      []string

	mu           sync.Mutex
	state        consumerState
	runStarted   bool
	forceCancel  context.CancelFunc
	drain        chan struct{}
	drainOnce    sync.Once
	done         chan struct{}
	doneOnce     sync.Once
	readyWorkers int
	workersReady bool
	workerErr    error

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
	decode := func(data []byte) (decodedMessage, error) {
		canonical, err := messenger.CanonicalizeEnvelope(data)
		if err != nil {
			return decodedMessage{}, err
		}
		message, err := messenger.DecodeCommand(descriptor, canonical)
		if err != nil {
			return decodedMessage{}, err
		}
		return decodedMessage{
			metadata: message.Metadata, canonical: canonical,
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
	decode := func(data []byte) (decodedMessage, error) {
		canonical, err := messenger.CanonicalizeEnvelope(data)
		if err != nil {
			return decodedMessage{}, err
		}
		message, err := messenger.DecodeEvent(descriptor, canonical)
		if err != nil {
			return decodedMessage{}, err
		}
		return decodedMessage{
			metadata: message.Metadata, canonical: canonical,
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
			var readyOnce sync.Once
			err := workerRun(runContext, index, func() {
				readyOnce.Do(c.markWorkerReady)
			})
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

// Readiness verifies transactional workers, broker connectivity, and declared topic presence.
func (c *Consumer) Readiness(ctx context.Context) error {
	c.mu.Lock()
	state := c.state
	workersReady := c.workersReady
	workerErr := c.workerErr
	c.mu.Unlock()
	if state != consumerRunning {
		return messenger.ErrRuntimeNotRunning
	}
	if workerErr != nil {
		return fmt.Errorf("messenger/kafka: consumer worker failed: %w", workerErr)
	}
	if !workersReady {
		return fmt.Errorf("%w: Kafka consumer workers are not ready", messenger.ErrRuntimeNotRunning)
	}
	if err := c.transport.client.Ping(ctx); err != nil {
		return fmt.Errorf("messenger/kafka: consumer readiness: %w", err)
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
	transactionID, err := transactionalID(c.groupID, c.transport.config.InstanceID, index+1)
	if err != nil {
		return err
	}
	instanceID, err := groupInstanceID(c.transport.config.InstanceID, index+1)
	if err != nil {
		return err
	}
	opts := c.transport.workerOptions(c.groupID, instanceID, transactionID, c.topics)
	opts = append(opts, kgo.RebalanceTimeout(consumerRebalanceTimeout(c.transport.config.OperationTimeout)))
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: create consumer worker %d: %w", index, err)
	}
	defer session.CloseAllowingRebalance()
	if err := checkTransactionalStartup(
		ctx, c.transport.config.OperationTimeout, session.Client(),
	); err != nil {
		return fmt.Errorf("messenger/kafka: worker %d startup: %w", index, err)
	}
	return c.runWorkerSession(ctx, franzConsumerSession{session: session}, ready, c.processRecord)
}

type consumerRecordProcessor func(context.Context, transactionalConsumerSession, *kgo.Record) error

func (c *Consumer) runWorkerSession(
	ctx context.Context,
	session transactionalConsumerSession,
	ready func(),
	process consumerRecordProcessor,
) error {
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
		pollContext, cancel := context.WithTimeout(ctx, time.Second)
		fetches := session.PollRecords(pollContext, 1)
		pollErr := pollContext.Err()
		cancel()
		if err := fetchError(fetches, pollErr); err != nil {
			return fmt.Errorf("messenger/kafka: consume %s: %w", c.config.ConsumerID, err)
		}
		if c.drainRequested() {
			return nil
		}
		markReady()
		iterator := fetches.RecordIter()
		if iterator.Done() {
			if pollErr != nil && !errors.Is(pollErr, context.DeadlineExceeded) && ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		record := iterator.Next()
		if err := process(ctx, session, record); err != nil {
			if errors.Is(err, errTransactionNotCommitted) {
				continue
			}
			return err
		}
	}
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

type fetchTopicController interface {
	PauseFetchTopics(topics ...string) []string
	ResumeFetchTopics(topics ...string)
}

type transactionalConsumerSession interface {
	consumerGroupMetadataReader
	fetchTopicController
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	Begin() error
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	End(ctx context.Context, operation kgo.TransactionEndTry) (bool, error)
}

type franzConsumerSession struct {
	session *kgo.GroupTransactSession
}

func (s franzConsumerSession) GroupMetadata() (string, int32) {
	return s.session.Client().GroupMetadata()
}

func (s franzConsumerSession) PauseFetchTopics(topics ...string) []string {
	return s.session.Client().PauseFetchTopics(topics...)
}

func (s franzConsumerSession) ResumeFetchTopics(topics ...string) {
	s.session.Client().ResumeFetchTopics(topics...)
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
	c.readyWorkers++
	if c.readyWorkers == c.config.Concurrency {
		c.workersReady = true
	}
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

func (c *Consumer) processRecord(ctx context.Context, session transactionalConsumerSession, record *kgo.Record) error {
	control, controlErr := parseControl(record, c.sourceTopic, c.replayTopic, c.retrySet)
	if controlErr != nil {
		return c.deadLetterRecord(ctx, session, record, sourceControl(record), "control", controlErr, 1, "")
	}
	if control.source.topic != c.sourceTopic {
		return c.deadLetterRecord(ctx, session, record, control, "control",
			fmt.Errorf("%w: source topic mismatch", messenger.ErrInvalidMessage), max(1, control.attempt), "")
	}
	decoded, err := c.decode(record.Value)
	if err != nil {
		return c.deadLetterRecord(ctx, session, record, control, "decode", err, max(1, control.attempt), "")
	}
	if err := validateRecordKey(record.Key, decoded.metadata); err != nil {
		return c.deadLetterRecord(ctx, session, record, control, "identity_conflict", err,
			max(1, control.attempt), decoded.metadata.ID.String())
	}
	now := c.clock().UTC()
	retryAt, timingErr := retryDue(control.notBefore, decoded.metadata.ExpiresAt, now)
	if timingErr != nil {
		return c.deadLetterRecord(ctx, session, record, control, "expired", timingErr,
			max(1, control.attempt), decoded.metadata.ID.String())
	}
	if !retryAt.IsZero() {
		if err := c.waitUntilDue(ctx, session, retryAt); err != nil {
			return err
		}
		now = c.clock().UTC()
	}
	if !decoded.metadata.ExpiresAt.IsZero() && !decoded.metadata.ExpiresAt.After(now) {
		return c.deadLetterRecord(ctx, session, record, control, "expired", ErrMessageExpired,
			max(1, control.attempt), decoded.metadata.ID.String())
	}
	if !decoded.metadata.NotBefore.IsZero() && decoded.metadata.NotBefore.After(now) {
		return c.retryRecord(ctx, session, record, decoded.canonical, control, control.attempt,
			decoded.metadata.NotBefore.Sub(now))
	}
	deliveryContext := c.config.Propagator.Extract(ctx, decoded.metadata.Headers)
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
	startedAt := now
	maxAttempts := uint64(c.config.MaxAttempts) //nolint:gosec // Configuration validation requires a positive value.
	result, processErr := c.store.ProcessAttempt(
		transactionContext, key, fingerprint, maxAttempts,
		func(transactionHandlerContext context.Context) error {
			handlerContext := processContext
			if tx, ok := inbox.SQLTxFromContext(transactionHandlerContext); ok {
				handlerContext = inbox.ContextWithSQLTx(processContext, tx)
			}
			return invokeMiddlewares(handlerContext, decoded.metadata, c.config.ConsumerID,
				decoded.handle, c.config.Middlewares)
		},
	)
	attempt := result.Attempt
	if processErr == nil {
		committed, commitErr := c.commitRecord(ctx, session, nil)
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, commitErr)
		if commitErr != nil {
			return commitErr
		}
		if !committed {
			return errTransactionNotCommitted
		}
		return nil
	}
	if ctx.Err() != nil {
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return ctx.Err()
	}
	exhausted := attempt >= maxAttempts
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
			c.forgetAttempt(deliveryContext, key, fingerprint, decoded.metadata.ID)
		}
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return commitErr
	}
	delay, ok := messenger.RetryDelay(processErr)
	if !ok {
		delay = retryDelay(c.config.BaseRetry, c.config.MaxRetry, attempt)
	}
	retryErr := c.retryRecord(ctx, session, record, decoded.canonical, control, attempt, delay)
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
	committed, err := c.commitRecord(ctx, session, retry)
	if err != nil {
		return err
	}
	if !committed {
		return errTransactionNotCommitted
	}
	return nil
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
	dlq := makeDLQRecord(c.config.ConsumerID, record, control, messageID, attempt, failureKind, failure, c.clock())
	data, err := encodeDLQRecord(dlq)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	committed, err := c.commitRecord(ctx, session, &kgo.Record{
		Topic: c.dlqTopic, Key: digest[:], Value: data, Timestamp: dlq.FailedAt,
	})
	if err != nil {
		return err
	}
	if !committed {
		return errTransactionNotCommitted
	}
	return nil
}

func (c *Consumer) commitRecord(
	ctx context.Context,
	session transactionalConsumerSession,
	produced *kgo.Record,
) (bool, error) {
	topic := c.sourceTopic
	if produced != nil {
		topic = produced.Topic
	}
	logAttrs := []messenger.LogAttr{
		{Key: logAttrConsumerID, Value: c.config.ConsumerID},
		{Key: logAttrTopic, Value: topic},
	}
	if err := session.Begin(); err != nil {
		failure := fmt.Errorf("messenger/kafka: begin consumer transaction: %w", err)
		c.transport.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "consumer_begin", failure,
			logAttrs...)
		return false, failure
	}
	brokerContext, cancel := c.brokerContext(ctx)
	defer cancel()
	if produced != nil {
		if err := session.ProduceSync(brokerContext, produced).FirstErr(); err != nil {
			abortContext, abortCancel := c.brokerContext(ctx)
			_, abortErr := session.End(abortContext, kgo.TryAbort)
			abortCancel()
			failure := fmt.Errorf("messenger/kafka: transactional handoff: %w", err)
			c.transport.logTransactionFailure(ctx, "consumer_handoff", failure, abortErr, logAttrs...)
			return false, errors.Join(failure, abortErr)
		}
	}
	committed, err := session.End(brokerContext, kgo.TryCommit)
	if err != nil {
		failure := fmt.Errorf("messenger/kafka: commit consumer transaction: %w", err)
		c.transport.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "consumer_commit", failure,
			logAttrs...)
		return false, failure
	}
	if !committed {
		c.transport.logFailure(ctx, messenger.LogWarn, "Kafka consumer transaction aborted", "consumer_commit",
			errTransactionNotCommitted, logAttrs...)
	}
	return committed, nil
}

// brokerContext keeps graceful drain finalization bounded while preserving
// run-context cancellation for forced shutdown and peer-service failure.
func (c *Consumer) brokerContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, c.transport.config.OperationTimeout)
}

func (c *Consumer) waitUntilDue(ctx context.Context, session transactionalConsumerSession, due time.Time) error {
	restore := pauseFetchTopics(session, c.topics)
	defer restore()
	for {
		remaining := due.Sub(c.clock().UTC())
		if remaining <= 0 {
			return nil
		}
		wait := min(remaining, time.Second)
		pollContext, cancel := context.WithTimeout(ctx, wait)
		fetches := session.PollRecords(pollContext, 1)
		pollErr := pollContext.Err()
		cancel()
		if err := fetchError(fetches, pollErr); err != nil {
			return fmt.Errorf("messenger/kafka: delayed retry poll: %w", err)
		}
		if !fetches.RecordIter().Done() {
			return errors.New("messenger/kafka: paused retry worker fetched an unexpected record")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.drain:
			return context.Canceled
		default:
		}
	}
}

func pauseFetchTopics(client fetchTopicController, topics []string) func() {
	previouslyPaused := client.PauseFetchTopics()
	paused := make(map[string]struct{}, len(previouslyPaused))
	for _, topic := range previouslyPaused {
		paused[topic] = struct{}{}
	}
	resume := make([]string, 0, len(topics))
	for _, topic := range topics {
		if _, ok := paused[topic]; !ok {
			resume = append(resume, topic)
		}
	}
	client.PauseFetchTopics(topics...)
	return func() { client.ResumeFetchTopics(resume...) }
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

func handlerTransactionTimeout(handlerTimeout, finalizationTimeout time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if handlerTimeout > maxDuration-finalizationTimeout {
		return maxDuration
	}
	return handlerTimeout + finalizationTimeout
}

func (c *Consumer) forgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	messageID messenger.MessageID,
) {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.store.ForgetAttempt(cleanupContext, key, fingerprint); err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "forget terminal handler attempt",
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
		Duration: c.clock().UTC().Sub(startedAt), Err: err,
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

func callHandler[T any](ctx context.Context, handler messenger.Handler[T], message messenger.Message[T]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("messenger/kafka: handler panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return handler(ctx, message)
}

var _ messenger.Service = (*Consumer)(nil)
