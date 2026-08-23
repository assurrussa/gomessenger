package nats

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// HandlerConfig declares a durable pull consumer and bounded retry behavior.
type HandlerConfig struct {
	Stream        string
	Namespace     string
	ConsumerID    string
	Description   string
	WireMode      WireMode
	Concurrency   int
	Timeout       time.Duration
	MaxAttempts   int
	BaseRetry     time.Duration
	MaxRetry      time.Duration
	AckWait       time.Duration
	DLQSubject    string
	Replicas      int
	MemoryStorage bool
	Logger        messenger.Logger
	Observers     []messenger.Observer
	Middlewares   []messenger.Middleware
	Propagator    messenger.ContextPropagator
}

type decodedMessage struct {
	metadata  messenger.Metadata
	canonical []byte
	handle    func(context.Context) error
}

type decoder func([]byte, natsio.Header, time.Time) (decodedMessage, error)

type consumerState uint8

const (
	consumerNew consumerState = iota
	consumerRunning
	consumerDraining
	consumerClosed
)

const brokerAckTimeout = 5 * time.Second

// Consumer is a durable pull consumer implementing messenger.Service.
type Consumer struct {
	connection  *natsio.Conn
	js          jetstream.JetStream
	store       *inbox.Store
	config      HandlerConfig
	maxAttempts uint64
	descriptor  messenger.DescriptorInfo
	subject     string
	decode      decoder
	clock       func() time.Time

	mu         sync.Mutex
	state      consumerState
	runStarted bool
	iterator   jetstream.MessagesContext
	done       chan struct{}
	doneOnce   sync.Once

	// beforeShutdownTransition synchronizes the locked transition in package tests.
	beforeShutdownTransition func()
}

// NewCommandConsumer constructs a native-envelope durable command consumer.
func NewCommandConsumer[T any](
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.Command[T],
	handler messenger.Handler[T],
	config HandlerConfig,
) (*Consumer, error) {
	if config.WireMode == "" {
		config.WireMode = WireNative
	}
	if config.WireMode != WireNative {
		return nil, fmt.Errorf("%w: commands require native wire mode", ErrInvalidConfig)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: nil command handler", ErrInvalidConfig)
	}
	decode := func(data []byte, _ natsio.Header, _ time.Time) (decodedMessage, error) {
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
	return newConsumer(connection, store, descriptor.Info(), config, decode)
}

// NewEventConsumer constructs a native or CloudEvents durable event consumer.
func NewEventConsumer[T any](
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.Event[T],
	handler messenger.Handler[T],
	config HandlerConfig,
) (*Consumer, error) {
	if config.WireMode == "" {
		config.WireMode = WireNative
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: nil event handler", ErrInvalidConfig)
	}
	decode := func(data []byte, headers natsio.Header, _ time.Time) (decodedMessage, error) {
		if config.WireMode == WireNative {
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
		var cloudEnvelope cloudEventEnvelope
		var err error
		switch config.WireMode {
		case WireCloudEventsStructured:
			cloudEnvelope, err = decodeCloudEventStructured(data)
		case WireCloudEventsBinary:
			cloudEnvelope, err = decodeCloudEventBinary(data, headers)
		default:
			err = fmt.Errorf("%w: wire mode %q", ErrInvalidConfig, config.WireMode)
		}
		if err != nil {
			return decodedMessage{}, err
		}
		info := descriptor.Info()
		if cloudEnvelope.metadata.Kind != info.Kind || cloudEnvelope.metadata.Name != info.Name ||
			cloudEnvelope.metadata.SchemaVersion != info.SchemaVersion ||
			cloudEnvelope.metadata.ContentType != info.ContentType || cloudEnvelope.encoding != info.DataEncoding ||
			cloudEnvelope.metadata.Schema != info.Schema {
			return decodedMessage{}, fmt.Errorf("%w: CloudEvent does not match %s v%d",
				messenger.ErrDescriptorConflict, info.Name, info.SchemaVersion)
		}
		payload, err := messenger.DecodeEventPayload(descriptor, cloudEnvelope.data)
		if err != nil {
			return decodedMessage{}, err
		}
		canonical, err := messenger.EncodeEventEnvelope(descriptor, cloudEnvelope.metadata, payload)
		if err != nil {
			return decodedMessage{}, err
		}
		message := messenger.Message[T]{Metadata: cloudEnvelope.metadata, Payload: payload}
		return decodedMessage{
			metadata: message.Metadata, canonical: canonical,
			handle: func(ctx context.Context) error { return callHandler(ctx, handler, message) },
		}, nil
	}
	return newConsumer(connection, store, descriptor.Info(), config, decode)
}

func newConsumer(
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.DescriptorInfo,
	config HandlerConfig,
	decode decoder,
) (*Consumer, error) {
	applyConsumerDefaults(&config)
	if err := applyObservabilityDefaults(&config); err != nil {
		return nil, err
	}
	if err := validateJetStreamResourceName(config.Stream); err != nil {
		return nil, fmt.Errorf("%w: stream: %w", ErrInvalidConfig, err)
	}
	if err := validateJetStreamResourceName(config.ConsumerID); err != nil {
		return nil, fmt.Errorf("%w: consumer ID: %w", ErrInvalidConfig, err)
	}
	if connection == nil || store == nil || decode == nil ||
		config.Concurrency < 1 || config.Concurrency > 128 ||
		config.Timeout <= 0 || config.MaxAttempts <= 0 || config.BaseRetry <= 0 ||
		config.MaxRetry < config.BaseRetry || config.AckWait < 100*time.Millisecond || !config.WireMode.valid() {
		return nil, fmt.Errorf("%w: durable consumer", ErrInvalidConfig)
	}
	if !store.SupportsAttempts() {
		return nil, fmt.Errorf("%w: durable consumer: %w", ErrInvalidConfig, inbox.ErrAttemptTrackingUnsupported)
	}
	maxAttempts := uint64(config.MaxAttempts)
	subject, err := Subject(config.Namespace, descriptor)
	if err != nil {
		return nil, err
	}
	if config.DLQSubject == "" {
		config.DLQSubject = config.Namespace + ".dlq"
	}
	if err := validateSubjectToken(config.DLQSubject, true); err != nil {
		return nil, fmt.Errorf("%w: DLQ subject: %w", ErrInvalidConfig, err)
	}
	if config.DLQSubject == subject {
		return nil, fmt.Errorf("%w: DLQ subject matches consumer subject", ErrInvalidConfig)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: create JetStream context: %w", err)
	}
	return &Consumer{
		connection: connection, js: js, store: store, config: config,
		maxAttempts: maxAttempts, descriptor: descriptor, subject: subject, decode: decode, clock: time.Now,
		done: make(chan struct{}),
	}, nil
}

func applyConsumerDefaults(config *HandlerConfig) {
	if config.Concurrency == 0 {
		config.Concurrency = 1
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
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
	if config.AckWait == 0 {
		config.AckWait = max(2*config.Timeout, 30*time.Second)
	}
	if config.Replicas == 0 {
		config.Replicas = 1
	}
}

// Run ensures safe consumer topology and processes messages until drain/cancel.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.state == consumerRunning {
		c.mu.Unlock()
		return messenger.ErrRuntimeRunning
	}
	if c.state == consumerDraining {
		if c.runStarted {
			c.mu.Unlock()
			return messenger.ErrRuntimeRunning
		}
		c.state = consumerClosed
		c.mu.Unlock()
		c.closeDone()
		return nil
	}
	if c.state == consumerClosed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}
	c.runStarted = true
	c.state = consumerRunning
	c.mu.Unlock()
	if err := c.ensureDLQStream(ctx); err != nil {
		c.markClosed()
		return err
	}
	consumer, err := c.ensureConsumer(ctx)
	if err != nil {
		c.markClosed()
		return err
	}
	iterator, err := consumer.Messages(
		jetstream.PullMaxMessages(c.config.Concurrency),
		jetstream.PullHeartbeat(min(5*time.Second, max(500*time.Millisecond, c.config.AckWait/3))),
	)
	if err != nil {
		c.markClosed()
		return fmt.Errorf("messenger/nats: start message iterator: %w", err)
	}
	c.mu.Lock()
	c.iterator = iterator
	draining := c.state == consumerDraining
	c.mu.Unlock()
	if draining {
		iterator.Stop()
		c.markClosed()
		return nil
	}

	jobs := make(chan jetstream.Msg, c.config.Concurrency)
	var workers sync.WaitGroup
	for range c.config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for message := range jobs {
				c.processMessage(ctx, message)
			}
		}()
	}
	var runErr error
pullLoop:
	for {
		message, err := iterator.Next(jetstream.NextContext(ctx))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
				break
			}
			if recoverablePullError(err) {
				continue
			}
			runErr = fmt.Errorf("messenger/nats: consume %s/%s: %w",
				c.config.Stream, c.config.ConsumerID, err)
			break
		}
		select {
		case jobs <- message:
		case <-ctx.Done():
			break pullLoop
		}
	}
	iterator.Stop()
	close(jobs)
	workers.Wait()
	c.markClosed()
	return runErr
}

func recoverablePullError(err error) bool {
	return errors.Is(err, jetstream.ErrNoHeartbeat)
}

// Readiness verifies the connection and exact durable consumer contract.
func (c *Consumer) Readiness(ctx context.Context) error {
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	if state != consumerRunning {
		return messenger.ErrRuntimeNotRunning
	}
	if !c.connection.IsConnected() {
		return errors.New("messenger/nats: connection is not connected")
	}
	if err := c.ensureDLQStream(ctx); err != nil {
		return err
	}
	consumer, err := c.js.Consumer(ctx, c.config.Stream, c.config.ConsumerID)
	if err != nil {
		return fmt.Errorf("messenger/nats: consumer readiness: %w", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("messenger/nats: consumer info: %w", err)
	}
	action, reason := compareConsumer(info.Config, c.consumerSpec())
	if action != ChangeNoop {
		return fmt.Errorf("%w: %s", ErrTopologyDrift, reason)
	}
	return nil
}

// BeginDrain stops new pulls. Already assigned messages finish when their
// contexts remain live; uncompleted messages are left unacknowledged.
func (c *Consumer) BeginDrain() {
	c.mu.Lock()
	iterator := c.beginDrainLocked()
	c.mu.Unlock()
	if iterator != nil {
		iterator.Stop()
	}
}

// Shutdown drains and waits for Run to return within ctx.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if !c.runStarted {
		if c.beforeShutdownTransition != nil {
			c.beforeShutdownTransition()
		}
		c.state = consumerClosed
		c.mu.Unlock()
		c.closeDone()
		return nil
	}
	iterator := c.beginDrainLocked()
	c.mu.Unlock()
	if iterator != nil {
		iterator.Stop()
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) beginDrainLocked() jetstream.MessagesContext {
	if c.state == consumerNew || c.state == consumerRunning {
		c.state = consumerDraining
	}
	return c.iterator
}

func (c *Consumer) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	spec := c.consumerSpec()
	consumer, err := c.js.Consumer(ctx, spec.Stream, spec.Name)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		created, createErr := c.js.CreateConsumer(ctx, spec.Stream, consumerConfig(spec))
		if createErr != nil {
			return nil, fmt.Errorf("messenger/nats: create consumer %s/%s: %w", spec.Stream, spec.Name, createErr)
		}
		return created, nil
	}
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: get consumer %s/%s: %w", spec.Stream, spec.Name, err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: inspect consumer %s/%s: %w", spec.Stream, spec.Name, err)
	}
	action, reason := compareConsumer(info.Config, spec)
	if action == ChangeConflict {
		return nil, fmt.Errorf("%w: consumer %s/%s: %s", ErrTopologyDrift, spec.Stream, spec.Name, reason)
	}
	if action == ChangeUpdate {
		updated, updateErr := c.js.UpdateConsumer(ctx, spec.Stream, mergeConsumerConfig(info.Config, spec))
		if updateErr != nil {
			return nil, fmt.Errorf("messenger/nats: update consumer %s/%s: %w", spec.Stream, spec.Name, updateErr)
		}
		return updated, nil
	}
	return consumer, nil
}

func (c *Consumer) ensureDLQStream(ctx context.Context) error {
	if maxPayload := c.connection.MaxPayload(); maxPayload < int64(DefaultMaxDLQMessageBytes) {
		return fmt.Errorf(
			"%w: NATS max payload is %d, need at least %d for DLQ messages",
			ErrTopologyDrift, maxPayload, DefaultMaxDLQMessageBytes,
		)
	}
	streamName, err := c.js.StreamNameBySubject(ctx, c.config.DLQSubject)
	if err != nil {
		return fmt.Errorf("%w: resolve DLQ subject %q: %w", ErrTopologyDrift, c.config.DLQSubject, err)
	}
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("%w: get DLQ stream %s: %w", ErrTopologyDrift, streamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("%w: inspect DLQ stream %s: %w", ErrTopologyDrift, streamName, err)
	}
	if maxMessageSize := info.Config.MaxMsgSize; maxMessageSize != -1 &&
		maxMessageSize < DefaultMaxDLQMessageBytes {
		return fmt.Errorf(
			"%w: DLQ stream %s max message size is %d, need -1 or at least %d",
			ErrTopologyDrift, streamName, maxMessageSize, DefaultMaxDLQMessageBytes,
		)
	}
	return nil
}

func (c *Consumer) consumerSpec() ConsumerSpec {
	return ConsumerSpec{
		Stream: c.config.Stream, Name: c.config.ConsumerID, Description: c.config.Description,
		FilterSubject: c.subject, AckWait: c.config.AckWait, MaxDeliver: -1,
		MaxAckPending: c.config.Concurrency, Replicas: c.config.Replicas, MemoryStorage: c.config.MemoryStorage,
	}
}

func (c *Consumer) processMessage(runContext context.Context, message jetstream.Msg) {
	metadata, err := message.Metadata()
	if err != nil {
		logInfrastructure(runContext, c.config.Logger, messenger.LogError, "read JetStream message metadata",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
		return
	}
	stopHeartbeat := c.startHeartbeat(runContext, message)
	defer stopHeartbeat()
	decoded, err := c.decode(message.Data(), message.Headers(), metadata.Timestamp)
	if err != nil {
		c.deadLetterAndAcknowledge(runContext, message, decoded, metadata.NumDelivered, "decode", err)
		return
	}
	key := inbox.Key{
		ConsumerID:        c.config.ConsumerID,
		Source:            decoded.metadata.Source,
		MessageID:         decoded.metadata.ID,
		AttemptGeneration: replayAttemptGeneration(message.Headers(), c.config.ConsumerID),
	}
	now := c.clock().UTC()
	if !decoded.metadata.ExpiresAt.IsZero() && !decoded.metadata.ExpiresAt.After(now) {
		c.deadLetterAndAcknowledge(
			runContext, message, decoded, metadata.NumDelivered, "expired", ErrMessageExpired,
		)
		return
	}
	if !decoded.metadata.NotBefore.IsZero() && decoded.metadata.NotBefore.After(now) {
		delay := decoded.metadata.NotBefore.Sub(now)
		if err := message.NakWithDelay(delay); err != nil {
			logInfrastructure(runContext, c.config.Logger, messenger.LogWarn, "delay scheduled JetStream message",
				messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
				messenger.LogAttr{Key: logAttrMessageID, Value: decoded.metadata.ID.String()},
				messenger.LogAttr{Key: "retry_delay", Value: delay},
				messenger.LogAttr{Key: logAttrError, Value: err},
			)
		}
		return
	}
	deliveryContext := c.config.Propagator.Extract(runContext, decoded.metadata.Headers)
	deliveryContext = messenger.ContextWithMetadata(deliveryContext, decoded.metadata)
	processContext, cancel := context.WithTimeout(deliveryContext, c.config.Timeout)
	defer cancel()
	transactionContext, cancelTransaction := context.WithTimeout(
		deliveryContext, handlerTransactionTimeout(c.config.Timeout),
	)
	defer cancelTransaction()
	startedAt := c.clock().UTC()
	fingerprint := inbox.FingerprintEnvelope(decoded.canonical)
	result, processErr := c.store.ProcessAttempt(
		transactionContext,
		key,
		fingerprint,
		c.maxAttempts,
		func(transactionHandlerContext context.Context) error {
			handlerContext := processContext
			if tx, ok := inbox.SQLTxFromContext(transactionHandlerContext); ok {
				handlerContext = inbox.ContextWithSQLTx(processContext, tx)
			}
			return invokeMiddlewares(
				handlerContext,
				decoded.metadata,
				c.config.ConsumerID,
				decoded.handle,
				c.config.Middlewares,
			)
		},
	)
	attempt := result.Attempt
	if processErr == nil {
		ackContext, ackCancel := context.WithTimeout(context.WithoutCancel(runContext), brokerAckTimeout)
		ackErr := message.DoubleAck(ackContext)
		ackCancel()
		if ackErr != nil {
			logInfrastructure(runContext, c.config.Logger, messenger.LogWarn, "acknowledge JetStream message",
				messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
				messenger.LogAttr{Key: logAttrMessageID, Value: decoded.metadata.ID.String()},
				messenger.LogAttr{Key: logAttrAttempt, Value: attempt},
				messenger.LogAttr{Key: logAttrError, Value: ackErr},
			)
		}
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, nil)
		return
	}
	if runContext.Err() != nil {
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return
	}
	exhausted := c.attemptsExhausted(attempt)
	identityConflict := errors.Is(processErr, inbox.ErrFingerprintConflict)
	if identityConflict || messenger.IsPermanent(processErr) || exhausted {
		failureKind := "permanent"
		if identityConflict {
			failureKind = "identity_conflict"
		} else if exhausted && !messenger.IsPermanent(processErr) {
			failureKind = "attempts_exhausted"
		}
		recordAttempt := terminalRecordAttempt(attempt, metadata.NumDelivered)
		acknowledged := c.deadLetterAndAcknowledge(
			deliveryContext, message, decoded, recordAttempt, failureKind, processErr,
		)
		if acknowledged && !identityConflict {
			c.forgetAttempt(deliveryContext, key, fingerprint, decoded.metadata.ID)
		}
		c.observeHandle(processContext, decoded, attempt, result, 0, startedAt, processErr)
		return
	}
	delay, ok := messenger.RetryDelay(processErr)
	if !ok {
		delay = c.retryDelay(attempt)
	}
	if err := message.NakWithDelay(delay); err != nil {
		logInfrastructure(runContext, c.config.Logger, messenger.LogWarn, "retry JetStream message",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrMessageID, Value: decoded.metadata.ID.String()},
			messenger.LogAttr{Key: logAttrAttempt, Value: attempt},
			messenger.LogAttr{Key: "retry_delay", Value: delay},
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
	}
	c.observeHandle(processContext, decoded, attempt, result, delay, startedAt, processErr)
}

func handlerTransactionTimeout(handlerTimeout time.Duration) time.Duration {
	const (
		finalizationTimeout = 5 * time.Second
		maxDuration         = time.Duration(1<<63 - 1)
	)
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
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
	}
}

func (c *Consumer) observeHandle(
	ctx context.Context,
	decoded decodedMessage,
	attempt uint64,
	result inbox.Result,
	retryDelay time.Duration,
	startedAt time.Time,
	err error,
) {
	if len(c.config.Observers) == 0 {
		return
	}
	notifyObservers(ctx, c.config, messenger.Observation{
		Operation:     messenger.OperationHandle,
		MessageID:     decoded.metadata.ID,
		Kind:          decoded.metadata.Kind,
		Name:          decoded.metadata.Name,
		SchemaVersion: decoded.metadata.SchemaVersion,
		HandlerID:     c.config.ConsumerID,
		ConsumerID:    c.config.ConsumerID,
		Attempt:       attempt,
		Duplicate:     result.Duplicate,
		RetryDelay:    retryDelay,
		StartedAt:     startedAt,
		Duration:      c.clock().UTC().Sub(startedAt),
		Err:           err,
	})
}

func (c *Consumer) startHeartbeat(ctx context.Context, message jetstream.Msg) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	interval := c.config.AckWait / 3
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := message.InProgress(); err != nil {
					logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "heartbeat JetStream message",
						messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
						messenger.LogAttr{Key: logAttrError, Value: err},
					)
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-finished
	}
}

func (c *Consumer) retryDelay(attempt uint64) time.Duration {
	delay := c.config.BaseRetry
	for current := uint64(1); current < attempt && delay < c.config.MaxRetry; current++ {
		if delay > c.config.MaxRetry/2 {
			delay = c.config.MaxRetry
			break
		}
		delay *= 2
	}
	if delay > c.config.MaxRetry {
		delay = c.config.MaxRetry
	}
	if delay <= 1 {
		return delay
	}
	n, err := crand.Int(crand.Reader, big.NewInt(int64(delay)+1))
	if err != nil {
		return delay
	}
	return time.Duration(n.Int64())
}

func (c *Consumer) attemptsExhausted(attempt uint64) bool {
	return attempt >= c.maxAttempts
}

func terminalRecordAttempt(handlerAttempt, deliveryCount uint64) uint64 {
	if handlerAttempt > 0 {
		return handlerAttempt
	}
	return max(uint64(1), deliveryCount)
}

const (
	// DLQSpecVersion is the schema version for dead-letter records.
	DLQSpecVersion = "1.0"
	// DefaultMaxDLQRecordBytes bounds encoded DLQ JSON accepted by tooling.
	DefaultMaxDLQRecordBytes = 3 * messenger.DefaultMaxEnvelopeBytes
	// DefaultMaxDLQMessageBytes includes room for the record's NATS headers.
	DefaultMaxDLQMessageBytes = DefaultMaxDLQRecordBytes + messenger.DefaultMaxHeaderBytes
)

// DLQRecord is the stable dead-letter record persisted before the original
// JetStream message receives a broker-confirmed acknowledgement.
type DLQRecord struct {
	SpecVersion     string              `json:"specVersion"`
	ConsumerID      string              `json:"consumerId"`
	Subject         string              `json:"subject"`
	Attempt         uint64              `json:"attempt"`
	FailureKind     string              `json:"failureKind"`
	Error           string              `json:"error"`
	FailedAt        time.Time           `json:"failedAt"`
	WireMode        WireMode            `json:"wireMode"`
	Envelope        json.RawMessage     `json:"envelope,omitempty"`
	OriginalHeaders map[string][]string `json:"originalHeaders,omitempty"`
	OriginalBase64  string              `json:"originalBase64"`
}

// DecodeDLQRecord strictly validates one DLQ record for inspection tooling.
func DecodeDLQRecord(data []byte) (DLQRecord, error) {
	if len(data) == 0 || len(data) > DefaultMaxDLQRecordBytes {
		return DLQRecord{}, fmt.Errorf("%w: invalid DLQ record size", messenger.ErrInvalidMessage)
	}
	var record DLQRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return DLQRecord{}, fmt.Errorf("%w: decode DLQ record: %w", messenger.ErrInvalidMessage, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return DLQRecord{}, fmt.Errorf("%w: trailing DLQ JSON value", messenger.ErrInvalidMessage)
	}
	var required struct {
		OriginalBase64 *string `json:"originalBase64"`
	}
	if err := json.Unmarshal(data, &required); err != nil || required.OriginalBase64 == nil {
		return DLQRecord{}, fmt.Errorf("%w: missing originalBase64", messenger.ErrInvalidMessage)
	}
	if err := validateDLQRecord(record); err != nil {
		return DLQRecord{}, err
	}
	if len(record.Envelope) > 0 {
		canonical, err := messenger.CanonicalizeEnvelope(record.Envelope)
		if err != nil {
			return DLQRecord{}, err
		}
		record.Envelope = canonical
	}
	clonedHeaders, err := copyReplayHeaders(record.OriginalHeaders)
	if err != nil {
		return DLQRecord{}, err
	}
	record.OriginalHeaders = clonedHeaders
	return record, nil
}

func (c *Consumer) deadLetterAndAcknowledge(
	ctx context.Context,
	message jetstream.Msg,
	decoded decodedMessage,
	attempt uint64,
	failureKind string,
	failure error,
) bool {
	headers, err := copyReplayHeaders(message.Headers())
	if err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogError, "capture DLQ message headers",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
		return false
	}
	failureText := "unspecified failure"
	if failure != nil {
		failureText = truncate(failure.Error(), 1024)
		if failureText == "" {
			failureText = "unspecified failure"
		}
	}
	record := DLQRecord{
		SpecVersion: "1.0", ConsumerID: c.config.ConsumerID, Subject: message.Subject(),
		Attempt: attempt, FailureKind: failureKind, Error: failureText,
		FailedAt: c.clock().UTC(), WireMode: c.config.WireMode,
		OriginalHeaders: headers,
		OriginalBase64:  base64.StdEncoding.EncodeToString(message.Data()),
	}
	if len(decoded.canonical) > 0 && json.Valid(decoded.canonical) {
		record.Envelope = append(json.RawMessage(nil), decoded.canonical...)
	}
	if err := validateDLQRecord(record); err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogError, "validate DLQ record",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
		return false
	}
	data, err := json.Marshal(record)
	if err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogError, "encode DLQ record",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: err},
		)
		return false
	}
	if len(data) > DefaultMaxDLQRecordBytes && len(record.Envelope) > 0 {
		record.Envelope = nil
		data, err = json.Marshal(record)
		if err != nil {
			logInfrastructure(ctx, c.config.Logger, messenger.LogError, "encode bounded DLQ record",
				messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
				messenger.LogAttr{Key: logAttrError, Value: err},
			)
			return false
		}
	}
	if len(data) > DefaultMaxDLQRecordBytes {
		sizeErr := fmt.Errorf(
			"%w: DLQ record size %d exceeds %d", messenger.ErrInvalidMessage, len(data), DefaultMaxDLQRecordBytes,
		)
		logInfrastructure(ctx, c.config.Logger, messenger.LogError, "validate encoded DLQ record",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: sizeErr},
		)
		return false
	}
	dedupID := dlqDedupID(
		c.config.ConsumerID, message.Subject(), c.config.WireMode, headers, message.Data(),
	)
	dlqMessage := &natsio.Msg{
		Subject: c.config.DLQSubject,
		Header:  natsio.Header{"Content-Type": []string{"application/vnd.gomessenger.dlq+json; version=1.0"}},
		Data:    data,
	}
	for handoffAttempt := uint64(1); ; handoffAttempt++ {
		ack, publishErr := c.js.PublishMsg(ctx, dlqMessage, jetstream.WithMsgID(dedupID))
		if publishErr == nil && ack != nil && ack.Stream != "" {
			break
		}
		if publishErr == nil {
			publishErr = errors.New("messenger/nats: DLQ broker returned an empty publish acknowledgement")
		}
		logInfrastructure(ctx, c.config.Logger, messenger.LogError, "publish DLQ record",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrMessageID, Value: decoded.metadata.ID.String()},
			messenger.LogAttr{Key: logAttrAttempt, Value: attempt},
			messenger.LogAttr{Key: "failure_kind", Value: failureKind},
			messenger.LogAttr{Key: logAttrError, Value: publishErr},
		)
		if !c.waitForHandoffRetry(ctx, handoffAttempt) {
			return false
		}
	}
	return c.acknowledgeDeadLetteredMessage(ctx, message, decoded.metadata.ID, attempt)
}

func (c *Consumer) acknowledgeDeadLetteredMessage(
	ctx context.Context,
	message jetstream.Msg,
	messageID messenger.MessageID,
	attempt uint64,
) bool {
	for handoffAttempt := uint64(1); ; handoffAttempt++ {
		ackContext, cancel := context.WithTimeout(ctx, brokerAckTimeout)
		ackErr := message.DoubleAck(ackContext)
		cancel()
		if ackErr == nil {
			return true
		}
		logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "confirm dead-lettered JetStream acknowledgement",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrMessageID, Value: messageID.String()},
			messenger.LogAttr{Key: logAttrAttempt, Value: attempt},
			messenger.LogAttr{Key: logAttrError, Value: ackErr},
		)
		if errors.Is(ackErr, jetstream.ErrMsgAlreadyAckd) {
			return false
		}
		if !c.waitForHandoffRetry(ctx, handoffAttempt) {
			return false
		}
	}
}

func (c *Consumer) waitForHandoffRetry(ctx context.Context, attempt uint64) bool {
	delay := max(10*time.Millisecond, c.retryDelay(attempt))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func dlqDedupID(
	consumerID string,
	subject string,
	mode WireMode,
	headers map[string][]string,
	data []byte,
) string {
	inputDigest := replayDigest(subject, mode, headers, data)
	hash := sha256.New()
	_, _ = hash.Write([]byte(consumerID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(inputDigest[:])
	return "gm-dlq-" + hex.EncodeToString(hash.Sum(nil))
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
			err = fmt.Errorf("messenger/nats: handler panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return handler(ctx, message)
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
