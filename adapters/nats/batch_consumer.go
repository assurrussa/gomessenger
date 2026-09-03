package nats

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const batchAckParallelism = 16

type batchConsumer struct {
	config messenger.BatchConfig
	invoke func(context.Context, []decodedMessage) (messenger.BatchResult, error)
}

type natsBatchBrokerMessage interface {
	AckSync(opts ...natsio.AckOpt) error
	NakWithDelay(delay time.Duration, opts ...natsio.AckOpt) error
	InProgress(opts ...natsio.AckOpt) error
}

type natsBatchDelivery struct {
	broker            *natsio.Msg
	brokerMsg         natsBatchBrokerMessage
	metadata          *natsio.MsgMetadata
	decoded           decodedMessage
	decodeErr         error
	canonicalBytes    int
	attemptGeneration string
}

func (d *natsBatchDelivery) brokerMessage() natsBatchBrokerMessage {
	if d.brokerMsg != nil {
		return d.brokerMsg
	}
	return d.broker
}

func (d *natsBatchDelivery) ackSync(opts ...natsio.AckOpt) error {
	if msg := d.brokerMessage(); msg != nil {
		return msg.AckSync(opts...)
	}
	return nil
}

func (d *natsBatchDelivery) nakWithDelay(delay time.Duration, opts ...natsio.AckOpt) error {
	if msg := d.brokerMessage(); msg != nil {
		return msg.NakWithDelay(delay, opts...)
	}
	return nil
}

func (d *natsBatchDelivery) toBatchItem(consumerID string) inbox.BatchItem {
	return inbox.BatchItem{
		Key: inbox.Key{
			ConsumerID:        consumerID,
			Source:            d.decoded.metadata.Source,
			MessageID:         d.decoded.metadata.ID,
			AttemptGeneration: d.attemptGeneration,
		},
		Fingerprint: inbox.FingerprintEnvelope(d.decoded.canonical),
	}
}

type natsBatch struct {
	deliveries []*natsBatchDelivery
	bytes      int
	startedAt  time.Time
	heartbeat  *natsBatchHeartbeat
}

type natsBatchFinalOutcome struct {
	item        inbox.BatchItemOutcome
	failureKind string
	err         error
}

// NewBatchCommandConsumer constructs a native-envelope command consumer that
// invokes handler once per real batch.
func NewBatchCommandConsumer[T any](
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.Command[T],
	handler messenger.BatchHandler[T],
	config HandlerConfig,
	batchConfig messenger.BatchConfig,
) (*Consumer, error) {
	if config.WireMode == "" {
		config.WireMode = WireNative
	}
	if config.WireMode != WireNative {
		return nil, fmt.Errorf("%w: commands require native wire mode", ErrInvalidConfig)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: nil batch command handler", ErrInvalidConfig)
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
		return decodedMessage{metadata: message.Metadata, canonical: canonical, value: message}, nil
	}
	return newNATSBatchConsumer(connection, store, descriptor.Info(), handler, config, batchConfig, decode)
}

// NewBatchEventConsumer constructs a native or CloudEvents event consumer that
// invokes handler once per real batch.
func NewBatchEventConsumer[T any](
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.Event[T],
	handler messenger.BatchHandler[T],
	config HandlerConfig,
	batchConfig messenger.BatchConfig,
) (*Consumer, error) {
	if config.WireMode == "" {
		config.WireMode = WireNative
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: nil batch event handler", ErrInvalidConfig)
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
			return decodedMessage{metadata: message.Metadata, canonical: canonical, value: message}, nil
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
		return decodedMessage{metadata: message.Metadata, canonical: canonical, value: message}, nil
	}
	return newNATSBatchConsumer(connection, store, descriptor.Info(), handler, config, batchConfig, decode)
}

func newNATSBatchConsumer[T any](
	connection *natsio.Conn,
	store *inbox.Store,
	descriptor messenger.DescriptorInfo,
	handler messenger.BatchHandler[T],
	config HandlerConfig,
	batchConfig messenger.BatchConfig,
	decode decoder,
) (*Consumer, error) {
	if len(config.Middlewares) != 0 {
		return nil, fmt.Errorf("%w: batch consumers reject single-message middleware", ErrInvalidConfig)
	}
	consumer, err := newConsumer(connection, store, descriptor, config, decode)
	if err != nil {
		return nil, err
	}
	if !store.SupportsBatchAttempts() {
		return nil, fmt.Errorf("%w: batch consumer: %w", ErrInvalidConfig,
			inbox.ErrBatchAttemptTrackingUnsupported)
	}
	normalized, err := batchConfig.Normalize(consumer.config.Concurrency)
	if err != nil {
		return nil, fmt.Errorf("%w: batch config: %w", ErrInvalidConfig, err)
	}
	consumer.batch = &batchConsumer{config: normalized}
	consumer.batch.invoke = func(ctx context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		messages := make([]messenger.Message[T], len(decoded))
		for index := range decoded {
			message, ok := decoded[index].value.(messenger.Message[T])
			if !ok {
				return messenger.BatchResult{}, fmt.Errorf("%w: decoded batch message type", messenger.ErrInvalidBatchResult)
			}
			messages[index] = message
		}
		return batchruntime.Invoke(ctx, messages, consumer.config.ConsumerID, handler,
			normalized.Middlewares, consumer.config.PanicReporter)
	}
	return consumer, nil
}

func (c *Consumer) runBatch(runContext context.Context) error {
	defer c.markClosed()
	if err := c.ensureDLQStream(runContext); err != nil {
		return err
	}
	if _, err := c.ensureConsumer(runContext); err != nil {
		return err
	}
	legacy, err := c.connection.JetStream()
	if err != nil {
		return fmt.Errorf("messenger/nats: create batch pull context: %w", err)
	}
	subscription, err := legacy.PullSubscribe(c.subject, "",
		natsio.Bind(c.config.Stream, c.config.ConsumerID), natsio.ManualAck())
	if err != nil {
		return fmt.Errorf("messenger/nats: bind batch pull consumer: %w", err)
	}
	defer func() { _ = subscription.Unsubscribe() }()

	admissionCtx, cancelAdmission := context.WithCancel(runContext)
	c.mu.Lock()
	c.admissionCancel = cancelAdmission
	draining := c.state == consumerDraining
	c.mu.Unlock()
	if draining {
		cancelAdmission()
	}
	defer cancelAdmission()

	fatal := make(chan error, 1)
	var workers sync.WaitGroup
	started := make(chan struct{}, c.config.Concurrency)
	for range c.config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c.runNATSBatchWorker(runContext, admissionCtx, subscription, func() {
				started <- struct{}{}
			}, fatal)
		}()
	}
	for range c.config.Concurrency {
		select {
		case <-started:
		case runErr := <-fatal:
			cancelAdmission()
			c.mu.Lock()
			forceCancel := c.forceCancel
			c.mu.Unlock()
			if forceCancel != nil {
				forceCancel()
			}
			workers.Wait()
			return runErr
		case <-runContext.Done():
			cancelAdmission()
			workers.Wait()
			return nil
		}
	}
	if !c.markPullLoopReady(runContext) {
		cancelAdmission()
		workers.Wait()
		return nil
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case runErr := <-fatal:
		cancelAdmission()
		c.mu.Lock()
		forceCancel := c.forceCancel
		c.mu.Unlock()
		if forceCancel != nil {
			forceCancel()
		}
		<-done
		return runErr
	case <-runContext.Done():
		cancelAdmission()
		<-done
		return nil
	case <-done:
		return nil
	}
}

func (c *Consumer) runNATSBatchWorker(
	runContext context.Context,
	admissionCtx context.Context,
	subscription *natsio.Subscription,
	ready func(),
	fatal chan<- error,
) {
	var topLevelStreak uint64
	var readyOnce sync.Once
	for admissionCtx.Err() == nil {
		readyOnce.Do(ready)
		batch, err := c.collectNATSBatch(runContext, admissionCtx, subscription)
		if err != nil {
			if normalNATSBatchBoundary(admissionCtx, err) {
				continue
			}
			select {
			case fatal <- err:
			default:
			}
			return
		}
		if batch == nil {
			continue
		}
		if err := c.processNATSBatch(runContext, batch, &topLevelStreak); err != nil {
			select {
			case fatal <- err:
			default:
			}
			return
		}
	}
}

func (c *Consumer) collectNATSBatch(
	runContext context.Context,
	admissionCtx context.Context,
	subscription *natsio.Subscription,
) (*natsBatch, error) {
	firstResult, err := subscription.FetchBatch(1, natsio.Context(admissionCtx))
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: fetch first batch delivery: %w", err)
	}
	var first *natsio.Msg
	for message := range firstResult.Messages() {
		first = message
		break
	}
	if firstErr := firstResult.Error(); firstErr != nil && first == nil {
		return nil, firstErr
	}
	if first == nil {
		return nil, nil //nolint:nilnil // Empty pulls are a normal admission boundary.
	}
	batch := &natsBatch{startedAt: c.clock().UTC(), heartbeat: newNATSBatchHeartbeat(runContext, c)}
	firstDelivery := c.decodeNATSBatchDelivery(first)
	batch.deliveries = append(batch.deliveries, firstDelivery)
	batch.bytes = firstDelivery.canonicalBytes
	batch.heartbeat.Add(firstDelivery.brokerMessage())
	if firstDelivery.decodeErr != nil || !natsBatchEligible(firstDelivery.decoded.metadata, c.clock().UTC()) ||
		c.batch.config.MaxMessages == 1 || batch.bytes >= c.batch.config.MaxBytes {
		return batch, nil
	}

	remainingMessages := c.batch.config.MaxMessages - 1
	remainingBytes := c.batch.config.MaxBytes - batch.bytes
	fillCtx, cancelFill := context.WithTimeout(admissionCtx, c.batch.config.MaxWait)
	defer cancelFill()
	result, err := subscription.FetchBatch(remainingMessages,
		natsio.Context(fillCtx), natsio.PullMaxBytes(remainingBytes))
	if err != nil {
		if normalNATSBatchBoundary(fillCtx, err) {
			return batch, nil
		}
		batch.heartbeat.Stop()
		return nil, fmt.Errorf("messenger/nats: fill batch: %w", err)
	}
	for message := range result.Messages() {
		delivery := c.decodeNATSBatchDelivery(message)
		if hasNATSBatchConflictingGeneration(batch.deliveries, delivery) {
			_ = message.NakWithDelay(c.batch.config.MaxWait)
			continue
		}
		if delivery.canonicalBytes > c.batch.config.MaxBytes-batch.bytes {
			_ = message.NakWithDelay(c.batch.config.MaxWait)
			continue
		}
		batch.deliveries = append(batch.deliveries, delivery)
		batch.bytes += delivery.canonicalBytes
		batch.heartbeat.Add(delivery.brokerMessage())
	}
	if fetchErr := result.Error(); fetchErr != nil && !normalNATSBatchBoundary(fillCtx, fetchErr) {
		batch.heartbeat.Stop()
		return nil, fmt.Errorf("messenger/nats: fill batch deliveries: %w", fetchErr)
	}
	return batch, nil
}

func hasNATSBatchConflictingGeneration(deliveries []*natsBatchDelivery, candidate *natsBatchDelivery) bool {
	if candidate == nil || candidate.decoded.metadata.ID.IsZero() {
		return false
	}
	candidateID := candidate.decoded.metadata.ID
	candidateSource := candidate.decoded.metadata.Source
	candidateGen := candidate.attemptGeneration
	for _, prev := range deliveries {
		if prev != nil && prev.decoded.metadata.ID == candidateID && prev.decoded.metadata.Source == candidateSource {
			if prev.attemptGeneration != candidateGen {
				return true
			}
		}
	}
	return false
}

func normalNATSBatchBoundary(ctx context.Context, err error) bool {
	return err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, natsio.ErrTimeout) ||
		errors.Is(err, jetstream.ErrBatchCompleted) ||
		// Legacy Subscription.FetchBatch converts the server's 409 status into
		// an unwrapped formatted error, so errors.Is cannot recognize it.
		strings.EqualFold(err.Error(), jetstream.ErrBatchCompleted.Error())
}

func (c *Consumer) decodeNATSBatchDelivery(message *natsio.Msg) *natsBatchDelivery {
	delivery := &natsBatchDelivery{
		broker:            message,
		brokerMsg:         message,
		canonicalBytes:    len(message.Data),
		attemptGeneration: replayAttemptGeneration(message.Header, c.config.ConsumerID),
	}
	metadata, err := message.Metadata()
	if err != nil {
		delivery.decodeErr = fmt.Errorf("read JetStream metadata: %w", err)
		return delivery
	}
	delivery.metadata = metadata
	delivery.decoded, delivery.decodeErr = c.decode(message.Data, message.Header, metadata.Timestamp)
	if delivery.decodeErr == nil {
		delivery.canonicalBytes = len(delivery.decoded.canonical)
	}
	return delivery
}

func natsBatchEligible(metadata messenger.Metadata, now time.Time) bool {
	return (metadata.ExpiresAt.IsZero() || metadata.ExpiresAt.After(now)) &&
		(metadata.NotBefore.IsZero() || !metadata.NotBefore.After(now))
}

//nolint:gocognit,gocyclo // Prefilter, atomic outcome processing, and broker handoff are intentionally explicit.
func (c *Consumer) processNATSBatch(
	runContext context.Context,
	batch *natsBatch,
	topLevelStreak *uint64,
) error {
	startedAt := c.clock().UTC()
	var handlerStarted time.Time
	var handlerDuration time.Duration
	var handlerMessages int
	var observationErr error
	var batchDuration time.Duration
	defer batch.heartbeat.Stop()
	outcomes := make([]natsBatchFinalOutcome, len(batch.deliveries))

	type pendingNATSItemObservation struct {
		delivery  *natsBatchDelivery
		outcome   natsBatchFinalOutcome
		startedAt time.Time
		duration  time.Duration
	}
	type pendingNATSBoundaryObservation struct {
		operation messenger.Operation
		messageID messenger.MessageID
		startedAt time.Time
		duration  time.Duration
		err       error
	}

	var obsMu sync.Mutex
	var pendingItemObs []pendingNATSItemObservation
	var pendingBoundaryObs []pendingNATSBoundaryObservation

	queueItemObservation := func(
		delivery *natsBatchDelivery,
		outcome natsBatchFinalOutcome,
		itemStarted time.Time,
		itemDuration time.Duration,
	) {
		if len(c.config.Observers) == 0 || delivery.decoded.metadata.ID.IsZero() {
			return
		}
		obsMu.Lock()
		pendingItemObs = append(pendingItemObs, pendingNATSItemObservation{
			delivery:  delivery,
			outcome:   outcome,
			startedAt: itemStarted,
			duration:  itemDuration,
		})
		obsMu.Unlock()
	}

	queueBoundaryObservation := func(
		operation messenger.Operation,
		messageID messenger.MessageID,
		startedAt time.Time,
		duration time.Duration,
		err error,
	) {
		if len(c.config.Observers) == 0 {
			return
		}
		obsMu.Lock()
		pendingBoundaryObs = append(pendingBoundaryObs, pendingNATSBoundaryObservation{
			operation: operation,
			messageID: messageID,
			startedAt: startedAt,
			duration:  duration,
			err:       err,
		})
		obsMu.Unlock()
	}

	defer func() {
		if batchDuration == 0 {
			batchDuration = c.clock().UTC().Sub(startedAt)
		}
		for _, obs := range pendingItemObs {
			c.observeNATSBatchItem(runContext, obs.delivery, obs.outcome, obs.startedAt, obs.duration)
		}
		for _, obs := range pendingBoundaryObs {
			c.observeBoundaryWithDuration(runContext, obs.operation, obs.messageID, obs.startedAt, obs.duration, obs.err)
		}
		c.observeNATSBatch(runContext, batch, outcomes, handlerMessages, handlerDuration,
			startedAt, batchDuration, observationErr)
	}()
	valid := make([]inbox.BatchItem, 0, len(batch.deliveries))
	validIndexes := make([]int, 0, len(batch.deliveries))
	byItem := make(map[inbox.BatchItem]decodedMessage, len(batch.deliveries))
	now := c.clock().UTC()
	for index, delivery := range batch.deliveries {
		if delivery.decodeErr != nil {
			outcomes[index] = natsBatchFinalOutcome{
				item: inbox.BatchItemOutcome{
					Outcome: inbox.BatchDLQ,
					Attempt: natsBatchBrokerAttempt(delivery),
				},
				failureKind: "decode", err: delivery.decodeErr,
			}
			continue
		}
		key := inbox.Key{
			ConsumerID: c.config.ConsumerID, Source: delivery.decoded.metadata.Source,
			MessageID:         delivery.decoded.metadata.ID,
			AttemptGeneration: delivery.attemptGeneration,
		}
		item := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope(delivery.decoded.canonical)}
		if !delivery.decoded.metadata.ExpiresAt.IsZero() && !delivery.decoded.metadata.ExpiresAt.After(now) {
			outcomes[index] = natsBatchFinalOutcome{item: inbox.BatchItemOutcome{
				Key: key, Fingerprint: item.Fingerprint, Outcome: inbox.BatchDLQ,
				Attempt: natsBatchBrokerAttempt(delivery),
			}, failureKind: "expired", err: ErrMessageExpired}
			continue
		}
		if !delivery.decoded.metadata.NotBefore.IsZero() && delivery.decoded.metadata.NotBefore.After(now) {
			outcomes[index] = natsBatchFinalOutcome{item: inbox.BatchItemOutcome{
				Key: key, Fingerprint: item.Fingerprint, Outcome: inbox.BatchDefer,
				Delay: delivery.decoded.metadata.NotBefore.Sub(now),
			}, err: ErrMessageNotReady}
			continue
		}
		valid = append(valid, item)
		validIndexes = append(validIndexes, index)
		if _, exists := byItem[item]; !exists {
			byItem[item] = delivery.decoded
		}
	}

	invoked := make(map[inbox.BatchItem]struct{}, len(valid))
	//nolint:nestif // The branch keeps the Inbox rollback boundary explicit.
	if len(valid) != 0 {
		transactionCtx, cancelTransaction := context.WithTimeout(runContext,
			handlerTransactionTimeout(c.config.Timeout, c.config.FinalizationTimeout))
		report, handlerErr := c.store.ProcessBatchAttempt(transactionCtx, valid, c.maxAttempts, func(
			transactionHandlerCtx context.Context,
			active []inbox.BatchItem,
		) (messenger.BatchResult, error) {
			handlerMessages = len(active)
			handlerCtx, cancelHandler := context.WithTimeout(transactionHandlerCtx, c.config.Timeout)
			defer cancelHandler()
			decoded, err := natsBatchDecodedMessages(active, byItem)
			if err != nil {
				return messenger.BatchResult{}, err
			}
			for _, item := range active {
				invoked[item] = struct{}{}
			}
			handlerStarted = c.clock().UTC()
			result, err := c.batch.invoke(handlerCtx, decoded)
			handlerDuration = c.clock().UTC().Sub(handlerStarted)
			return result, err
		})
		cancelTransaction()
		if handlerErr != nil {
			observationErr = handlerErr
			if batchruntime.IsFailClosed(handlerErr) {
				for _, delivery := range batch.deliveries {
					item := delivery.toBatchItem(c.config.ConsumerID)
					if _, ok := invoked[item]; ok {
						queueItemObservation(delivery, natsBatchFinalOutcome{
							item: inbox.BatchItemOutcome{
								Key:         item.Key,
								Fingerprint: item.Fingerprint,
								Outcome:     inbox.BatchRetry,
								Attempt:     max(uint64(1), natsBatchBrokerAttempt(delivery)),
							},
							err: handlerErr,
						}, handlerStarted, handlerDuration)
					}
				}
				return handlerErr
			}
			*topLevelStreak++
			delay, explicit := messenger.DeferDelay(handlerErr)
			isDefer := explicit
			if !explicit {
				delay, explicit = messenger.RetryDelay(handlerErr)
			}
			if !explicit {
				delay = c.retryDelay(*topLevelStreak)
			}
			outcomeKind := inbox.BatchRetry
			if isDefer {
				outcomeKind = inbox.BatchDefer
			}
			for validIndex, inputIndex := range validIndexes {
				outcomes[inputIndex] = natsBatchFinalOutcome{item: inbox.BatchItemOutcome{
					Outcome: outcomeKind, Delay: delay,
					Attempt: 0,
				}, err: handlerErr}
				outcomes[inputIndex].item.Key = valid[validIndex].Key
				outcomes[inputIndex].item.Fingerprint = valid[validIndex].Fingerprint
			}
		} else {
			*topLevelStreak = 0
			for reportIndex, inputIndex := range validIndexes {
				outcomes[inputIndex] = natsBatchFinalOutcome{
					item:        report.Items[reportIndex],
					failureKind: report.Items[reportIndex].FailureKind,
					err:         report.Items[reportIndex].Err,
				}
			}
		}
	}
	for index, outcome := range outcomes {
		delivery := batch.deliveries[index]
		item := delivery.toBatchItem(c.config.ConsumerID)
		if _, ok := invoked[item]; ok {
			queueItemObservation(delivery, outcome, handlerStarted, handlerDuration)
		} else if outcome.item.Duplicate {
			queueItemObservation(delivery, outcome, handlerStarted, 0)
		}
	}
	finRes := c.finalizeNATSBatch(runContext, batch, outcomes, queueBoundaryObservation)
	processingCompletedAt := c.clock().UTC()
	batchDuration = processingCompletedAt.Sub(startedAt)
	if finRes.observationErr != nil {
		if observationErr != nil {
			observationErr = errors.Join(observationErr, finRes.observationErr)
		} else {
			observationErr = finRes.observationErr
		}
	}
	if finRes.fatalErr != nil {
		return finRes.fatalErr
	}
	return nil
}

func natsBatchDecodedMessages(
	active []inbox.BatchItem,
	byItem map[inbox.BatchItem]decodedMessage,
) ([]decodedMessage, error) {
	decoded := make([]decodedMessage, len(active))
	for index, item := range active {
		value, exists := byItem[item]
		if !exists {
			return nil, fmt.Errorf("%w: active Inbox item is absent", messenger.ErrInvalidBatchResult)
		}
		decoded[index] = value
	}
	return decoded, nil
}

type natsBatchFinalizationResult struct {
	observationErr error
	fatalErr       error
}

type natsDLQHandoffResult struct {
	acknowledged bool
	stage        string
	err          error
}

type natsBoundaryQueueFunc func(
	operation messenger.Operation,
	messageID messenger.MessageID,
	startedAt time.Time,
	duration time.Duration,
	err error,
)

type natsNonDLQTask struct {
	index   int
	outcome natsBatchFinalOutcome
}

func (c *Consumer) finalizeNATSBatch(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
	queueBoundary ...natsBoundaryQueueFunc,
) natsBatchFinalizationResult {
	var queue natsBoundaryQueueFunc
	if len(queueBoundary) > 0 && queueBoundary[0] != nil {
		queue = queueBoundary[0]
	} else {
		queue = func(
			operation messenger.Operation,
			messageID messenger.MessageID,
			startedAt time.Time,
			duration time.Duration,
			err error,
		) {
			c.observeBoundaryWithDuration(ctx, operation, messageID, startedAt, duration, err)
		}
	}
	nonDLQObsErr, nonDLQFatalErr := c.finalizeNATSNonDLQDeliveries(ctx, batch, outcomes, queue)
	dlqObsErr, dlqFatalErr := c.finalizeNATSDLQDeliveries(ctx, batch, outcomes, queue)
	return natsBatchFinalizationResult{
		observationErr: errors.Join(nonDLQObsErr, dlqObsErr),
		fatalErr:       errors.Join(nonDLQFatalErr, dlqFatalErr),
	}
}

type natsNonDLQCoordinator struct {
	consumer *Consumer
	batch    *natsBatch
	tasksCh  <-chan natsNonDLQTask
	queue    natsBoundaryQueueFunc
	mu       sync.Mutex
	obsErr   error
	fatalErr error
}

func (c *natsNonDLQCoordinator) runWorker(ctx context.Context) {
	for task := range c.tasksCh {
		if ctx.Err() != nil {
			c.mu.Lock()
			if c.fatalErr == nil {
				c.obsErr = errors.Join(c.obsErr, ctx.Err())
				c.fatalErr = errors.Join(c.fatalErr, ctx.Err())
			}
			c.mu.Unlock()
			return
		}
		res := c.consumer.finalizeNATSBatchNonDLQ(ctx, c.batch.heartbeat, c.batch.deliveries[task.index], task.outcome, c.queue)
		if res.observationErr != nil || res.fatalErr != nil {
			c.mu.Lock()
			if res.observationErr != nil {
				c.obsErr = errors.Join(c.obsErr, res.observationErr)
			}
			if res.fatalErr != nil {
				c.fatalErr = errors.Join(c.fatalErr, res.fatalErr)
			}
			c.mu.Unlock()
		}
	}
}

func natsNonDLQTasks(outcomes []natsBatchFinalOutcome) []natsNonDLQTask {
	var tasks []natsNonDLQTask
	for index, outcome := range outcomes {
		if outcome.item.Outcome != inbox.BatchDLQ {
			tasks = append(tasks, natsNonDLQTask{index: index, outcome: outcome})
		}
	}
	return tasks
}

func (c *Consumer) finalizeNATSNonDLQDeliveries(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
	queue natsBoundaryQueueFunc,
) (obsErr, fatalErr error) {
	tasks := natsNonDLQTasks(outcomes)
	if len(tasks) == 0 {
		return nil, nil
	}

	tasksCh := make(chan natsNonDLQTask, len(tasks))
	for _, task := range tasks {
		tasksCh <- task
	}
	close(tasksCh)

	coordinator := &natsNonDLQCoordinator{
		consumer: c,
		batch:    batch,
		tasksCh:  tasksCh,
		queue:    queue,
	}

	workerCount := min(batchAckParallelism, len(tasks))
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			coordinator.runWorker(ctx)
		}()
	}
	workers.Wait()
	return coordinator.obsErr, coordinator.fatalErr
}

func (c *Consumer) finalizeNATSDLQDeliveries(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
	queue natsBoundaryQueueFunc,
) (obsErr, fatalErr error) {
	var dlqObsErr error
	var dlqFatalErr error
	var terminalCleanup []int
	for index, outcome := range outcomes {
		if outcome.item.Outcome != inbox.BatchDLQ {
			continue
		}
		delivery := batch.deliveries[index]
		handoffStarted := c.clock().UTC()
		handoffRes := c.deadLetterAndAcknowledgeNATSBatch(ctx, delivery, outcome)
		if !handoffRes.acknowledged {
			dlqErr := handoffRes.err
			if dlqErr == nil {
				dlqErr = errors.New("messenger/nats: batch DLQ handoff failed")
			}
			if handoffRes.stage != "" {
				dlqErr = fmt.Errorf("messenger/nats: batch DLQ [%s]: %w", handoffRes.stage, dlqErr)
			}
			dlqFatalErr = errors.Join(dlqFatalErr, dlqErr)
			queue(operationDLQHandoff, delivery.decoded.metadata.ID, handoffStarted, c.clock().UTC().Sub(handoffStarted), dlqErr)
			dlqObsErr = errors.Join(dlqObsErr, dlqErr)
			continue
		}
		queue(operationDLQHandoff, delivery.decoded.metadata.ID, handoffStarted, c.clock().UTC().Sub(handoffStarted), nil)
		batch.heartbeat.Remove(delivery.brokerMessage())
		if outcome.failureKind == inbox.FailurePermanent ||
			outcome.failureKind == inbox.FailureAttemptsExhausted {
			terminalCleanup = append(terminalCleanup, index)
		}
	}
	c.cleanupNATSTerminalAttempts(ctx, batch, outcomes, terminalCleanup)
	return dlqObsErr, dlqFatalErr
}

func (c *Consumer) cleanupNATSTerminalAttempts(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
	terminalCleanup []int,
) {
	if len(terminalCleanup) == 0 {
		return
	}
	type cleanupKey struct {
		key         inbox.Key
		fingerprint inbox.Fingerprint
	}
	seen := make(map[cleanupKey]messenger.MessageID, len(terminalCleanup))
	for _, index := range terminalCleanup {
		outcome := outcomes[index]
		ck := cleanupKey{key: outcome.item.Key, fingerprint: outcome.item.Fingerprint}
		if _, exists := seen[ck]; !exists {
			seen[ck] = batch.deliveries[index].decoded.metadata.ID
		}
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 5*time.Second)
	defer cancelCleanup()
	for ck, msgID := range seen {
		if cleanupCtx.Err() != nil {
			break
		}
		if err := c.store.ForgetAttempt(cleanupCtx, ck.key, ck.fingerprint); err != nil {
			logInfrastructure(cleanupCtx, c.config.Logger, messenger.LogWarn, "forget terminal handler attempt",
				messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
				messenger.LogAttr{Key: logAttrMessageID, Value: msgID.String()},
				messenger.LogAttr{Key: logAttrError, Value: err})
		}
	}
}

func (c *Consumer) finalizeNATSBatchNonDLQ(
	ctx context.Context,
	heartbeat *natsBatchHeartbeat,
	delivery *natsBatchDelivery,
	outcome natsBatchFinalOutcome,
	queueBoundary ...natsBoundaryQueueFunc,
) natsBatchFinalizationResult {
	var queue natsBoundaryQueueFunc
	if len(queueBoundary) > 0 && queueBoundary[0] != nil {
		queue = queueBoundary[0]
	} else {
		queue = func(
			operation messenger.Operation,
			messageID messenger.MessageID,
			startedAt time.Time,
			duration time.Duration,
			err error,
		) {
			c.observeBoundaryWithDuration(ctx, operation, messageID, startedAt, duration, err)
		}
	}
	var obsErr error
	var fatalErr error
	switch outcome.item.Outcome {
	case inbox.BatchACK:
		startedAt := c.clock().UTC()
		ackCtx, cancel := context.WithTimeout(ctx, brokerAckTimeout)
		err := delivery.ackSync(natsio.Context(ackCtx))
		cancel()
		if errors.Is(err, natsio.ErrMsgAlreadyAckd) {
			err = nil
		}
		queue(operationBrokerAck, delivery.decoded.metadata.ID, startedAt, c.clock().UTC().Sub(startedAt), err)
		if err == nil {
			heartbeat.Remove(delivery.brokerMessage())
		} else {
			if ctx.Err() != nil {
				obsErr = ctx.Err()
				fatalErr = ctx.Err()
			} else {
				obsErr = err
			}
			logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "acknowledge JetStream batch item",
				messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
				messenger.LogAttr{Key: logAttrMessageID, Value: delivery.decoded.metadata.ID.String()},
				messenger.LogAttr{Key: logAttrError, Value: sanitizeError(c.config.FailureSanitizer, err)})
		}
	case inbox.BatchRetry, inbox.BatchDefer:
		delay := outcome.item.Delay
		if delay <= 0 {
			delay = c.retryDelay(max(uint64(1), outcome.item.Attempt))
		}
		startedAt := c.clock().UTC()
		var nakAttempt uint64
		for {
			nakAttempt++
			err := delivery.nakWithDelay(delay)
			if err == nil {
				queue(operationRetryHandoff, delivery.decoded.metadata.ID, startedAt, c.clock().UTC().Sub(startedAt), nil)
				heartbeat.Remove(delivery.brokerMessage())
				break
			}
			if ctx.Err() != nil {
				obsErr = ctx.Err()
				fatalErr = ctx.Err()
				queue(operationRetryHandoff, delivery.decoded.metadata.ID, startedAt, c.clock().UTC().Sub(startedAt), obsErr)
				break
			}
			if !c.waitForHandoffRetry(ctx, nakAttempt) {
				obsErr = ctx.Err()
				fatalErr = ctx.Err()
				queue(operationRetryHandoff, delivery.decoded.metadata.ID, startedAt, c.clock().UTC().Sub(startedAt), obsErr)
				break
			}
		}
	case inbox.BatchDLQ:
		return natsBatchFinalizationResult{}
	}
	if obsErr != nil && outcome.item.Outcome != inbox.BatchACK {
		logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "finalize JetStream batch item",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: sanitizeError(c.config.FailureSanitizer, obsErr)})
	}
	return natsBatchFinalizationResult{observationErr: obsErr, fatalErr: fatalErr}
}

func (c *Consumer) deadLetterAndAcknowledgeNATSBatch(
	ctx context.Context,
	delivery *natsBatchDelivery,
	outcome natsBatchFinalOutcome,
) natsDLQHandoffResult {
	headers, err := copyReplayHeaders(delivery.broker.Header)
	if err != nil {
		return natsDLQHandoffResult{stage: "headers", err: err}
	}
	attempt := max(outcome.item.Attempt, natsBatchBrokerAttempt(delivery))
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: c.config.ConsumerID, Subject: delivery.broker.Subject,
		Attempt: max(uint64(1), attempt), FailureKind: outcome.failureKind,
		Error: boundedFailureText(defaultFailureSanitizer{}, outcome.err, 1024), FailedAt: c.clock().UTC(),
		WireMode: c.config.WireMode, OriginalHeaders: headers,
		OriginalBase64: base64.StdEncoding.EncodeToString(delivery.broker.Data),
	}
	if record.FailureKind == "" {
		record.FailureKind = "unknown"
	}
	if record.Error == "" {
		record.Error = "unspecified failure"
	}
	if len(delivery.decoded.canonical) != 0 && json.Valid(delivery.decoded.canonical) {
		record.Envelope = append(json.RawMessage(nil), delivery.decoded.canonical...)
	}
	if err := validateDLQRecord(record); err != nil {
		return natsDLQHandoffResult{stage: "validate", err: err}
	}
	data, err := json.Marshal(record)
	if err != nil || len(data) > DefaultMaxDLQRecordBytes {
		record.Envelope = nil
		data, err = json.Marshal(record)
	}
	if err != nil {
		return natsDLQHandoffResult{stage: "encode", err: err}
	}
	if len(data) > DefaultMaxDLQRecordBytes {
		return natsDLQHandoffResult{stage: "encode", err: fmt.Errorf(
			"%w: DLQ record size %d exceeds %d", messenger.ErrInvalidMessage, len(data), DefaultMaxDLQRecordBytes,
		)}
	}
	dedupID := dlqDedupID(c.config.ConsumerID, delivery.broker.Subject, c.config.WireMode,
		headers, delivery.broker.Data)
	for handoffAttempt := uint64(1); ; handoffAttempt++ {
		message := &natsio.Msg{
			Subject: c.config.DLQSubject,
			Header: natsio.Header{"Content-Type": []string{
				"application/vnd.gomessenger.dlq+json; version=1.0",
			}},
			Data: data,
		}
		ack, publishErr := c.js.PublishMsg(ctx, message, jetstream.WithMsgID(dedupID))
		if publishErr == nil && ack != nil && ack.Stream != "" {
			break
		}
		if !c.waitForHandoffRetry(ctx, handoffAttempt) {
			if ctx.Err() != nil {
				return natsDLQHandoffResult{stage: "publish", err: ctx.Err()}
			}
			return natsDLQHandoffResult{stage: "publish", err: publishErr}
		}
	}
	for handoffAttempt := uint64(1); ; handoffAttempt++ {
		ackCtx, cancel := context.WithTimeout(ctx, brokerAckTimeout)
		ackErr := delivery.ackSync(natsio.Context(ackCtx))
		cancel()
		if ackErr == nil || errors.Is(ackErr, natsio.ErrMsgAlreadyAckd) {
			return natsDLQHandoffResult{acknowledged: true}
		}
		if !c.waitForHandoffRetry(ctx, handoffAttempt) {
			if ctx.Err() != nil {
				return natsDLQHandoffResult{stage: "ack", err: ctx.Err()}
			}
			return natsDLQHandoffResult{stage: "ack", err: ackErr}
		}
	}
}

func natsBatchBrokerAttempt(delivery *natsBatchDelivery) uint64 {
	if delivery.metadata == nil {
		return 1
	}
	return max(uint64(1), delivery.metadata.NumDelivered)
}

func (c *Consumer) observeNATSBatch(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
	handlerMessages int,
	handlerDuration time.Duration,
	startedAt time.Time,
	batchDuration time.Duration,
	err error,
) {
	if len(c.config.Observers) == 0 {
		return
	}
	observation := messenger.Observation{
		Operation: messenger.OperationBatchHandle, Kind: c.descriptor.Kind,
		Name: c.descriptor.Name, SchemaVersion: c.descriptor.SchemaVersion,
		ConsumerID: c.config.ConsumerID, HandlerID: c.config.ConsumerID,
		BatchSize: len(batch.deliveries), BatchBytes: batch.bytes,
		BatchHandlerMessages: handlerMessages,
		BatchFillDuration:    startedAt.Sub(batch.startedAt), BatchHandlerDuration: handlerDuration,
		StartedAt: startedAt, Duration: batchDuration,
		Err: sanitizeError(c.config.FailureSanitizer, err),
	}
	for _, outcome := range outcomes {
		switch outcome.item.Outcome {
		case inbox.BatchACK:
			observation.BatchACKs++
		case inbox.BatchRetry:
			observation.BatchRetries++
			if outcome.item.Delay > 0 && observation.RetryDelay == 0 {
				observation.RetryDelay = outcome.item.Delay
			}
		case inbox.BatchDefer:
			observation.BatchDeferrals++
			if outcome.item.Delay > 0 && observation.RetryDelay == 0 {
				observation.RetryDelay = outcome.item.Delay
			}
		case inbox.BatchDLQ:
			observation.BatchDLQs++
		}
	}
	notifyObservers(ctx, c.config, observation)
}

func (c *Consumer) observeNATSBatchItem(
	ctx context.Context,
	delivery *natsBatchDelivery,
	outcome natsBatchFinalOutcome,
	startedAt time.Time,
	duration time.Duration,
) {
	if len(c.config.Observers) == 0 || delivery.decodeErr != nil {
		return
	}
	itemCtx := extractDeliveryContext(ctx, c.config, delivery.decoded.metadata.Headers)
	itemCtx = messenger.ContextWithMetadata(itemCtx, delivery.decoded.metadata)
	notifyObservers(itemCtx, c.config, messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: delivery.decoded.metadata.ID,
		Kind: delivery.decoded.metadata.Kind, Name: delivery.decoded.metadata.Name,
		SchemaVersion: delivery.decoded.metadata.SchemaVersion,
		ConsumerID:    c.config.ConsumerID, HandlerID: c.config.ConsumerID,
		Attempt: outcome.item.Attempt, Duplicate: outcome.item.Duplicate,
		RetryDelay: outcome.item.Delay,
		StartedAt:  startedAt,
		Duration:   duration,
		Err:        sanitizeError(c.config.FailureSanitizer, outcome.err),
	})
}

type natsBatchHeartbeat struct {
	runDone  <-chan struct{}
	consumer *Consumer
	mu       sync.Mutex
	messages map[natsBatchBrokerMessage]struct{}
	done     chan struct{}
	finished chan struct{}
	once     sync.Once
}

func newNATSBatchHeartbeat(ctx context.Context, consumer *Consumer) *natsBatchHeartbeat {
	heartbeat := &natsBatchHeartbeat{
		runDone: ctx.Done(), consumer: consumer, messages: make(map[natsBatchBrokerMessage]struct{}),
		done: make(chan struct{}), finished: make(chan struct{}),
	}
	go heartbeat.run(context.WithoutCancel(ctx))
	return heartbeat
}

func (h *natsBatchHeartbeat) Add(message natsBatchBrokerMessage) {
	if message == nil {
		return
	}
	h.mu.Lock()
	h.messages[message] = struct{}{}
	h.mu.Unlock()
}

func (h *natsBatchHeartbeat) Remove(message natsBatchBrokerMessage) {
	if message == nil {
		return
	}
	h.mu.Lock()
	delete(h.messages, message)
	h.mu.Unlock()
}

func (h *natsBatchHeartbeat) Stop() {
	h.once.Do(func() { close(h.done) })
	<-h.finished
}

func (h *natsBatchHeartbeat) run(logCtx context.Context) {
	defer close(h.finished)
	interval := h.consumer.config.AckWait / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()
			messages := make([]natsBatchBrokerMessage, 0, len(h.messages))
			for message := range h.messages {
				messages = append(messages, message)
			}
			h.mu.Unlock()
			for _, message := range messages {
				if err := message.InProgress(); err != nil {
					logInfrastructure(logCtx, h.consumer.config.Logger, messenger.LogWarn,
						"heartbeat JetStream batch",
						messenger.LogAttr{Key: logAttrConsumerID, Value: h.consumer.config.ConsumerID},
						messenger.LogAttr{Key: logAttrError, Value: err})
				}
			}
		case <-h.done:
			return
		case <-h.runDone:
			return
		}
	}
}
