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

type natsBatchDelivery struct {
	broker            *natsio.Msg
	metadata          *natsio.MsgMetadata
	decoded           decodedMessage
	decodeErr         error
	canonicalBytes    int
	attemptGeneration string
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
			workers.Wait()
			c.mu.Lock()
			forceCancel := c.forceCancel
			c.mu.Unlock()
			if forceCancel != nil {
				forceCancel()
			}
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
	batch.heartbeat.Add(first)
	if firstDelivery.decodeErr != nil || !natsBatchEligible(firstDelivery.decoded.metadata, c.clock().UTC()) ||
		c.batch.config.MaxMessages == 1 || batch.bytes > c.batch.config.MaxBytes {
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
		batch.heartbeat.Add(message)
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

//nolint:gocognit // Prefilter, atomic outcome processing, and broker handoff are intentionally explicit.
func (c *Consumer) processNATSBatch(
	runContext context.Context,
	batch *natsBatch,
	topLevelStreak *uint64,
) (processErr error) {
	startedAt := c.clock().UTC()
	var handlerDuration time.Duration
	var handlerMessages int
	defer batch.heartbeat.Stop()
	outcomes := make([]natsBatchFinalOutcome, len(batch.deliveries))
	defer func() {
		c.observeNATSBatch(runContext, batch, outcomes, handlerMessages, handlerDuration,
			startedAt, processErr)
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

	//nolint:nestif // The branch keeps the Inbox rollback boundary explicit.
	if len(valid) != 0 {
		transactionCtx, cancelTransaction := context.WithTimeout(runContext,
			handlerTransactionTimeout(c.config.Timeout, c.config.FinalizationTimeout))
		report, processErr := c.store.ProcessBatchAttempt(transactionCtx, valid, c.maxAttempts, func(
			transactionHandlerCtx context.Context,
			active []inbox.BatchItem,
		) (messenger.BatchResult, error) {
			handlerMessages = len(active)
			handlerCtx, cancelHandler := context.WithTimeout(runContext, c.config.Timeout)
			defer cancelHandler()
			if tx, ok := inbox.SQLTxFromContext(transactionHandlerCtx); ok {
				handlerCtx = inbox.ContextWithSQLTx(handlerCtx, tx)
			}
			decoded, err := natsBatchDecodedMessages(active, byItem)
			if err != nil {
				return messenger.BatchResult{}, err
			}
			handlerStarted := c.clock().UTC()
			result, err := c.batch.invoke(handlerCtx, decoded)
			handlerDuration = c.clock().UTC().Sub(handlerStarted)
			return result, err
		})
		cancelTransaction()
		if processErr != nil {
			if batchruntime.IsFailClosed(processErr) {
				return processErr
			}
			*topLevelStreak++
			delay, explicit := messenger.DeferDelay(processErr)
			if !explicit {
				delay, explicit = messenger.RetryDelay(processErr)
			}
			if !explicit {
				delay = c.retryDelay(*topLevelStreak)
			}
			for validIndex, inputIndex := range validIndexes {
				outcomes[inputIndex] = natsBatchFinalOutcome{item: inbox.BatchItemOutcome{
					Outcome: inbox.BatchRetry, Delay: delay,
					Attempt: 0,
				}, err: processErr}
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
	return c.finalizeNATSBatch(runContext, batch, outcomes)
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

func (c *Consumer) finalizeNATSBatch(
	ctx context.Context,
	batch *natsBatch,
	outcomes []natsBatchFinalOutcome,
) error {
	semaphore := make(chan struct{}, batchAckParallelism)
	var workers sync.WaitGroup
	for index, outcome := range outcomes {
		if outcome.item.Outcome == inbox.BatchDLQ {
			continue
		}
		workers.Add(1)
		go func(index int, outcome natsBatchFinalOutcome) {
			defer workers.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			c.finalizeNATSBatchNonDLQ(ctx, batch.deliveries[index], outcome)
		}(index, outcome)
	}
	workers.Wait()
	for index, outcome := range outcomes {
		if outcome.item.Outcome != inbox.BatchDLQ {
			continue
		}
		delivery := batch.deliveries[index]
		handoffStarted := c.clock().UTC()
		if !c.deadLetterAndAcknowledgeNATSBatch(ctx, delivery, outcome) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errors.New("messenger/nats: batch DLQ handoff failed")
		}
		c.observeBoundary(ctx, operationDLQHandoff, delivery.decoded.metadata.ID, handoffStarted, nil)
		c.observeNATSBatchItem(ctx, delivery, outcome)
		batch.heartbeat.Remove(delivery.broker)
		if outcome.failureKind == inbox.FailurePermanent ||
			outcome.failureKind == inbox.FailureAttemptsExhausted {
			c.forgetAttempt(ctx, outcome.item.Key, outcome.item.Fingerprint,
				delivery.decoded.metadata.ID)
		}
	}
	return nil
}

func (c *Consumer) finalizeNATSBatchNonDLQ(
	ctx context.Context,
	delivery *natsBatchDelivery,
	outcome natsBatchFinalOutcome,
) {
	var err error
	switch outcome.item.Outcome {
	case inbox.BatchACK:
		startedAt := c.clock().UTC()
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), brokerAckTimeout)
		err = delivery.broker.AckSync(natsio.Context(ackCtx))
		cancel()
		if errors.Is(err, natsio.ErrMsgAlreadyAckd) {
			err = nil
		}
		c.observeBoundary(ctx, operationBrokerAck, delivery.decoded.metadata.ID, startedAt, err)
	case inbox.BatchRetry, inbox.BatchDefer:
		delay := outcome.item.Delay
		if delay <= 0 {
			delay = c.retryDelay(max(uint64(1), outcome.item.Attempt))
		}
		startedAt := c.clock().UTC()
		err = delivery.broker.NakWithDelay(delay)
		c.observeBoundary(ctx, operationRetryHandoff, delivery.decoded.metadata.ID, startedAt, err)
	case inbox.BatchDLQ:
		return
	}
	if err != nil {
		logInfrastructure(ctx, c.config.Logger, messenger.LogWarn, "finalize JetStream batch item",
			messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
			messenger.LogAttr{Key: logAttrError, Value: sanitizeError(c.config.FailureSanitizer, err)})
	}
	c.observeNATSBatchItem(ctx, delivery, outcome)
}

func (c *Consumer) deadLetterAndAcknowledgeNATSBatch(
	ctx context.Context,
	delivery *natsBatchDelivery,
	outcome natsBatchFinalOutcome,
) bool {
	headers, err := copyReplayHeaders(delivery.broker.Header)
	if err != nil {
		return false
	}
	attempt := max(outcome.item.Attempt, natsBatchBrokerAttempt(delivery))
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: c.config.ConsumerID, Subject: delivery.broker.Subject,
		Attempt: max(uint64(1), attempt), FailureKind: outcome.failureKind,
		Error: boundedFailureText(c.config.FailureSanitizer, outcome.err, 1024), FailedAt: c.clock().UTC(),
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
		return false
	}
	data, err := json.Marshal(record)
	if err != nil || len(data) > DefaultMaxDLQRecordBytes {
		record.Envelope = nil
		data, err = json.Marshal(record)
	}
	if err != nil || len(data) > DefaultMaxDLQRecordBytes {
		return false
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
			return false
		}
	}
	for handoffAttempt := uint64(1); ; handoffAttempt++ {
		ackCtx, cancel := context.WithTimeout(ctx, brokerAckTimeout)
		ackErr := delivery.broker.AckSync(natsio.Context(ackCtx))
		cancel()
		if ackErr == nil || errors.Is(ackErr, natsio.ErrMsgAlreadyAckd) {
			return true
		}
		if !c.waitForHandoffRetry(ctx, handoffAttempt) {
			return false
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
		StartedAt: startedAt, Duration: c.clock().UTC().Sub(startedAt),
		Err: sanitizeError(c.config.FailureSanitizer, err),
	}
	for _, outcome := range outcomes {
		switch outcome.item.Outcome {
		case inbox.BatchACK:
			observation.BatchACKs++
		case inbox.BatchRetry:
			observation.BatchRetries++
		case inbox.BatchDefer:
			observation.BatchDeferrals++
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
		Err:        sanitizeError(c.config.FailureSanitizer, outcome.err),
	})
}

type natsBatchHeartbeat struct {
	runDone  <-chan struct{}
	consumer *Consumer
	mu       sync.Mutex
	messages map[*natsio.Msg]struct{}
	done     chan struct{}
	finished chan struct{}
	once     sync.Once
}

func newNATSBatchHeartbeat(ctx context.Context, consumer *Consumer) *natsBatchHeartbeat {
	heartbeat := &natsBatchHeartbeat{
		runDone: ctx.Done(), consumer: consumer, messages: make(map[*natsio.Msg]struct{}),
		done: make(chan struct{}), finished: make(chan struct{}),
	}
	go heartbeat.run(context.WithoutCancel(ctx))
	return heartbeat
}

func (h *natsBatchHeartbeat) Add(message *natsio.Msg) {
	h.mu.Lock()
	h.messages[message] = struct{}{}
	h.mu.Unlock()
}

func (h *natsBatchHeartbeat) Remove(message *natsio.Msg) {
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
	ticker := time.NewTicker(h.consumer.config.AckWait / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.mu.Lock()
			messages := make([]*natsio.Msg, 0, len(h.messages))
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
