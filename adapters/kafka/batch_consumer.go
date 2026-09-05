package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaBatchConsumer struct {
	config           messenger.BatchConfig
	rebalanceTimeout time.Duration
	invoke           func(context.Context, []decodedMessage) (messenger.BatchResult, error)
}

type kafkaBatchRecord struct {
	record   *kgo.Record
	prepared preparedRecord
	decoded  decodedMessage
	bytes    int
}

type kafkaPolledBatch struct {
	records          []kafkaBatchRecord
	earliestOffsets  map[string]map[int32]kgo.EpochOffset
	partition        topicPartition
	first            *kgo.Record
	firstDeferred    *kgo.Record
	deferUntil       time.Time
	firstUnprocessed *kgo.Record
	selectionStopped bool
	bytes            int
	fillStarted      time.Time
}

type kafkaBatchFailClosedError struct{ cause error }

func (e *kafkaBatchFailClosedError) Error() string { return e.cause.Error() }
func (e *kafkaBatchFailClosedError) Unwrap() error { return e.cause }

// NewBatchCommandConsumer constructs a native Kafka command consumer that
// invokes handler once per concrete topic-partition batch.
func NewBatchCommandConsumer[T any](
	transport *Transport,
	store *inbox.Store,
	descriptor messenger.Command[T],
	handler messenger.BatchHandler[T],
	config HandlerConfig,
	batchConfig messenger.BatchConfig,
) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil batch command handler", ErrInvalidConfig)
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
		return decodedMessage{metadata: message.Metadata, canonical: canonical, value: message}, nil
	}
	return newKafkaBatchConsumer(transport, store, descriptor.Info(), handler, config, batchConfig, decode)
}

// NewBatchEventConsumer constructs a native Kafka event consumer that invokes
// handler once per concrete topic-partition batch.
func NewBatchEventConsumer[T any](
	transport *Transport,
	store *inbox.Store,
	descriptor messenger.Event[T],
	handler messenger.BatchHandler[T],
	config HandlerConfig,
	batchConfig messenger.BatchConfig,
) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil batch event handler", ErrInvalidConfig)
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
		return decodedMessage{metadata: message.Metadata, canonical: canonical, value: message}, nil
	}
	return newKafkaBatchConsumer(transport, store, descriptor.Info(), handler, config, batchConfig, decode)
}

func newKafkaBatchConsumer[T any](
	transport *Transport,
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
	consumer, err := newConsumer(transport, store, descriptor, config, decode)
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
	rebalanceTimeout, err := batchConsumerRebalanceTimeout(
		normalized.MaxWait,
		consumer.config.Timeout,
		consumer.config.FinalizationTimeout,
		consumer.transport.config.OperationTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: batch rebalance timeout: %w", ErrInvalidConfig, err)
	}
	consumer.batch = &kafkaBatchConsumer{config: normalized, rebalanceTimeout: rebalanceTimeout}
	consumer.batch.invoke = func(ctx context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
		messages := make([]messenger.Message[T], len(decoded))
		for index := range decoded {
			message, ok := decoded[index].value.(messenger.Message[T])
			if !ok {
				return messenger.BatchResult{}, fmt.Errorf("%w: decoded batch message type",
					messenger.ErrInvalidBatchResult)
			}
			messages[index] = message
		}
		return batchruntime.Invoke(ctx, messages, consumer.config.ConsumerID, handler,
			normalized.Middlewares, consumer.config.PanicReporter)
	}
	return consumer, nil
}

func (c *Consumer) runKafkaBatchWorkerSupervisor(ctx context.Context, index int, ready func()) error {
	var recreationStreak uint64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.drainRequested() {
			return nil
		}
		err := c.runKafkaBatchWorker(ctx, index, ready)
		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
		var failClosed *kafkaBatchFailClosedError
		if errors.As(err, &failClosed) {
			return err
		}
		recreationStreak++
		delay := retryDelay(c.config.BaseRetry, c.config.MaxRetry, recreationStreak)
		c.setKafkaBatchBackoff(index, err, true)
		select {
		case <-time.After(delay):
			c.setKafkaBatchBackoff(index, nil, false)
		case <-ctx.Done():
			c.setKafkaBatchBackoff(index, nil, false)
			return ctx.Err()
		case <-c.drain:
			c.setKafkaBatchBackoff(index, nil, false)
			return nil
		}
	}
}

func (c *Consumer) setKafkaBatchBackoff(index int, err error, active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if active {
		c.batchBackoffWorkers++
		c.batchLastError = err
		if c.workerReady != nil {
			delete(c.workerReady, index)
		}
		c.workersReady = false
		return
	}
	if c.batchBackoffWorkers == 0 {
		return
	}
	c.batchBackoffWorkers--
	if c.batchBackoffWorkers != 0 {
		return
	}
	c.batchLastError = nil
	if len(c.workerReady) == c.config.Concurrency && c.state == consumerRunning {
		c.workersReady = true
	}
}

func (c *Consumer) runKafkaBatchWorker(ctx context.Context, index int, ready func()) error {
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
		kgo.RebalanceTimeout(c.batch.rebalanceTimeout),
	)
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: create batch consumer worker %d: %w", index, err)
	}
	defer session.CloseAllowingRebalance()
	if err := checkTransactionalStartup(sessionCtx, c.transport.config.OperationTimeout, session.Client()); err != nil {
		return fmt.Errorf("messenger/kafka: batch worker %d startup: %w", index, err)
	}
	return c.runKafkaBatchSession(sessionCtx, franzConsumerSession{session: session}, ready)
}

func (c *Consumer) runKafkaBatchSession(
	ctx context.Context,
	session transactionalConsumerSession,
	ready func(),
) error {
	scheduler := newRetryPartitionScheduler()
	markReady := func() {
		if ready == nil || ctx.Err() != nil || !consumerGroupJoined(session) {
			return
		}
		ready()
		ready = nil
	}
	markReady()
	var topLevelStreak uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.drain:
			return nil
		default:
		}
		now := c.clock().UTC()
		if due := scheduler.releaseDue(now); len(due) != 0 {
			session.ResumeFetchPartitions(due)
		}
		pollTimeout := scheduler.pollTimeout(now, time.Second)
		if pollTimeout <= 0 {
			continue
		}
		batch, err := c.collectKafkaBatch(ctx, session, pollTimeout)
		if err != nil {
			return err
		}
		markReady()
		if batch == nil {
			continue
		}
		if c.drainRequested() {
			rewindKafkaBatch(session, batch, batch.first)
			session.AllowRebalance()
			return nil
		}
		if len(batch.records) == 0 && batch.firstDeferred != nil {
			rewindKafkaBatch(session, batch, batch.firstDeferred)
			if err := c.pauseKafkaBatchPartition(session, scheduler,
				batch.firstDeferred, batch.deferUntil); err != nil {
				session.AllowRebalance()
				return err
			}
			session.AllowRebalance()
			c.logDeferredPartition(ctx, batch.firstDeferred, batch.deferUntil)
			continue
		}
		if err := c.processKafkaBatch(ctx, session, scheduler, batch, &topLevelStreak); err != nil {
			session.AllowRebalance()
			if errors.Is(err, errTransactionNotCommitted) {
				continue
			}
			return err
		}
	}
}

func (c *Consumer) collectKafkaBatch(
	ctx context.Context,
	session transactionalConsumerSession,
	pollTimeout time.Duration,
) (*kafkaPolledBatch, error) {
	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	fetches := session.PollRecords(pollCtx, c.batch.config.MaxMessages)
	pollErr := pollCtx.Err()
	cancel()
	if err := fetchError(fetches, pollErr); err != nil {
		session.AllowRebalance()
		return nil, fmt.Errorf("messenger/kafka: consume batch %s: %w", c.config.ConsumerID, err)
	}
	records := fetches.Records()
	if len(records) == 0 {
		session.AllowRebalance()
		return nil, nil //nolint:nilnil // Empty polls are a normal admission boundary.
	}
	batch := &kafkaPolledBatch{
		earliestOffsets: earliestKafkaOffsets(records), first: records[0], fillStarted: c.clock().UTC(),
		partition: topicPartition{topic: records[0].Topic, partition: records[0].Partition},
	}
	c.selectKafkaBatchRecords(batch, records)
	deadline := batch.fillStarted.Add(c.batch.config.MaxWait)
	for !batch.selectionStopped &&
		len(batch.records) < c.batch.config.MaxMessages &&
		batch.firstDeferred == nil &&
		c.clock().UTC().Before(deadline) &&
		batch.bytes < c.batch.config.MaxBytes {
		remaining := deadline.Sub(c.clock().UTC())
		if remaining <= 0 {
			break
		}
		fillCtx, cancelFill := context.WithTimeout(ctx, remaining)
		more := session.PollRecords(fillCtx, c.batch.config.MaxMessages-len(batch.records))
		fillErr := fillCtx.Err()
		cancelFill()
		if err := fetchError(more, fillErr); err != nil {
			return nil, fmt.Errorf("messenger/kafka: fill batch %s: %w", c.config.ConsumerID, err)
		}
		fetched := more.Records()
		if len(fetched) == 0 {
			break
		}
		recordEarliestKafkaOffsets(batch.earliestOffsets, fetched)
		prevCount := len(batch.records)
		c.selectKafkaBatchRecords(batch, fetched)
		if batch.selectionStopped || batch.bytes >= c.batch.config.MaxBytes || len(batch.records) == prevCount {
			break
		}
	}
	sort.Slice(batch.records, func(i, j int) bool {
		return batch.records[i].record.Offset < batch.records[j].record.Offset
	})
	return batch, nil
}

func (c *Consumer) selectKafkaBatchRecords(batch *kafkaPolledBatch, records []*kgo.Record) {
	for _, record := range records {
		partition := topicPartition{topic: record.Topic, partition: record.Partition}
		if partition != batch.partition {
			continue
		}
		if batch.selectionStopped || len(batch.records) >= c.batch.config.MaxMessages {
			if batch.firstUnprocessed == nil {
				batch.firstUnprocessed = record
			}
			batch.selectionStopped = true
			continue
		}
		prepared := c.prepareRecord(record)
		if !prepared.retryAt.IsZero() {
			batch.firstDeferred = record
			batch.deferUntil = prepared.retryAt
			batch.firstUnprocessed = record
			batch.selectionStopped = true
			continue
		}
		item := kafkaBatchRecord{record: record, prepared: prepared, bytes: len(record.Value)}
		if prepared.failure == nil {
			decoded, err := c.decode(prepared.envelope)
			if err != nil {
				item.prepared.failureKind = failureKindDecode
				item.prepared.failure = err
				item.prepared.attempt = max(uint64(1), prepared.control.attempt)
				item.prepared.messageID = prepared.envelope.envelope.ID.String()
			} else {
				item.decoded = decoded
				item.bytes = len(decoded.canonical)
			}
		}
		if hasKafkaBatchConflictingGeneration(batch.records, item) {
			batch.firstUnprocessed = record
			batch.selectionStopped = true
			continue
		}
		if len(batch.records) != 0 && item.bytes > c.batch.config.MaxBytes-batch.bytes {
			batch.firstUnprocessed = record
			batch.selectionStopped = true
			continue
		}
		batch.records = append(batch.records, item)
		batch.bytes += item.bytes
		if batch.bytes >= c.batch.config.MaxBytes {
			batch.selectionStopped = true
		}
	}
}

func hasKafkaBatchConflictingGeneration(records []kafkaBatchRecord, candidate kafkaBatchRecord) bool {
	candidateID := candidate.decoded.metadata.ID
	if candidateID.IsZero() {
		return false
	}
	candidateSource := candidate.decoded.metadata.Source
	candidateGen := candidate.prepared.control.attemptGeneration
	for _, prev := range records {
		if prev.decoded.metadata.ID == candidateID && prev.decoded.metadata.Source == candidateSource {
			if prev.prepared.control.attemptGeneration != candidateGen {
				return true
			}
		}
	}
	return false
}

type kafkaBatchFinalOutcome struct {
	item        inbox.BatchItemOutcome
	failureKind string
	err         error
}

//nolint:gocognit,gocyclo // Partial outcomes and the single Kafka transaction are intentionally explicit.
func (c *Consumer) processKafkaBatch(
	ctx context.Context,
	session transactionalConsumerSession,
	scheduler *retryPartitionScheduler,
	batch *kafkaPolledBatch,
	topLevelStreak *uint64,
) error {
	startedAt := c.clock().UTC()
	var handlerStarted time.Time
	var handlerDuration time.Duration
	var handlerMessages int
	var observationErr error
	var batchDuration time.Duration
	failObservation := func(err error) error {
		if err == nil {
			return nil
		}
		if observationErr != nil {
			observationErr = errors.Join(observationErr, err)
		} else {
			observationErr = err
		}
		return err
	}
	outcomes := make([]kafkaBatchFinalOutcome, len(batch.records))
	type pendingItemObservation struct {
		record    kafkaBatchRecord
		outcome   kafkaBatchFinalOutcome
		startedAt time.Time
		duration  time.Duration
	}
	type pendingBoundaryObservation struct {
		operation messenger.Operation
		messageID messenger.MessageID
		startedAt time.Time
		duration  time.Duration
		err       error
	}

	var pendingItemObs []pendingItemObservation
	var pendingBoundaryObs []pendingBoundaryObservation

	queueItemObservation := func(
		record kafkaBatchRecord,
		outcome kafkaBatchFinalOutcome,
		itemStarted time.Time,
		itemDuration time.Duration,
	) {
		if len(c.config.Observers) == 0 || record.decoded.metadata.ID.IsZero() {
			return
		}
		pendingItemObs = append(pendingItemObs, pendingItemObservation{
			record:    record,
			outcome:   outcome,
			startedAt: itemStarted,
			duration:  itemDuration,
		})
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
		pendingBoundaryObs = append(pendingBoundaryObs, pendingBoundaryObservation{
			operation: operation,
			messageID: messageID,
			startedAt: startedAt,
			duration:  duration,
			err:       err,
		})
	}
	var commitLog func()
	var rebalanceAllowed bool
	allowRebalance := func() {
		if !rebalanceAllowed {
			rebalanceAllowed = true
			session.AllowRebalance()
		}
	}
	defer func() {
		if batchDuration == 0 {
			batchDuration = c.clock().UTC().Sub(startedAt)
		}
		allowRebalance()
		if commitLog != nil {
			commitLog()
		}
		for _, p := range pendingItemObs {
			itemCtx := extractDeliveryContext(ctx, c.config, p.record.decoded.metadata.Headers)
			itemCtx = messenger.ContextWithMetadata(itemCtx, p.record.decoded.metadata)
			notifyObservers(itemCtx, c.config, messenger.Observation{
				Operation:     messenger.OperationHandle,
				MessageID:     p.record.decoded.metadata.ID,
				Kind:          p.record.decoded.metadata.Kind,
				Name:          p.record.decoded.metadata.Name,
				SchemaVersion: p.record.decoded.metadata.SchemaVersion,
				ConsumerID:    c.config.ConsumerID,
				HandlerID:     c.config.ConsumerID,
				Attempt:       p.outcome.item.Attempt,
				Duplicate:     p.outcome.item.Duplicate,
				RetryDelay:    p.outcome.item.Delay,
				StartedAt:     p.startedAt,
				Duration:      p.duration,
				Err:           sanitizeError(c.config.FailureSanitizer, p.outcome.err),
			})
		}
		for _, b := range pendingBoundaryObs {
			notifyObservers(ctx, c.config, messenger.Observation{
				Operation:  b.operation,
				MessageID:  b.messageID,
				ConsumerID: c.config.ConsumerID,
				HandlerID:  c.config.ConsumerID,
				StartedAt:  b.startedAt,
				Duration:   b.duration,
				Err:        sanitizeError(c.config.FailureSanitizer, b.err),
			})
		}
		c.observeKafkaBatch(ctx, batch, outcomes, handlerMessages, handlerDuration, startedAt, batchDuration, observationErr)
	}()
	valid := make([]inbox.BatchItem, 0, len(batch.records))
	validIndexes := make([]int, 0, len(batch.records))
	byItem := make(map[inbox.BatchItem]decodedMessage, len(batch.records))
	now := c.clock().UTC()
	for index, record := range batch.records {
		if record.prepared.failure != nil {
			outcomes[index] = kafkaBatchFinalOutcome{item: inbox.BatchItemOutcome{
				Outcome: inbox.BatchDLQ, Attempt: max(uint64(1), record.prepared.attempt),
			}, failureKind: record.prepared.failureKind, err: record.prepared.failure}
			continue
		}
		decoded := record.decoded
		if !decoded.metadata.ExpiresAt.IsZero() && !decoded.metadata.ExpiresAt.After(now) {
			outcomes[index] = kafkaBatchFinalOutcome{item: inbox.BatchItemOutcome{
				Outcome: inbox.BatchDLQ, Attempt: max(uint64(1), record.prepared.control.attempt),
			}, failureKind: "expired", err: ErrMessageExpired}
			continue
		}
		key := inbox.Key{
			ConsumerID: c.config.ConsumerID, Source: decoded.metadata.Source,
			MessageID: decoded.metadata.ID, AttemptGeneration: record.prepared.control.attemptGeneration,
		}
		item := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope(decoded.canonical)}
		if !decoded.metadata.NotBefore.IsZero() && decoded.metadata.NotBefore.After(now) {
			outcomes[index] = kafkaBatchFinalOutcome{item: inbox.BatchItemOutcome{
				Key: key, Fingerprint: item.Fingerprint, Outcome: inbox.BatchDefer,
				Attempt: record.prepared.control.attempt, Delay: decoded.metadata.NotBefore.Sub(now),
			}, err: ErrMessageNotReady}
			continue
		}
		valid = append(valid, item)
		validIndexes = append(validIndexes, index)
		if _, exists := byItem[item]; !exists {
			byItem[item] = decoded
		}
	}

	invoked := make(map[inbox.BatchItem]struct{}, len(valid))
	//nolint:nestif // The branch keeps the Inbox rollback boundary explicit.
	if len(valid) != 0 {
		transactionCtx, cancelTransaction := context.WithTimeout(ctx,
			handlerTransactionTimeout(c.config.Timeout, c.config.FinalizationTimeout))
		report, handlerErr := c.store.ProcessBatchAttempt(transactionCtx, valid,
			uint64(c.config.MaxAttempts), func( //nolint:gosec // Constructor requires a positive int.
				transactionHandlerCtx context.Context,
				active []inbox.BatchItem,
			) (messenger.BatchResult, error) {
				handlerMessages = len(active)
				handlerCtx, cancelHandler := context.WithTimeout(transactionHandlerCtx, c.config.Timeout)
				defer cancelHandler()
				decoded, err := kafkaBatchDecodedMessages(active, byItem)
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
				for _, record := range batch.records {
					item := inbox.BatchItem{
						Key: inbox.Key{
							ConsumerID:        c.config.ConsumerID,
							Source:            record.decoded.metadata.Source,
							MessageID:         record.decoded.metadata.ID,
							AttemptGeneration: record.prepared.control.attemptGeneration,
						},
						Fingerprint: inbox.FingerprintEnvelope(record.decoded.canonical),
					}
					if _, ok := invoked[item]; ok {
						queueItemObservation(record, kafkaBatchFinalOutcome{
							item: inbox.BatchItemOutcome{
								Key:         item.Key,
								Fingerprint: item.Fingerprint,
								Attempt:     max(uint64(1), record.prepared.control.attempt),
							},
							err: handlerErr,
						}, handlerStarted, handlerDuration)
					}
				}
				rewindKafkaBatch(session, batch, batch.first)
				allowRebalance()
				return failObservation(&kafkaBatchFailClosedError{cause: handlerErr})
			}
			(*topLevelStreak)++
			delay, explicit := messenger.DeferDelay(handlerErr)
			isDefer := explicit
			if !explicit {
				delay, explicit = messenger.RetryDelay(handlerErr)
			}
			if !explicit {
				delay = retryDelay(c.config.BaseRetry, c.config.MaxRetry, *topLevelStreak)
			}
			outcomeKind := inbox.BatchRetry
			if isDefer {
				outcomeKind = inbox.BatchDefer
			}
			for validIndex, inputIndex := range validIndexes {
				outcomes[inputIndex] = kafkaBatchFinalOutcome{
					item: inbox.BatchItemOutcome{
						Key:         valid[validIndex].Key,
						Fingerprint: valid[validIndex].Fingerprint,
						Outcome:     outcomeKind,
						Delay:       delay,
						Attempt:     batch.records[inputIndex].prepared.control.attempt,
					},
					err: handlerErr,
				}
			}
		} else {
			*topLevelStreak = 0
			for reportIndex, inputIndex := range validIndexes {
				outcomes[inputIndex] = kafkaBatchFinalOutcome{
					item: report.Items[reportIndex], failureKind: report.Items[reportIndex].FailureKind,
					err: report.Items[reportIndex].Err,
				}
			}
		}
	}

	for index, outcome := range outcomes {
		record := batch.records[index]
		item := inbox.BatchItem{
			Key: inbox.Key{
				ConsumerID:        c.config.ConsumerID,
				Source:            record.decoded.metadata.Source,
				MessageID:         record.decoded.metadata.ID,
				AttemptGeneration: record.prepared.control.attemptGeneration,
			},
			Fingerprint: inbox.FingerprintEnvelope(record.decoded.canonical),
		}
		if _, ok := invoked[item]; ok {
			queueItemObservation(record, outcome, handlerStarted, handlerDuration)
		} else if outcome.item.Duplicate {
			queueItemObservation(record, outcome, handlerStarted, 0)
		}
	}

	produced := make([]*kgo.Record, 0, len(outcomes))
	terminalConfirmations := make([]int, 0, len(outcomes))
	for index, outcome := range outcomes {
		record := batch.records[index]
		switch outcome.item.Outcome {
		case inbox.BatchACK:
		case inbox.BatchRetry, inbox.BatchDefer:
			retry, err := c.makeKafkaBatchRetry(record, outcome)
			if err != nil {
				allowRebalance()
				return failObservation(err)
			}
			produced = append(produced, retry)
		case inbox.BatchDLQ:
			dlq, err := c.makeKafkaBatchDLQ(record, outcome)
			if err != nil {
				allowRebalance()
				return failObservation(err)
			}
			produced = append(produced, dlq)
			if outcome.failureKind == inbox.FailurePermanent ||
				outcome.failureKind == inbox.FailureAttemptsExhausted {
				terminalConfirmations = append(terminalConfirmations, index)
			}
		default:
			allowRebalance()
			return failObservation(&kafkaBatchFailClosedError{cause: fmt.Errorf(
				"%w: missing Kafka outcome at index %d", messenger.ErrInvalidBatchResult, index)})
		}
	}
	setKafkaBatchProcessedOffsets(session, batch)
	commitStarted := c.clock().UTC()
	var committed bool
	var commitErr error
	committed, commitLog, commitErr = c.commitKafkaBatch(ctx, session, produced)
	commitDuration := c.clock().UTC().Sub(commitStarted)

	var finalizationErr error
	if commitErr != nil {
		finalizationErr = commitErr
	} else if !committed {
		finalizationErr = errTransactionNotCommitted
	}

	queueBoundaryObservation(operationOffsetCommit, messenger.MessageID{},
		commitStarted, commitDuration, finalizationErr)
	for index, outcome := range outcomes {
		switch outcome.item.Outcome {
		case inbox.BatchRetry, inbox.BatchDefer:
			queueBoundaryObservation(operationRetryHandoff,
				batch.records[index].decoded.metadata.ID, commitStarted, commitDuration, finalizationErr)
		case inbox.BatchDLQ:
			queueBoundaryObservation(operationDLQHandoff,
				batch.records[index].decoded.metadata.ID, commitStarted, commitDuration, finalizationErr)
		case inbox.BatchACK:
		}
	}
	if finalizationErr != nil {
		allowRebalance()
		return failObservation(finalizationErr)
	}
	if batch.firstDeferred != nil {
		if err := c.pauseKafkaBatchPartition(session, scheduler,
			batch.firstDeferred, batch.deferUntil); err != nil {
			allowRebalance()
			return failObservation(err)
		}
	}
	processingCompletedAt := c.clock().UTC()
	batchDuration = processingCompletedAt.Sub(startedAt)
	allowRebalance()
	if batch.firstDeferred != nil {
		c.logDeferredPartition(ctx, batch.firstDeferred, batch.deferUntil)
	}
	if len(terminalConfirmations) != 0 && c.store.SupportsTerminalRetention() {
		type confirmationKey struct {
			key         inbox.Key
			fingerprint inbox.Fingerprint
		}
		seen := make(map[confirmationKey]messenger.MessageID, len(terminalConfirmations))
		for _, index := range terminalConfirmations {
			outcome := outcomes[index]
			ck := confirmationKey{key: outcome.item.Key, fingerprint: outcome.item.Fingerprint}
			if _, exists := seen[ck]; !exists {
				seen[ck] = batch.records[index].decoded.metadata.ID
			}
		}
		confirmationCtx, cancelConfirmation := context.WithTimeout(ctx, 5*time.Second)
		defer cancelConfirmation()
		for ck, msgID := range seen {
			if confirmationCtx.Err() != nil {
				break
			}
			if err := c.store.ConfirmTerminalHandoff(confirmationCtx, ck.key, ck.fingerprint); err != nil {
				logInfrastructure(confirmationCtx, c.config.Logger, messenger.LogWarn, "confirm terminal handler handoff",
					messenger.LogAttr{Key: logAttrConsumerID, Value: c.config.ConsumerID},
					messenger.LogAttr{Key: logAttrMessageID, Value: msgID.String()},
					messenger.LogAttr{Key: logAttrError, Value: err})
			}
		}
	}
	return nil
}

func kafkaBatchDecodedMessages(
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

func (c *Consumer) makeKafkaBatchRetry(
	record kafkaBatchRecord,
	outcome kafkaBatchFinalOutcome,
) (*kgo.Record, error) {
	delay := outcome.item.Delay
	if delay <= 0 {
		delay = retryDelay(c.config.BaseRetry, c.config.MaxRetry,
			max(uint64(1), outcome.item.Attempt))
	}
	control := record.prepared.control
	control.attempt = outcome.item.Attempt
	control.notBefore = c.clock().UTC().Add(delay)
	canonical := record.decoded.canonical
	if len(canonical) == 0 {
		return nil, errors.New("messenger/kafka: retry outcome has no canonical envelope")
	}
	return &kgo.Record{
		Topic: c.retryTopics[retryTier(c.config.RetryTiers, delay)],
		Key:   append([]byte(nil), record.record.Key...), Value: append([]byte(nil), canonical...),
		Headers: controlHeaders(control), Timestamp: record.record.Timestamp,
	}, nil
}

func (c *Consumer) makeKafkaBatchDLQ(
	record kafkaBatchRecord,
	outcome kafkaBatchFinalOutcome,
) (*kgo.Record, error) {
	messageID := record.prepared.messageID
	if messageID == "" {
		messageID = record.decoded.metadata.ID.String()
	}
	attempt := max(uint64(1), outcome.item.Attempt)
	failureKind := outcome.failureKind
	if failureKind == "" {
		failureKind = "unknown"
	}
	dlq := makeDLQRecord(c.config.ConsumerID, record.record, record.prepared.control,
		messageID, attempt, failureKind, sanitizeError(defaultFailureSanitizer{}, outcome.err), c.clock())
	data, err := encodeDLQRecord(dlq)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	return &kgo.Record{Topic: c.dlqTopic, Key: digest[:], Value: data, Timestamp: dlq.FailedAt}, nil
}

func setKafkaBatchProcessedOffsets(session transactionalConsumerSession, batch *kafkaPolledBatch) {
	offsets := cloneKafkaOffsets(batch.earliestOffsets)
	last := batch.records[len(batch.records)-1].record
	byPartition := offsets[last.Topic]
	if byPartition == nil {
		byPartition = make(map[int32]kgo.EpochOffset)
		offsets[last.Topic] = byPartition
	}
	byPartition[last.Partition] = kgo.EpochOffset{
		Epoch: last.LeaderEpoch, Offset: last.Offset + 1,
	}
	session.SetOffsets(offsets)
}

func (c *Consumer) commitKafkaBatch(
	ctx context.Context,
	session transactionalConsumerSession,
	produced []*kgo.Record,
) (bool, func(), error) {
	return c.commitTransactionalRecords(ctx, session, produced)
}

func (c *Consumer) observeKafkaBatch(
	ctx context.Context,
	batch *kafkaPolledBatch,
	outcomes []kafkaBatchFinalOutcome,
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
		BatchSize: len(batch.records), BatchBytes: batch.bytes,
		BatchHandlerMessages: handlerMessages,
		BatchFillDuration:    startedAt.Sub(batch.fillStarted), BatchHandlerDuration: handlerDuration,
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

func rewindKafkaBatch(session transactionalConsumerSession, batch *kafkaPolledBatch, selected *kgo.Record) {
	offsets := cloneKafkaOffsets(batch.earliestOffsets)
	if selected != nil {
		byPartition := offsets[selected.Topic]
		if byPartition == nil {
			byPartition = make(map[int32]kgo.EpochOffset)
			offsets[selected.Topic] = byPartition
		}
		byPartition[selected.Partition] = kgo.EpochOffset{
			Epoch: selected.LeaderEpoch, Offset: selected.Offset,
		}
	}
	session.SetOffsets(offsets)
}

func recordEarliestKafkaOffsets(offsets map[string]map[int32]kgo.EpochOffset, records []*kgo.Record) {
	for _, record := range records {
		byPartition := offsets[record.Topic]
		if byPartition == nil {
			byPartition = make(map[int32]kgo.EpochOffset)
			offsets[record.Topic] = byPartition
		}
		current, exists := byPartition[record.Partition]
		if !exists || record.Offset < current.Offset {
			byPartition[record.Partition] = kgo.EpochOffset{
				Epoch: record.LeaderEpoch, Offset: record.Offset,
			}
		}
	}
}

func cloneKafkaOffsets(offsets map[string]map[int32]kgo.EpochOffset) map[string]map[int32]kgo.EpochOffset {
	cloned := make(map[string]map[int32]kgo.EpochOffset, len(offsets))
	for topic, partitions := range offsets {
		clonedPartitions := make(map[int32]kgo.EpochOffset, len(partitions))
		for partition, offset := range partitions {
			clonedPartitions[partition] = offset
		}
		cloned[topic] = clonedPartitions
	}
	return cloned
}

func earliestKafkaOffsets(records []*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	recordEarliestKafkaOffsets(offsets, records)
	return offsets
}

func (c *Consumer) pauseKafkaBatchPartition(
	session transactionalConsumerSession,
	scheduler *retryPartitionScheduler,
	record *kgo.Record,
	deadline time.Time,
) error {
	partition, ownedPause, err := pauseAndRewindRetryPartition(session, record)
	if err != nil {
		return err
	}
	scheduler.schedule(partition, deadline, ownedPause)
	return nil
}
