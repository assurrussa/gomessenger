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
	config messenger.BatchConfig
	invoke func(context.Context, []decodedMessage) (messenger.BatchResult, error)
}

type kafkaBatchRecord struct {
	record   *kgo.Record
	prepared preparedRecord
	decoded  decodedMessage
	bytes    int
}

type kafkaPolledBatch struct {
	records          []kafkaBatchRecord
	all              []*kgo.Record
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
	consumer.batch = &kafkaBatchConsumer{config: normalized}
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
	opts := c.transport.workerOptions(c.groupID, instanceID, transactionID, c.topics)
	opts = append(opts, kgo.RebalanceTimeout(consumerRebalanceTimeout(c.transport.config.OperationTimeout)))
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("messenger/kafka: create batch consumer worker %d: %w", index, err)
	}
	defer session.CloseAllowingRebalance()
	if err := checkTransactionalStartup(ctx, c.transport.config.OperationTimeout, session.Client()); err != nil {
		return fmt.Errorf("messenger/kafka: batch worker %d startup: %w", index, err)
	}
	return c.runKafkaBatchSession(ctx, franzConsumerSession{session: session}, ready)
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
			if err := c.pauseKafkaBatchPartition(ctx, session, scheduler,
				batch.firstDeferred, batch.deferUntil); err != nil {
				session.AllowRebalance()
				return err
			}
			session.AllowRebalance()
			continue
		}
		if err := c.processKafkaBatch(ctx, session, scheduler, batch, &topLevelStreak); err != nil {
			session.AllowRebalance()
			if errors.Is(err, errTransactionNotCommitted) {
				continue
			}
			return err
		}
		session.AllowRebalance()
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
		all: records, first: records[0], fillStarted: c.clock().UTC(),
		partition: topicPartition{topic: records[0].Topic, partition: records[0].Partition},
	}
	c.selectKafkaBatchRecords(batch, records)
	deadline := batch.fillStarted.Add(c.batch.config.MaxWait)
	for len(batch.records) < c.batch.config.MaxMessages && batch.firstDeferred == nil &&
		c.clock().UTC().Before(deadline) && batch.bytes <= c.batch.config.MaxBytes {
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
		batch.all = append(batch.all, fetched...)
		c.selectKafkaBatchRecords(batch, fetched)
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
		if len(batch.records) == 1 && batch.bytes > c.batch.config.MaxBytes {
			return
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
) (processErr error) {
	startedAt := c.clock().UTC()
	var handlerDuration time.Duration
	var handlerMessages int
	outcomes := make([]kafkaBatchFinalOutcome, len(batch.records))
	defer func() {
		c.observeKafkaBatch(ctx, batch, outcomes, handlerMessages, handlerDuration, startedAt, processErr)
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

	//nolint:nestif // The branch keeps the Inbox rollback boundary explicit.
	if len(valid) != 0 {
		transactionCtx, cancelTransaction := context.WithTimeout(ctx,
			handlerTransactionTimeout(c.config.Timeout, c.config.FinalizationTimeout))
		report, processErr := c.store.ProcessBatchAttempt(transactionCtx, valid,
			uint64(c.config.MaxAttempts), func( //nolint:gosec // Constructor requires a positive int.
				transactionHandlerCtx context.Context,
				active []inbox.BatchItem,
			) (messenger.BatchResult, error) {
				handlerMessages = len(active)
				handlerCtx, cancelHandler := context.WithTimeout(ctx, c.config.Timeout)
				defer cancelHandler()
				if tx, ok := inbox.SQLTxFromContext(transactionHandlerCtx); ok {
					handlerCtx = inbox.ContextWithSQLTx(handlerCtx, tx)
				}
				decoded, err := kafkaBatchDecodedMessages(active, byItem)
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
			rewindKafkaBatch(session, batch, batch.first)
			if batchruntime.IsFailClosed(processErr) {
				return &kafkaBatchFailClosedError{cause: processErr}
			}
			(*topLevelStreak)++
			delay, explicit := messenger.DeferDelay(processErr)
			if !explicit {
				delay, explicit = messenger.RetryDelay(processErr)
			}
			if !explicit {
				delay = retryDelay(c.config.BaseRetry, c.config.MaxRetry, *topLevelStreak)
			}
			return c.pauseKafkaBatchPartition(ctx, session, scheduler, batch.first,
				c.clock().UTC().Add(delay))
		}
		*topLevelStreak = 0
		for reportIndex, inputIndex := range validIndexes {
			outcomes[inputIndex] = kafkaBatchFinalOutcome{
				item: report.Items[reportIndex], failureKind: report.Items[reportIndex].FailureKind,
				err: report.Items[reportIndex].Err,
			}
		}
	}

	produced := make([]*kgo.Record, 0, len(outcomes))
	terminalCleanup := make([]int, 0, len(outcomes))
	for index, outcome := range outcomes {
		record := batch.records[index]
		switch outcome.item.Outcome {
		case inbox.BatchACK:
		case inbox.BatchRetry, inbox.BatchDefer:
			retry, err := c.makeKafkaBatchRetry(record, outcome)
			if err != nil {
				return err
			}
			produced = append(produced, retry)
		case inbox.BatchDLQ:
			dlq, err := c.makeKafkaBatchDLQ(record, outcome)
			if err != nil {
				return err
			}
			produced = append(produced, dlq)
			if outcome.failureKind == inbox.FailurePermanent ||
				outcome.failureKind == inbox.FailureAttemptsExhausted {
				terminalCleanup = append(terminalCleanup, index)
			}
		default:
			return &kafkaBatchFailClosedError{cause: fmt.Errorf(
				"%w: missing Kafka outcome at index %d", messenger.ErrInvalidBatchResult, index)}
		}
	}
	setKafkaBatchProcessedOffsets(session, batch)
	committed, err := c.commitKafkaBatch(ctx, session, produced)
	if err != nil {
		return err
	}
	if !committed {
		processErr = errTransactionNotCommitted
		return processErr
	}
	commitStarted := c.clock().UTC()
	c.observeBoundary(ctx, operationOffsetCommit, messenger.MessageID{}, commitStarted, nil)
	for index, outcome := range outcomes {
		c.observeKafkaBatchItem(ctx, batch.records[index], outcome)
		switch outcome.item.Outcome {
		case inbox.BatchRetry, inbox.BatchDefer:
			c.observeBoundary(ctx, operationRetryHandoff,
				batch.records[index].decoded.metadata.ID, commitStarted, nil)
		case inbox.BatchDLQ:
			c.observeBoundary(ctx, operationDLQHandoff,
				batch.records[index].decoded.metadata.ID, commitStarted, nil)
		case inbox.BatchACK:
		}
	}
	for _, index := range terminalCleanup {
		outcome := outcomes[index]
		decoded := batch.records[index].decoded
		c.forgetAttempt(ctx, outcome.item.Key, outcome.item.Fingerprint, decoded.metadata.ID)
	}
	if batch.firstDeferred != nil {
		if err := c.pauseKafkaBatchPartition(ctx, session, scheduler,
			batch.firstDeferred, batch.deferUntil); err != nil {
			return err
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
		messageID, attempt, failureKind, sanitizeError(c.config.FailureSanitizer, outcome.err), c.clock())
	data, err := encodeDLQRecord(dlq)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	return &kgo.Record{Topic: c.dlqTopic, Key: digest[:], Value: data, Timestamp: dlq.FailedAt}, nil
}

func setKafkaBatchProcessedOffsets(session transactionalConsumerSession, batch *kafkaPolledBatch) {
	offsets := earliestKafkaOffsets(batch.all)
	last := batch.records[len(batch.records)-1].record
	offsets[last.Topic][last.Partition] = kgo.EpochOffset{
		Epoch: last.LeaderEpoch, Offset: last.Offset + 1,
	}
	session.SetOffsets(offsets)
}

func (c *Consumer) commitKafkaBatch(
	ctx context.Context,
	session transactionalConsumerSession,
	produced []*kgo.Record,
) (bool, error) {
	if err := session.Begin(); err != nil {
		return false, fmt.Errorf("messenger/kafka: begin batch transaction: %w", err)
	}
	brokerCtx, cancel := c.brokerContext(ctx)
	defer cancel()
	if len(produced) != 0 {
		if err := session.ProduceSync(brokerCtx, produced...).FirstErr(); err != nil {
			abortCtx, abortCancel := c.brokerContext(ctx)
			_, abortErr := session.End(abortCtx, kgo.TryAbort)
			abortCancel()
			return false, errors.Join(fmt.Errorf("messenger/kafka: transactional batch handoff: %w", err), abortErr)
		}
	}
	committed, err := session.End(brokerCtx, kgo.TryCommit)
	if err != nil {
		return false, fmt.Errorf("messenger/kafka: commit batch transaction: %w", err)
	}
	return committed, nil
}

func (c *Consumer) observeKafkaBatch(
	ctx context.Context,
	batch *kafkaPolledBatch,
	outcomes []kafkaBatchFinalOutcome,
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
		BatchSize: len(batch.records), BatchBytes: batch.bytes,
		BatchHandlerMessages: handlerMessages,
		BatchFillDuration:    startedAt.Sub(batch.fillStarted), BatchHandlerDuration: handlerDuration,
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

func (c *Consumer) observeKafkaBatchItem(
	ctx context.Context,
	record kafkaBatchRecord,
	outcome kafkaBatchFinalOutcome,
) {
	if len(c.config.Observers) == 0 || record.decoded.metadata.ID.IsZero() {
		return
	}
	itemCtx := extractDeliveryContext(ctx, c.config, record.decoded.metadata.Headers)
	itemCtx = messenger.ContextWithMetadata(itemCtx, record.decoded.metadata)
	notifyObservers(itemCtx, c.config, messenger.Observation{
		Operation: messenger.OperationHandle, MessageID: record.decoded.metadata.ID,
		Kind: record.decoded.metadata.Kind, Name: record.decoded.metadata.Name,
		SchemaVersion: record.decoded.metadata.SchemaVersion,
		ConsumerID:    c.config.ConsumerID, HandlerID: c.config.ConsumerID,
		Attempt: outcome.item.Attempt, Duplicate: outcome.item.Duplicate,
		RetryDelay: outcome.item.Delay,
		Err:        sanitizeError(c.config.FailureSanitizer, outcome.err),
	})
}

func rewindKafkaBatch(session transactionalConsumerSession, batch *kafkaPolledBatch, selected *kgo.Record) {
	offsets := earliestKafkaOffsets(batch.all)
	if selected != nil {
		offsets[selected.Topic][selected.Partition] = kgo.EpochOffset{
			Epoch: selected.LeaderEpoch, Offset: selected.Offset,
		}
	}
	session.SetOffsets(offsets)
}

func earliestKafkaOffsets(records []*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
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
	return offsets
}

func (c *Consumer) pauseKafkaBatchPartition(
	ctx context.Context,
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
	c.logDeferredPartition(ctx, record, deadline)
	return nil
}
