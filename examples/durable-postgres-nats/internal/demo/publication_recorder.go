package demo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	publicationBatchSize     = 256
	publicationFlushInterval = 50 * time.Millisecond
	measurementQueueCapacity = 100_000
)

type publicationConfirmation struct {
	envelopeMeasurement
	PublishedAt time.Time
}

type publicationBatchWriter func(context.Context, []publicationConfirmation) error

// PublicationRecorderStats reports the health and bounded in-memory state of
// broker-confirmation telemetry. A recorder error invalidates benchmark
// integrity but is deliberately not returned through the relay handler.
type PublicationRecorderStats struct {
	Recorded             int64  `json:"recorded"`
	Duplicates           int64  `json:"duplicates"`
	Flushed              int64  `json:"flushed"`
	Batches              int64  `json:"batches"`
	Pending              int    `json:"pending"`
	Error                string `json:"error,omitempty"`
	MeasurementsRecorded int64  `json:"measurementsRecorded"`
	MeasurementsFlushed  int64  `json:"measurementsFlushed"`
	MeasurementOverflow  int64  `json:"measurementOverflow"`
}

type publicationRecorder struct {
	write             publicationBatchWriter
	batchSize         int
	interval          time.Duration
	flushReady        chan struct{}
	running           atomic.Bool
	measurements      chan envelopeMeasurement
	writeMeasurements func(context.Context, []envelopeMeasurement) error

	flushGate            chan struct{}
	mu                   sync.Mutex
	pending              map[string]publicationConfirmation
	seen                 map[string]publicationConfirmation
	failure              error
	recorded             int64
	dupes                int64
	flushed              int64
	batches              int64
	measurementsRecorded int64
	measurementsFlushed  int64
	measurementOverflow  int64
}

func newPublicationRecorder(db *sql.DB) (*publicationRecorder, error) {
	if db == nil {
		return nil, errors.New("publication recorder database is required")
	}
	recorder, err := newPublicationRecorderWithWriter(
		publicationBatchSize,
		publicationFlushInterval,
		func(ctx context.Context, batch []publicationConfirmation) error {
			return updatePublishedMeasurements(ctx, db, batch)
		},
	)
	if err != nil {
		return nil, err
	}
	recorder.writeMeasurements = func(ctx context.Context, batch []envelopeMeasurement) error {
		return insertEnvelopeMeasurements(ctx, db, batch)
	}
	return recorder, nil
}

func newPublicationRecorderWithWriter(
	batchSize int,
	interval time.Duration,
	write publicationBatchWriter,
) (*publicationRecorder, error) {
	if batchSize < 1 || interval <= 0 || write == nil {
		return nil, errors.New("publication recorder requires a positive batch size, interval, and writer")
	}
	flushGate := make(chan struct{}, 1)
	flushGate <- struct{}{}
	return &publicationRecorder{
		write: write, batchSize: batchSize, interval: interval,
		flushReady:        make(chan struct{}, 1),
		flushGate:         flushGate,
		pending:           make(map[string]publicationConfirmation),
		seen:              make(map[string]publicationConfirmation),
		measurements:      make(chan envelopeMeasurement, measurementQueueCapacity),
		writeMeasurements: func(context.Context, []envelopeMeasurement) error { return nil },
	}, nil
}

// RecordMeasurement admits one staging observation to a bounded asynchronous
// recorder. Overflow invalidates the run but never changes delivery outcome.
func (r *publicationRecorder) RecordMeasurement(measurement envelopeMeasurement) {
	if r == nil {
		return
	}
	if measurement.MessageID == "" || measurement.EnvelopeBytes <= 0 || measurement.SHA256 == "" {
		r.fail(errors.New("envelope measurement is incomplete"))
		return
	}
	r.mu.Lock()
	r.measurementsRecorded++
	r.mu.Unlock()
	select {
	case r.measurements <- measurement:
	default:
		r.mu.Lock()
		r.measurementOverflow++
		r.setFailureLocked(errors.New("envelope measurement recorder overflow"))
		r.mu.Unlock()
	}
}

func (r *publicationRecorder) Record(confirmation publicationConfirmation) {
	if r == nil {
		return
	}
	if confirmation.MessageID == "" || confirmation.PublishedAt.IsZero() {
		r.fail(errors.New("publication confirmation requires message ID and PubAck timestamp"))
		return
	}
	confirmation.PublishedAt = confirmation.PublishedAt.UTC()

	r.mu.Lock()
	if previous, exists := r.seen[confirmation.MessageID]; exists {
		if !samePublication(previous, confirmation) {
			r.setFailureLocked(fmt.Errorf(
				"broker-confirmed envelope %s conflicts with its first confirmation",
				confirmation.MessageID,
			))
		} else {
			r.dupes++
		}
		r.mu.Unlock()
		return
	}
	r.seen[confirmation.MessageID] = confirmation
	r.pending[confirmation.MessageID] = confirmation
	r.recorded++
	shouldFlush := len(r.pending) >= r.batchSize
	r.mu.Unlock()

	if shouldFlush {
		select {
		case r.flushReady <- struct{}{}:
		default:
		}
	}
}

func (r *publicationRecorder) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("publication recorder is not initialized")
	}
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("publication recorder is already running")
	}
	defer r.running.Store(false)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.flushPending(ctx); err != nil {
				return err
			}
		case <-r.flushReady:
			if err := r.flushFullBatches(ctx); err != nil {
				return err
			}
		case measurement := <-r.measurements:
			if err := r.flushMeasurements(ctx, measurement); err != nil {
				return err
			}
		}
	}
}

func (r *publicationRecorder) Readiness(context.Context) error {
	if r == nil {
		return errors.New("publication recorder is not initialized")
	}
	r.mu.Lock()
	failure := r.failure
	r.mu.Unlock()
	if failure != nil {
		return failure
	}
	if !r.running.Load() {
		return errors.New("publication recorder is not running")
	}
	return nil
}

// Flush persists every confirmation currently in memory. Callers provide the
// bound; historical recorder failures remain visible even if a final retry can
// persist the retained batch.
func (r *publicationRecorder) Flush(ctx context.Context) error {
	if r == nil {
		return errors.New("publication recorder is not initialized")
	}
	measurementErr := r.flushAllMeasurements(ctx)
	flushErr := r.flushPending(ctx)
	r.mu.Lock()
	failure := r.failure
	r.mu.Unlock()
	return errors.Join(measurementErr, flushErr, failure)
}

func (r *publicationRecorder) Stats() PublicationRecorderStats {
	if r == nil {
		return PublicationRecorderStats{Error: "publication recorder is not initialized"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := PublicationRecorderStats{
		Recorded: r.recorded, Duplicates: r.dupes, Flushed: r.flushed,
		Batches: r.batches, Pending: len(r.pending),
		MeasurementsRecorded: r.measurementsRecorded,
		MeasurementsFlushed:  r.measurementsFlushed,
		MeasurementOverflow:  r.measurementOverflow,
	}
	if r.failure != nil {
		result.Error = r.failure.Error()
	}
	return result
}

func (r *publicationRecorder) flushMeasurements(ctx context.Context, first envelopeMeasurement) error {
	batch := make([]envelopeMeasurement, 0, r.batchSize)
	batch = append(batch, first)
	for len(batch) < r.batchSize {
		select {
		case measurement := <-r.measurements:
			batch = append(batch, measurement)
		default:
			if err := r.writeMeasurements(ctx, batch); err != nil {
				r.fail(fmt.Errorf("write envelope measurements: %w", err))
				return err
			}
			r.mu.Lock()
			r.measurementsFlushed += int64(len(batch))
			r.mu.Unlock()
			return nil
		}
	}
	if err := r.writeMeasurements(ctx, batch); err != nil {
		r.fail(fmt.Errorf("write envelope measurements: %w", err))
		return err
	}
	r.mu.Lock()
	r.measurementsFlushed += int64(len(batch))
	r.mu.Unlock()
	return nil
}

func (r *publicationRecorder) flushAllMeasurements(ctx context.Context) error {
	for {
		select {
		case measurement := <-r.measurements:
			if err := r.flushMeasurements(ctx, measurement); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// flushFullBatches services a size-trigger without chasing confirmations that
// arrive while a database write is in flight. A partial tail remains pending
// for the interval tick or the final bounded Flush.
func (r *publicationRecorder) flushFullBatches(ctx context.Context) error {
	if err := r.acquireFlush(ctx); err != nil {
		return err
	}
	defer r.releaseFlush()
	for {
		batch := r.takeFullBatch()
		if len(batch) == 0 {
			return nil
		}
		if err := r.writeBatch(ctx, batch); err != nil {
			return err
		}
	}
}

// flushPending persists the confirmations that were pending when the flush
// began. New arrivals are deliberately left for the next full-batch trigger or
// interval so a busy relay cannot turn one flush into continuous tiny writes.
func (r *publicationRecorder) flushPending(ctx context.Context) error {
	if err := r.acquireFlush(ctx); err != nil {
		return err
	}
	defer r.releaseFlush()

	remaining := r.pendingCount()
	for remaining > 0 {
		batch := r.takeBatch(min(r.batchSize, remaining))
		if len(batch) == 0 {
			return nil
		}
		remaining -= len(batch)
		if err := r.writeBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *publicationRecorder) acquireFlush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.flushGate:
		return nil
	}
}

func (r *publicationRecorder) releaseFlush() {
	r.flushGate <- struct{}{}
}

func (r *publicationRecorder) writeBatch(
	ctx context.Context,
	batch []publicationConfirmation,
) error {
	if err := r.write(ctx, batch); err != nil {
		r.restoreBatch(batch)
		err = fmt.Errorf("flush broker-confirmed envelope measurements: %w", err)
		if ctx.Err() == nil {
			r.fail(err)
		}
		return err
	}
	r.mu.Lock()
	for _, confirmation := range batch {
		delete(r.seen, confirmation.MessageID)
	}
	r.flushed += int64(len(batch))
	r.batches++
	r.mu.Unlock()
	return nil
}

func (r *publicationRecorder) pendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

func (r *publicationRecorder) takeFullBatch() []publicationConfirmation {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) < r.batchSize {
		return nil
	}
	return r.takeBatchLocked(r.batchSize)
}

func (r *publicationRecorder) takeBatch(limit int) []publicationConfirmation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.takeBatchLocked(limit)
}

func (r *publicationRecorder) takeBatchLocked(limit int) []publicationConfirmation {
	if len(r.pending) == 0 || limit < 1 {
		return nil
	}
	batch := make([]publicationConfirmation, 0, min(limit, len(r.pending)))
	for messageID, confirmation := range r.pending {
		batch = append(batch, confirmation)
		delete(r.pending, messageID)
		if len(batch) == limit {
			break
		}
	}
	return batch
}

func (r *publicationRecorder) restoreBatch(batch []publicationConfirmation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, confirmation := range batch {
		if _, exists := r.pending[confirmation.MessageID]; !exists {
			r.pending[confirmation.MessageID] = confirmation
		}
	}
}

func (r *publicationRecorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setFailureLocked(err)
}

func (r *publicationRecorder) setFailureLocked(err error) {
	if r.failure == nil {
		r.failure = err
	}
}

func samePublication(left, right publicationConfirmation) bool {
	return left.MessageID == right.MessageID &&
		left.Labels == right.Labels &&
		left.EnvelopeBytes == right.EnvelopeBytes &&
		left.SHA256 == right.SHA256
}

func updatePublishedMeasurements(
	ctx context.Context,
	db *sql.DB,
	batch []publicationConfirmation,
) error {
	messageIDs := make([]string, len(batch))
	runIDs := make([]string, len(batch))
	stageIDs := make([]string, len(batch))
	envelopeBytes := make([]int64, len(batch))
	digests := make([]string, len(batch))
	publishedAt := make([]time.Time, len(batch))
	for index, confirmation := range batch {
		messageIDs[index] = confirmation.MessageID
		runIDs[index] = confirmation.Labels.RunID
		stageIDs[index] = confirmation.Labels.StageID
		envelopeBytes[index] = confirmation.EnvelopeBytes
		digests[index] = confirmation.SHA256
		publishedAt[index] = confirmation.PublishedAt
	}
	tag, err := db.ExecContext(ctx, `INSERT INTO demo.envelope_measurements (
		message_id, run_id, stage_id, envelope_bytes, envelope_sha256, published_at
	)
	SELECT * FROM unnest(
			$1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::timestamptz[]
		)
	ON CONFLICT (message_id) DO UPDATE SET
		published_at = COALESCE(demo.envelope_measurements.published_at, EXCLUDED.published_at)
	WHERE demo.envelope_measurements.run_id = EXCLUDED.run_id
	  AND demo.envelope_measurements.stage_id = EXCLUDED.stage_id
	  AND demo.envelope_measurements.envelope_bytes = EXCLUDED.envelope_bytes
	  AND demo.envelope_measurements.envelope_sha256 = EXCLUDED.envelope_sha256`,
		messageIDs, runIDs, stageIDs, envelopeBytes, digests, publishedAt,
	)
	if err != nil {
		return err
	}
	rows, err := tag.RowsAffected()
	if err != nil {
		return fmt.Errorf("read publication batch result: %w", err)
	}
	if rows != int64(len(batch)) {
		return fmt.Errorf("publication batch matched %d of %d staged measurements", rows, len(batch))
	}
	return nil
}

func insertEnvelopeMeasurements(
	ctx context.Context,
	db *sql.DB,
	batch []envelopeMeasurement,
) error {
	messageIDs := make([]string, len(batch))
	runIDs := make([]string, len(batch))
	stageIDs := make([]string, len(batch))
	bytes := make([]int64, len(batch))
	digests := make([]string, len(batch))
	for index, measurement := range batch {
		messageIDs[index] = measurement.MessageID
		runIDs[index] = measurement.Labels.RunID
		stageIDs[index] = measurement.Labels.StageID
		bytes[index] = measurement.EnvelopeBytes
		digests[index] = measurement.SHA256
	}
	tag, err := db.ExecContext(ctx, `INSERT INTO demo.envelope_measurements (
		message_id, run_id, stage_id, envelope_bytes, envelope_sha256
	)
	SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::text[])
	ON CONFLICT (message_id) DO UPDATE SET message_id = EXCLUDED.message_id
	WHERE demo.envelope_measurements.run_id = EXCLUDED.run_id
	  AND demo.envelope_measurements.stage_id = EXCLUDED.stage_id
	  AND demo.envelope_measurements.envelope_bytes = EXCLUDED.envelope_bytes
	  AND demo.envelope_measurements.envelope_sha256 = EXCLUDED.envelope_sha256`,
		messageIDs, runIDs, stageIDs, bytes, digests,
	)
	if err != nil {
		return err
	}
	rows, err := tag.RowsAffected()
	if err != nil {
		return err
	}
	if rows != int64(len(batch)) {
		return fmt.Errorf("measurement batch matched %d of %d rows", rows, len(batch))
	}
	return nil
}
