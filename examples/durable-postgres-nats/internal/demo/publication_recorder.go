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
	Recorded   int64  `json:"recorded"`
	Duplicates int64  `json:"duplicates"`
	Flushed    int64  `json:"flushed"`
	Batches    int64  `json:"batches"`
	Pending    int    `json:"pending"`
	Error      string `json:"error,omitempty"`
}

type publicationRecorder struct {
	write      publicationBatchWriter
	batchSize  int
	interval   time.Duration
	flushReady chan struct{}
	running    atomic.Bool

	flushGate chan struct{}
	mu        sync.Mutex
	pending   map[string]publicationConfirmation
	seen      map[string]publicationConfirmation
	failure   error
	recorded  int64
	dupes     int64
	flushed   int64
	batches   int64
}

func newPublicationRecorder(db *sql.DB) (*publicationRecorder, error) {
	if db == nil {
		return nil, errors.New("publication recorder database is required")
	}
	return newPublicationRecorderWithWriter(
		publicationBatchSize,
		publicationFlushInterval,
		func(ctx context.Context, batch []publicationConfirmation) error {
			return updatePublishedMeasurements(ctx, db, batch)
		},
	)
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
		flushReady: make(chan struct{}, 1),
		flushGate:  flushGate,
		pending:    make(map[string]publicationConfirmation),
		seen:       make(map[string]publicationConfirmation),
	}, nil
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
	flushErr := r.flushPending(ctx)
	r.mu.Lock()
	failure := r.failure
	r.mu.Unlock()
	return errors.Join(flushErr, failure)
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
	}
	if r.failure != nil {
		result.Error = r.failure.Error()
	}
	return result
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
	tag, err := db.ExecContext(ctx, `UPDATE demo.envelope_measurements AS measurement
		SET published_at = COALESCE(measurement.published_at, confirmed.published_at)
		FROM unnest(
			$1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::timestamptz[]
		) AS confirmed(message_id, run_id, stage_id, envelope_bytes, envelope_sha256, published_at)
		WHERE measurement.message_id = confirmed.message_id
		  AND measurement.run_id = confirmed.run_id
		  AND measurement.stage_id = confirmed.stage_id
		  AND measurement.envelope_bytes = confirmed.envelope_bytes
		  AND measurement.envelope_sha256 = confirmed.envelope_sha256`,
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
