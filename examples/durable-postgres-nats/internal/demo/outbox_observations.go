package demo

import (
	"context"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsrepo"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/outbox/models"
	"github.com/assurrussa/outbox/shared/types"
)

type outboxOperation struct {
	durations []time.Duration
	errors    int64
	messages  int64
	maximum   int
}

type outboxStageObservation struct {
	handler, publish, finalization outboxOperation
	outcomes                       BatchOutcomeStats
}

type outboxObservationRecorder struct {
	mu        sync.Mutex
	byStage   map[BenchmarkLabels]*outboxStageObservation
	jobLabels map[types.JobID]BenchmarkLabels
}

func newOutboxObservationRecorder() *outboxObservationRecorder {
	return &outboxObservationRecorder{
		byStage:   make(map[BenchmarkLabels]*outboxStageObservation),
		jobLabels: make(map[types.JobID]BenchmarkLabels),
	}
}

func envelopeLabels(payload []byte) (BenchmarkLabels, bool) {
	envelope, err := messenger.UnmarshalEnvelope(payload)
	if err != nil {
		return BenchmarkLabels{}, false
	}
	labels, measured, err := benchmarkLabels(envelope.Headers)
	return labels, measured && err == nil
}

func payloadLabelCounts(payloads [][]byte) map[BenchmarkLabels]int {
	counts := make(map[BenchmarkLabels]int)
	for _, payload := range payloads {
		if labels, ok := envelopeLabels(payload); ok {
			counts[labels]++
		}
	}
	return counts
}

func (r *outboxObservationRecorder) record(
	counts map[BenchmarkLabels]int,
	duration time.Duration,
	failed bool,
	operation func(*outboxStageObservation) *outboxOperation,
) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for labels, messages := range counts {
		if messages < 1 {
			continue
		}
		stage := r.byStage[labels]
		if stage == nil {
			stage = &outboxStageObservation{}
			r.byStage[labels] = stage
		}
		observed := operation(stage)
		observed.durations = append(observed.durations, duration)
		observed.messages += int64(messages)
		observed.maximum = max(observed.maximum, messages)
		if failed {
			observed.errors++
		}
	}
}

func (r *outboxObservationRecorder) recordHandler(payloads [][]byte, duration time.Duration, err error) {
	r.record(payloadLabelCounts(payloads), duration, err != nil,
		func(stage *outboxStageObservation) *outboxOperation { return &stage.handler })
}

func (r *outboxObservationRecorder) recordPublish(
	payloads [][]byte,
	duration time.Duration,
	itemErrors []error,
	topErr error,
) {
	counts := make(map[BenchmarkLabels]int)
	failures := make(map[BenchmarkLabels]bool)
	for index, payload := range payloads {
		labels, ok := envelopeLabels(payload)
		if !ok {
			continue
		}
		counts[labels]++
		if topErr != nil || (index < len(itemErrors) && itemErrors[index] != nil) {
			failures[labels] = true
		}
	}
	for labels, messages := range counts {
		r.record(map[BenchmarkLabels]int{labels: messages}, duration, failures[labels],
			func(stage *outboxStageObservation) *outboxOperation { return &stage.publish })
	}
}

func (r *outboxObservationRecorder) trackClaimed(jobs []models.Job) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, job := range jobs {
		if labels, ok := envelopeLabels([]byte(job.Payload)); ok {
			r.jobLabels[job.ID] = labels
		}
	}
}

func (r *outboxObservationRecorder) recordFinalization(
	outcomes []coreoutbox.BatchJobOutcome,
	duration time.Duration,
	err error,
) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make(map[BenchmarkLabels]int)
	classified := make(map[BenchmarkLabels]BatchOutcomeStats)
	for _, outcome := range outcomes {
		labels, ok := r.jobLabels[outcome.JobID]
		if !ok {
			continue
		}
		counts[labels]++
		item := classified[labels]
		switch outcome.Kind {
		case coreoutbox.BatchJobOutcomeSuccess:
			item.Success++
		case coreoutbox.BatchJobOutcomeRetry:
			item.Retry++
		case coreoutbox.BatchJobOutcomeDefer:
			item.Defer++
		case coreoutbox.BatchJobOutcomeDLQ:
			item.DLQ++
		}
		classified[labels] = item
		if err == nil && (outcome.Kind == coreoutbox.BatchJobOutcomeSuccess ||
			outcome.Kind == coreoutbox.BatchJobOutcomeDLQ) {
			delete(r.jobLabels, outcome.JobID)
		}
	}
	for labels, messages := range counts {
		stage := r.byStage[labels]
		if stage == nil {
			stage = &outboxStageObservation{}
			r.byStage[labels] = stage
		}
		stage.finalization.durations = append(stage.finalization.durations, duration)
		stage.finalization.messages += int64(messages)
		stage.finalization.maximum = max(stage.finalization.maximum, messages)
		if err != nil {
			stage.finalization.errors++
			continue
		}
		outcomes := classified[labels]
		stage.outcomes.Success += outcomes.Success
		stage.outcomes.Retry += outcomes.Retry
		stage.outcomes.Defer += outcomes.Defer
		stage.outcomes.DLQ += outcomes.DLQ
	}
}

func (r *outboxObservationRecorder) stats(labels BenchmarkLabels) OutboxExecutionStats {
	if r == nil {
		return OutboxExecutionStats{}
	}
	r.mu.Lock()
	stage := r.byStage[labels]
	if stage == nil {
		r.mu.Unlock()
		return OutboxExecutionStats{}
	}
	handler := copyOutboxOperation(stage.handler)
	publish := copyOutboxOperation(stage.publish)
	finalization := copyOutboxOperation(stage.finalization)
	outcomes := stage.outcomes
	r.mu.Unlock()
	return OutboxExecutionStats{
		Handler: operationBatchStats(handler), Publish: operationBatchStats(publish),
		Finalization: operationBatchStats(finalization), Outcomes: outcomes,
	}
}

func copyOutboxOperation(source outboxOperation) outboxOperation {
	source.durations = append([]time.Duration(nil), source.durations...)
	return source
}

func operationBatchStats(operation outboxOperation) BatchHandlerStats {
	invocations := int64(len(operation.durations))
	var average float64
	if invocations > 0 {
		average = float64(operation.messages) / float64(invocations)
	}
	return BatchHandlerStats{
		Invocations: invocations, Messages: operation.messages,
		AverageMessages: average, MaxMessages: operation.maximum,
		Handler: operationStats(operation.durations, operation.errors),
	}
}

type observedOutboxJobsRepository struct {
	*jobsrepo.Repo
	observations *outboxObservationRecorder
}

var _ coreoutbox.BoundedBatchJobsRepository = (*observedOutboxJobsRepository)(nil)

func (r *observedOutboxJobsRepository) FindAndReserveJobsForCapabilities(
	ctx context.Context,
	now, until time.Time,
	leaseToken coreoutbox.LeaseToken,
	capabilities []coreoutbox.JobCapability,
	limit int,
) ([]models.Job, error) {
	jobs, err := r.Repo.FindAndReserveJobsForCapabilities(ctx, now, until, leaseToken, capabilities, limit)
	if err == nil {
		r.observations.trackClaimed(jobs)
	}
	return jobs, err
}

func (r *observedOutboxJobsRepository) FindAndReserveJobsForCapability(
	ctx context.Context,
	now, until time.Time,
	leaseToken coreoutbox.LeaseToken,
	capability coreoutbox.JobCapability,
	limit int,
) ([]models.Job, error) {
	jobs, err := r.Repo.FindAndReserveJobsForCapability(ctx, now, until, leaseToken, capability, limit)
	if err == nil {
		r.observations.trackClaimed(jobs)
	}
	return jobs, err
}

func (r *observedOutboxJobsRepository) FindAndReserveJobsForCapabilityBounded(
	ctx context.Context,
	now, until time.Time,
	leaseToken coreoutbox.LeaseToken,
	capability coreoutbox.JobCapability,
	limits coreoutbox.BatchClaimLimits,
) ([]models.Job, error) {
	jobs, err := r.Repo.FindAndReserveJobsForCapabilityBounded(
		ctx,
		now,
		until,
		leaseToken,
		capability,
		limits,
	)
	if err == nil {
		r.observations.trackClaimed(jobs)
	}
	return jobs, err
}

func (r *observedOutboxJobsRepository) ApplyBatchJobOutcomes(
	ctx context.Context,
	leaseToken coreoutbox.LeaseToken,
	now time.Time,
	outcomes []coreoutbox.BatchJobOutcome,
) (int64, error) {
	started := time.Now()
	affected, err := r.Repo.ApplyBatchJobOutcomes(ctx, leaseToken, now, outcomes)
	r.observations.recordFinalization(outcomes, time.Since(started), err)
	return affected, err
}

func (r *observedOutboxJobsRepository) DeleteJobWithLease(
	ctx context.Context,
	jobID types.JobID,
	leaseToken coreoutbox.LeaseToken,
	now time.Time,
) (int64, error) {
	started := time.Now()
	affected, err := r.Repo.DeleteJobWithLease(ctx, jobID, leaseToken, now)
	if affected == 1 || err != nil {
		r.observations.recordFinalization([]coreoutbox.BatchJobOutcome{{
			JobID: jobID, Kind: coreoutbox.BatchJobOutcomeSuccess,
		}}, time.Since(started), err)
	}
	return affected, err
}

func (r *observedOutboxJobsRepository) RescheduleJobWithLease(
	ctx context.Context,
	jobID types.JobID,
	leaseToken coreoutbox.LeaseToken,
	now, availableAt time.Time,
) (int64, error) {
	started := time.Now()
	affected, err := r.Repo.RescheduleJobWithLease(ctx, jobID, leaseToken, now, availableAt)
	if affected == 1 || err != nil {
		r.observations.recordFinalization([]coreoutbox.BatchJobOutcome{{
			JobID: jobID, Kind: coreoutbox.BatchJobOutcomeRetry, AvailableAt: availableAt,
		}}, time.Since(started), err)
	}
	return affected, err
}

type observedRelayJob struct {
	delegate coreoutbox.VersionedJob
	recorder *outboxObservationRecorder
}

func (j observedRelayJob) Name() string {
	return j.delegate.Name()
}

func (j observedRelayJob) SchemaVersion() coreoutbox.SchemaVersion {
	return j.delegate.SchemaVersion()
}

func (j observedRelayJob) ExecutionTimeout() time.Duration {
	return j.delegate.ExecutionTimeout()
}

func (j observedRelayJob) MaxAttempts() int {
	return j.delegate.MaxAttempts()
}

func (j observedRelayJob) Handle(ctx context.Context, payload string) error {
	started := time.Now()
	err := j.delegate.Handle(ctx, payload)
	j.recorder.recordHandler([][]byte{[]byte(payload)}, time.Since(started), err)
	return err
}

type observedBatchRelayJob struct {
	delegate coreoutbox.VersionedBatchJob
	recorder *outboxObservationRecorder
}

func (j observedBatchRelayJob) Name() string { return j.delegate.Name() }
func (j observedBatchRelayJob) SchemaVersion() coreoutbox.SchemaVersion {
	return j.delegate.SchemaVersion()
}

func (j observedBatchRelayJob) ExecutionTimeout() time.Duration { return j.delegate.ExecutionTimeout() }
func (j observedBatchRelayJob) MaxAttempts() int                { return j.delegate.MaxAttempts() }
func (j observedBatchRelayJob) HandleBatch(
	ctx context.Context,
	items []coreoutbox.BatchJobItem,
) (coreoutbox.BatchResult, error) {
	payloads := make([][]byte, len(items))
	for index, item := range items {
		payloads[index] = []byte(item.Payload)
	}
	started := time.Now()
	result, err := j.delegate.HandleBatch(ctx, items)
	j.recorder.recordHandler(payloads, time.Since(started), err)
	return result, err
}

var (
	_ coreoutbox.VersionedJob      = observedRelayJob{}
	_ coreoutbox.VersionedBatchJob = observedBatchRelayJob{}
)
