package capacity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

// IntegrityError reports a completed stage whose accepted messages could not
// be reconciled across the business, Outbox, broker, and Inbox boundaries.
type IntegrityError struct {
	StageID string
	Reasons []string
}

func (e *IntegrityError) Error() string {
	return fmt.Sprintf("capacity stage %s failed integrity reconciliation: %v", e.StageID, e.Reasons)
}

// MinimumRateError reports an opt-in local performance gate failure.
type MinimumRateError struct {
	Minimum int
	Actual  int
}

type execution struct {
	config    Config
	probe     *probe
	artifacts *artifacts
	log       *slog.Logger
}

type stageSpec struct {
	id       string
	rate     int
	duration time.Duration
	warmup   bool
}

type drainResult struct {
	final                Sample
	duration             time.Duration
	completedWithinLimit bool
	fullyDrained         bool
}

type postgresBoundaryResult struct {
	snapshot pgtelemetry.Snapshot
	err      error
}

func (e *MinimumRateError) Error() string {
	return fmt.Sprintf("maximum sustainable rate %d msg/s is below required %d msg/s", e.Actual, e.Minimum)
}

// Run executes warm-up and increasing open-loop stages until the first
// unsustainable rate or the end of the configured schedule.
func Run(ctx context.Context, config Config, log *slog.Logger) (report RunReport, runErr error) {
	if log == nil {
		log = slog.Default()
	}
	artifacts, err := newArtifacts(config.ResultDir())
	if err != nil {
		return RunReport{}, err
	}
	defer func() {
		joinErr := artifacts.close()
		if joinErr != nil {
			runErr = errors.Join(runErr, joinErr)
		}
	}()

	report = RunReport{
		SpecVersion: reportSpecVersion,
		RunID:       config.RunID,
		StartedAt:   time.Now().UTC(),
		Config: ReportConfig{
			Profile: config.Profile, Rates: append([]int(nil), config.Rates...),
			WarmupSeconds:         config.WarmupDuration.Seconds(),
			StageSeconds:          config.StageDuration.Seconds(),
			DrainTimeoutSeconds:   config.DrainTimeout.Seconds(),
			SampleIntervalSeconds: config.SampleInterval.Seconds(),
			E2EP95SLOMillis:       float64(config.E2EP95SLO.Milliseconds()),
			MinimumRate:           config.MinimumRate,
			PayloadProfile:        config.PayloadProfile,
		},
		IntegrityPassed: true,
		Stages:          make([]StageReport, 0, len(config.Rates)),
		Environment: Environment{
			HostOS: config.HostOS, HostArch: config.HostArch, HostCPUs: config.HostCPUs,
			GitCommit: config.GitCommit, GitDirty: config.GitDirty,
			OutboxWorkers: config.OutboxWorkers, ConsumerConcurrency: config.ConsumerConcurrency,
			DBMaxOpenConns:   config.DBMaxOpenConns,
			JetStreamStorage: "file",
		},
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, config.ReadyTimeout)
	defer readyCancel()
	probe, err := openProbe(readyCtx, config)
	if err != nil {
		return report, failReport(artifacts, &report, err)
	}
	defer func() {
		if err := probe.close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close capacity probe: %w", err))
		}
	}()
	if err := probe.waitReady(readyCtx); err != nil {
		return report, failReport(artifacts, &report, err)
	}
	environment, err := probe.environment(readyCtx, config)
	if err != nil {
		return report, failReport(artifacts, &report, err)
	}
	report.Environment = environment
	if err := artifacts.writeEnvironment(environment); err != nil {
		return report, failReport(artifacts, &report, err)
	}
	log.Info("capacity stack ready",
		"run_id", config.RunID, "profile", config.Profile, "results", config.ResultDir())
	experiment := &execution{config: config, probe: probe, artifacts: artifacts, log: log}

	warmup, err := experiment.runStage(ctx, stageSpec{
		id: "warmup", rate: config.Rates[0], duration: config.WarmupDuration, warmup: true,
	})
	if err != nil {
		return report, failReport(artifacts, &report, err)
	}
	report.Warmup = &warmup
	if !warmup.Integrity.Passed {
		report.IntegrityPassed = false
		err := &IntegrityError{StageID: warmup.StageID, Reasons: warmup.Integrity.Reasons}
		return report, failReport(artifacts, &report, err)
	}
	if err := artifacts.writeReport(report); err != nil {
		return report, err
	}

	firstUnsustainable, err := experiment.runMeasuredStages(ctx, &report)
	if err != nil {
		return report, failReport(artifacts, &report, err)
	}

	report.CompletedAt = time.Now().UTC()
	switch {
	case firstUnsustainable == 0:
		report.CapacityAtLeastTested = true
		report.CapacityStatement = fmt.Sprintf(
			"capacity >= %d msg/s under this checkout-local profile", report.MaxSustainableRate,
		)
	case report.MaxSustainableRate == 0:
		report.CapacityStatement = fmt.Sprintf(
			"capacity < %d msg/s under this checkout-local profile", firstUnsustainable,
		)
	default:
		report.CapacityStatement = fmt.Sprintf(
			"maximum sustainable tested rate is %d msg/s; first unsustainable rate is %d msg/s",
			report.MaxSustainableRate, firstUnsustainable,
		)
	}
	if err := artifacts.writeReport(report); err != nil {
		return report, err
	}
	if config.MinimumRate > 0 && report.MaxSustainableRate < config.MinimumRate {
		err := &MinimumRateError{Minimum: config.MinimumRate, Actual: report.MaxSustainableRate}
		return report, failReport(artifacts, &report, err)
	}
	return report, nil
}

func (e *execution) runMeasuredStages(ctx context.Context, report *RunReport) (int, error) {
	for _, rate := range e.config.Rates {
		stageID := fmt.Sprintf("r%06d", rate)
		stage, err := e.runStage(ctx, stageSpec{id: stageID, rate: rate, duration: e.config.StageDuration})
		if err != nil {
			return 0, err
		}
		report.Stages = append(report.Stages, stage)
		if !stage.Integrity.Passed {
			report.IntegrityPassed = false
			return 0, &IntegrityError{StageID: stage.StageID, Reasons: stage.Integrity.Reasons}
		}
		if stage.Sustainable {
			report.MaxSustainableRate = rate
		}
		if err := e.artifacts.writeReport(*report); err != nil {
			return 0, err
		}
		e.log.Info("capacity stage complete",
			"stage", stageID,
			"target_msg_s", rate,
			"effective_msg_s", stage.EffectiveMessagesPerSec,
			"effective_mib_s", stage.EffectiveMiBPerSec,
			"sustainable", stage.Sustainable,
			"drain_seconds", stage.DrainSeconds,
		)
		if !stage.Sustainable {
			return rate, nil
		}
	}
	return 0, nil
}

//nolint:gocognit // This is the single bounded orchestration state machine for load, snapshots, and drain.
func (e *execution) runStage(ctx context.Context, spec stageSpec) (StageReport, error) {
	labels := demo.BenchmarkLabels{RunID: e.config.RunID, StageID: spec.id}
	if err := waitQuiescent(ctx, e.config, e.probe, labels); err != nil {
		return StageReport{}, fmt.Errorf("prepare stage %s: %w", spec.id, err)
	}
	postgresBefore, err := e.probe.postgresSnapshot(ctx)
	if err != nil {
		return StageReport{}, err
	}
	initial, err := e.probe.snapshot(ctx, labels, "load", 0)
	if err != nil {
		return StageReport{}, err
	}
	samples := []Sample{initial}
	if err := e.artifacts.appendSample(initial); err != nil {
		return StageReport{}, err
	}
	e.log.Info("start capacity stage", "stage", spec.id, "target_msg_s", spec.rate, "duration", spec.duration)
	stageCtx, cancelStage := context.WithCancel(ctx)
	defer cancelStage()
	process, err := startK6(stageCtx, e.config, spec.id, spec.rate, spec.duration)
	if err != nil {
		return StageReport{}, err
	}
	controllerStartedAt := time.Now().UTC()
	ticker := time.NewTicker(e.config.SampleInterval)
	defer ticker.Stop()
	postgresBoundary := startPostgresBoundarySnapshot(
		stageCtx,
		spec.duration,
		func(snapshotCtx context.Context) (time.Time, error) {
			return e.probe.loadWindowStartedAt(snapshotCtx, labels)
		},
		e.probe.postgresSnapshot,
	)
	loadDeadline := time.NewTimer(spec.duration + 15*time.Second)
	defer loadDeadline.Stop()
	var loadEnd Sample
	var loadStartedAt time.Time
	var loadEndedAt time.Time
	var commandErr error
	var k6Result K6Result
	var postgresLoadEnd pgtelemetry.Snapshot
	processDone := process.done
	boundaryDone := postgresBoundary
	processCompleted := false
	boundaryCaptured := false
	for !processCompleted || !boundaryCaptured {
		select {
		case <-ctx.Done():
			cancelStage()
			var cleanupErr error
			if processCompleted {
				_, cleanupErr = process.finish(commandErr)
			} else {
				cleanupErr = process.abort()
			}
			return StageReport{}, errors.Join(ctx.Err(), cleanupErr)
		case commandErr = <-processDone:
			processDone = nil
			if time.Since(controllerStartedAt) < spec.duration-100*time.Millisecond {
				_, finishErr := process.finish(commandErr)
				return StageReport{}, errors.Join(
					fmt.Errorf("k6 stage %s stopped before its load window completed", spec.id), finishErr,
				)
			}
			processCompleted = true
		case boundary := <-boundaryDone:
			boundaryDone = nil
			if boundary.err != nil {
				cancelStage()
				var cleanupErr error
				if processCompleted {
					_, cleanupErr = process.finish(commandErr)
				} else {
					cleanupErr = process.abort()
				}
				return StageReport{}, errors.Join(boundary.err, cleanupErr)
			}
			postgresLoadEnd = boundary.snapshot
			boundaryCaptured = true
		case <-loadDeadline.C:
			cancelStage()
			var cleanupErr error
			if processCompleted {
				_, cleanupErr = process.finish(commandErr)
			} else {
				cleanupErr = process.abort()
			}
			return StageReport{}, errors.Join(
				fmt.Errorf("k6 stage %s exceeded its bounded load window", spec.id), cleanupErr,
			)
		case <-ticker.C:
			elapsed := time.Since(controllerStartedAt)
			_, err = takeSample(ctx, e.probe, e.artifacts, labels, "load", elapsed, &samples)
		}
		if err != nil {
			return StageReport{}, err
		}
	}
	if k6Result, err = process.finish(commandErr); err != nil {
		return StageReport{}, err
	}
	loadEnd, loadStartedAt, err = takeLoadEndSample(
		ctx, e.probe, e.artifacts, labels, spec.duration, controllerStartedAt, &samples,
	)
	if err != nil {
		return StageReport{}, err
	}
	loadEndedAt = loadStartedAt.Add(spec.duration)
	for index := range samples {
		samples[index].ElapsedSeconds = samples[index].ObservedAt.Sub(loadStartedAt).Seconds()
	}
	drain, err := drainStage(
		ctx, e.config, e.probe, e.artifacts, labels, loadStartedAt, &samples,
	)
	if err != nil {
		return StageReport{}, err
	}
	observedApplication, err := e.probe.applicationStats(ctx, labels)
	if err != nil {
		return StageReport{}, err
	}
	drain.final.Application.Consumer = observedApplication.Consumer
	postgresAfterDrain, err := e.probe.postgresSnapshot(ctx)
	if err != nil {
		return StageReport{}, err
	}
	latency, err := e.probe.latencyStats(ctx, labels)
	if err != nil {
		return StageReport{}, err
	}
	envelopes, err := e.probe.envelopeStats(ctx, labels)
	if err != nil {
		return StageReport{}, err
	}
	integrity, err := e.probe.integrity(ctx, labels)
	if err != nil {
		return StageReport{}, err
	}
	if !drain.fullyDrained {
		integrity.Reasons = append(integrity.Reasons, "pipeline did not reach a quiescent state for final reconciliation")
	}
	reportConfig := e.config
	reportConfig.StageDuration = spec.duration
	if spec.warmup {
		reportConfig.WarmupDuration = spec.duration
	}
	report := buildStageReport(stageReportInput{
		config: reportConfig, stageID: spec.id, warmup: spec.warmup, rate: spec.rate,
		loadStartedAt: loadStartedAt, loadEndedAt: loadEndedAt,
		drainDuration: drain.duration, drainCompleted: drain.completedWithinLimit,
		initial: initial, loadEnd: loadEnd, final: drain.final, samples: samples,
		k6: k6Result, latency: latency, envelopes: envelopes, integrity: integrity,
		postgres: pgtelemetry.BuildTimeline(postgresBefore, postgresLoadEnd, postgresAfterDrain),
	})
	return report, nil
}

func startPostgresBoundarySnapshot(
	ctx context.Context,
	duration time.Duration,
	loadStartedAt func(context.Context) (time.Time, error),
	capture func(context.Context) (pgtelemetry.Snapshot, error),
) <-chan postgresBoundaryResult {
	result := make(chan postgresBoundaryResult, 1)
	go func() {
		poll := time.NewTicker(25 * time.Millisecond)
		defer poll.Stop()
		var startedAt time.Time
		for startedAt.IsZero() {
			queryCtx, cancel := context.WithTimeout(ctx, time.Second)
			var err error
			startedAt, err = loadStartedAt(queryCtx)
			cancel()
			if err != nil {
				result <- postgresBoundaryResult{err: err}
				return
			}
			if !startedAt.IsZero() {
				break
			}
			select {
			case <-ctx.Done():
				result <- postgresBoundaryResult{err: ctx.Err()}
				return
			case <-poll.C:
			}
		}
		boundary := time.NewTimer(max(time.Until(startedAt.Add(duration)), 0))
		defer boundary.Stop()
		select {
		case <-ctx.Done():
			result <- postgresBoundaryResult{err: ctx.Err()}
			return
		case <-boundary.C:
		}
		snapshotCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		snapshot, err := capture(snapshotCtx)
		result <- postgresBoundaryResult{snapshot: snapshot, err: err}
	}()
	return result
}

func takeLoadEndSample(
	ctx context.Context,
	probe *probe,
	artifacts *artifacts,
	labels demo.BenchmarkLabels,
	duration time.Duration,
	fallbackStartedAt time.Time,
	samples *[]Sample,
) (Sample, time.Time, error) {
	sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	sample, err := probe.snapshot(sampleCtx, labels, "load", duration)
	if err != nil {
		return Sample{}, time.Time{}, err
	}
	business, startedAt, err := probe.loadWindowBusinessSnapshot(sampleCtx, labels, duration)
	if err != nil {
		return Sample{}, time.Time{}, err
	}
	if startedAt.IsZero() {
		startedAt = fallbackStartedAt
	}
	sample.Business = business
	sample.ObservedAt = startedAt.Add(duration)
	*samples = append(*samples, sample)
	if err := artifacts.appendSample(sample); err != nil {
		return Sample{}, time.Time{}, err
	}
	return sample, startedAt, nil
}

func takeSample(
	ctx context.Context,
	probe *probe,
	artifacts *artifacts,
	labels demo.BenchmarkLabels,
	phase string,
	elapsed time.Duration,
	samples *[]Sample,
) (Sample, error) {
	sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	sample, err := probe.snapshot(sampleCtx, labels, phase, elapsed)
	if err != nil {
		return Sample{}, err
	}
	*samples = append(*samples, sample)
	if err := artifacts.appendSample(sample); err != nil {
		return Sample{}, err
	}
	return sample, nil
}

func waitQuiescent(
	ctx context.Context,
	config Config,
	probe *probe,
	labels demo.BenchmarkLabels,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, config.DrainTimeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		sampleCtx, sampleCancel := context.WithTimeout(waitCtx, 3*time.Second)
		sample, err := probe.snapshot(sampleCtx, labels, "prepare", 0)
		sampleCancel()
		if err == nil && systemQuiescent(sample) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for empty Outbox and NATS consumer: %w", errors.Join(waitCtx.Err(), err))
		case <-ticker.C:
		}
	}
}

func drainStage(
	ctx context.Context,
	config Config,
	probe *probe,
	artifacts *artifacts,
	labels demo.BenchmarkLabels,
	loadStartedAt time.Time,
	samples *[]Sample,
) (drainResult, error) {
	drainStartedAt := time.Now()
	performanceDeadline := drainStartedAt.Add(config.DrainTimeout)
	reconciliationGrace := max(config.DrainTimeout, time.Minute)
	hardDeadline := performanceDeadline.Add(reconciliationGrace)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastWrittenAt := time.Time{}
	for {
		elapsed := time.Since(loadStartedAt)
		sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		sample, err := probe.snapshot(sampleCtx, labels, "drain", elapsed)
		cancel()
		if err != nil {
			return drainResult{}, err
		}
		if lastWrittenAt.IsZero() || time.Since(lastWrittenAt) >= config.SampleInterval || systemQuiescent(sample) {
			*samples = append(*samples, sample)
			if err := artifacts.appendSample(sample); err != nil {
				return drainResult{}, err
			}
			lastWrittenAt = time.Now()
		}
		if systemQuiescent(sample) {
			duration := time.Since(drainStartedAt)
			completedWithinLimit := !time.Now().After(performanceDeadline)
			return drainResult{
				final: sample, duration: duration,
				completedWithinLimit: completedWithinLimit, fullyDrained: true,
			}, nil
		}
		if time.Now().After(hardDeadline) {
			return drainResult{final: sample, duration: time.Since(drainStartedAt)}, nil
		}
		select {
		case <-ctx.Done():
			return drainResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func systemQuiescent(sample Sample) bool {
	return sample.Business.Accepted == sample.Business.Staged &&
		sample.Business.Staged == sample.Business.Published &&
		sample.Business.Published == sample.Business.Committed &&
		sample.Application.Outbox.Total == 0 &&
		sample.Broker.ConsumerPending == 0 &&
		sample.Broker.AckPending == 0
}

func failReport(artifacts *artifacts, report *RunReport, err error) error {
	report.CompletedAt = time.Now().UTC()
	report.Failure = err.Error()
	if writeErr := artifacts.writeReport(*report); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}
