package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

const (
	pilotSmokeMode    = "smoke"
	pilotSoakMode     = "soak"
	pilotRate         = 5
	pilotDuration     = 15 * time.Minute
	pilotSmokeTime    = 5 * time.Second
	pilotDrainTimeout = 30 * time.Second
	pilotTimeout      = 17 * time.Minute
)

// TestNATSSyntheticPilotSoak adds duration coverage to the unchanged v3 helpers.
// This is steady direct-NATS/SQLite coverage, not another failure campaign.
func TestNATSSyntheticPilotSoak(t *testing.T) {
	mode := os.Getenv("GOMESSENGER_NATS_PILOT_MODE")
	if mode == "" {
		t.Skip("set GOMESSENGER_NATS_PILOT_MODE=smoke or soak for explicit pilot execution")
	}
	duration, err := pilotModeDuration(mode)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := opsPrepareEvidence(os.Getenv("GOMESSENGER_NATS_PILOT_REPORT"))
	if err != nil {
		t.Fatal(err)
	}
	report := opsCampaignReport{
		Scope: "synthetic/steady/direct-NATS/shared-SQLite/" + mode +
			"; failure/restart evidence is separate; not PostgreSQL, Outbox, handler latency or production",
		StartedAt: time.Now().UTC(), GoVersion: runtime.Version(), CPUCount: runtime.NumCPU(),
	}
	t.Cleanup(func() {
		if err := evidence.ledgers.Close(); err != nil {
			t.Errorf("close pilot evidence directory: %v", err)
		}
		report.FinishedAt = time.Now().UTC()
		if t.Failed() {
			report.Passed = false
			if report.Failure == "" {
				report.Failure = "pilot test or owned-resource cleanup failed"
			}
		}
		if err := opsWriteReport(evidence.report, report); err != nil {
			t.Errorf("retain pilot report: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), pilotTimeout)
	defer cancel()
	if info, ok := debug.ReadBuildInfo(); ok {
		report.BuildInfo = info.String()
	}
	report.BinaryHash, err = opsExecutableHash()
	if err == nil {
		err = runSyntheticPilot(t, ctx, duration, &report, evidence)
	}
	if err != nil {
		report.Failure = err.Error()
		t.Fatal(err)
	}
	report.Passed = true
	// Cleanup may still fail and clear Passed before the final report is written.
	t.Logf("steady synthetic pilot completed: mode=%s duration=%s rate=%d/s", mode, duration, pilotRate)
}

func pilotModeDuration(mode string) (time.Duration, error) {
	switch mode {
	case pilotSmokeMode:
		return pilotSmokeTime, nil
	case pilotSoakMode:
		return pilotDuration, nil
	default:
		return 0, fmt.Errorf("unsupported pilot mode %q", mode)
	}
}

func runSyntheticPilot(t *testing.T, ctx context.Context, duration time.Duration,
	report *opsCampaignReport, evidence *opsEvidence,
) error {
	t.Helper()
	fixture, err := opsNewFixture(t, ctx, report, evidence)
	if err != nil {
		return err
	}
	for range 2 {
		if err := fixture.addConsumer(ctx, 1); err != nil {
			return err
		}
	}
	stage, err := runSyntheticPilotStage(ctx, fixture, duration)
	report.Stages = append(report.Stages, stage)
	if err != nil {
		return err
	}
	// The existing fixture also retains fallback ownership for every child.
	for _, child := range fixture.consumers {
		stopCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		err := child.stop(stopCtx)
		cancel()
		if err != nil {
			return err
		}
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return fixture.broker.stop(stopCtx)
}

func runSyntheticPilotStage(ctx context.Context, fixture *opsFixture, duration time.Duration,
) (result opsStageReport, runErr error) {
	result = opsStageReport{
		Name: "synthetic-steady-soak", Replicas: 2, Rate: pilotRate,
		DurationSeconds: duration.Seconds(), Expected: int(duration.Seconds()) * pilotRate,
	}
	state := &opsLoadState{byID: make(map[string]*opsDelivery)}
	defer func() {
		if err := fixture.retainStageLedger(&result, state); err != nil {
			runErr = errors.Join(runErr, err)
		}
		if runErr != nil {
			result.Failure = runErr.Error()
		}
	}()
	before, err := fixture.health(ctx)
	if err != nil {
		return result, err
	}
	// opsNewFixture creates an empty, isolated process_effects table.
	result.StartedAt = time.Now()
	result.LoadEndedAt = result.StartedAt.Add(duration)
	loadCtx, cancel := context.WithDeadline(ctx, result.LoadEndedAt.Add(pilotDrainTimeout))
	loadDone := make(chan struct{})
	// Copy the window into the child; named return slots may be assigned before
	// the deferred join runs on an early return.
	go func(startedAt, endedAt time.Time) {
		defer close(loadDone)
		fixture.generate(loadCtx, state, pilotRate, startedAt, endedAt, false)
	}(result.StartedAt, result.LoadEndedAt)
	defer func() {
		cancel()
		<-loadDone
	}()
	if err := waitSyntheticPilotDrain(loadCtx, fixture, state, &result, loadDone); err != nil {
		return result, err
	}
	if err := fixture.reconcile(loadCtx, state); err != nil {
		return result, err
	}
	after, err := fixture.health(loadCtx)
	if err != nil {
		return result, err
	}
	result.HandleErrors = after.HandleErrors - before.HandleErrors
	result.AckErrors = after.AckErrors - before.AckErrors
	result.ObservedDuplicates = after.Duplicates - before.Duplicates
	if err := opsSummarizeTiming(&result, state); err != nil {
		return result, err
	}
	pids := make([]int, 0, len(fixture.consumers))
	for _, child := range fixture.consumers {
		pids = append(pids, child.ready.PID)
	}
	if err := opsVerifyWorkerProgress(state.byID, pids, time.Time{}); err != nil {
		return result, err
	}
	result.IntegrityPassed = true
	result.Reasons = syntheticPilotReasons(result)
	result.Sustainable = len(result.Reasons) == 0
	if !result.Sustainable {
		return result, fmt.Errorf("synthetic steady acceptance failed: %v", result.Reasons)
	}
	return result, nil
}

func waitSyntheticPilotDrain(ctx context.Context, fixture *opsFixture, state *opsLoadState,
	result *opsStageReport, loadDone <-chan struct{},
) error {
	ticker := time.NewTicker(opsPollInterval)
	defer ticker.Stop()
	var middleLag, lastLag int
	var middleAt, lastAt time.Time
	midpoint := result.StartedAt.Add(result.LoadEndedAt.Sub(result.StartedAt) / 2)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("pilot load/drain deadline: %w", ctx.Err())
		case <-ticker.C:
		}
		if err := fixture.observe(ctx, state, time.Now()); err != nil {
			return err
		}
		state.mu.Lock()
		lag, publishErr := len(state.byID)-state.committed, state.firstError
		state.mu.Unlock()
		if publishErr != nil {
			return publishErr
		}
		pending, waiting, redelivered, err := fixture.brokerSnapshot(ctx)
		if err != nil {
			return err
		}
		if pending > opsAckWindow {
			return fmt.Errorf("pilot global ACK window exceeded: %d", pending)
		}
		result.MaxAckPending = max(result.MaxAckPending, pending)
		result.MaxRedelivered = max(result.MaxRedelivered, redelivered)
		now := time.Now()
		if now.Before(result.LoadEndedAt) {
			if middleAt.IsZero() && !now.Before(midpoint) {
				middleLag, middleAt = lag, now
			}
			lastLag, lastAt = lag, now
		}
		if !now.Before(result.LoadEndedAt) && pending == 0 && waiting == 0 && opsLoadSettled(state, loadDone) {
			result.DrainSeconds = now.Sub(result.LoadEndedAt).Seconds()
			if lastAt.After(middleAt) && !middleAt.IsZero() {
				result.LagGrowthPerSecond = float64(lastLag-middleLag) / lastAt.Sub(middleAt).Seconds()
			}
			return nil
		}
	}
}

func syntheticPilotReasons(stage opsStageReport) []string {
	var reasons []string
	if !stage.IntegrityPassed || stage.Expected <= 0 || stage.Attempted != stage.Expected ||
		stage.Confirmed != stage.Expected || stage.Committed != stage.Expected || stage.GeneratorDropped != 0 {
		reasons = append(reasons, "scheduled, admitted, confirmed and exact durable identities must reconcile")
	}
	if float64(stage.ConfirmedInWindow) < 0.99*float64(stage.Expected) ||
		float64(stage.ObservedInWindow) < 0.99*float64(stage.Expected) {
		reasons = append(reasons, "less than 99% completed within the scheduled window")
	}
	// This is offered-to-SQL-visibility latency, including the polling delay.
	// It is not a measurement of handler latency or the real-host ADR criterion.
	if stage.ObservedP95Millis >= 500 {
		reasons = append(reasons, "observed commit p95 upper bound must be below 500ms")
	}
	if stage.DrainSeconds > pilotDrainTimeout.Seconds() {
		reasons = append(reasons, "drain exceeded 30s")
	}
	if stage.LagGrowthPerSecond > 0.01*float64(pilotRate) {
		reasons = append(reasons, "second-half lag growth exceeded 1% of offered rate")
	}
	if stage.HandleErrors != 0 || stage.AckErrors != 0 || stage.MaxRedelivered != 0 || stage.ObservedDuplicates != 0 {
		reasons = append(reasons, "unexpected steady delivery error or redelivery")
	}
	return reasons
}

func TestNATSSyntheticPilotPolicy(t *testing.T) {
	if pilotSmokeMode != "smoke" || pilotSoakMode != "soak" ||
		pilotRate != 5 || pilotDuration != 15*time.Minute || pilotSmokeTime != 5*time.Second ||
		pilotDrainTimeout != 30*time.Second || pilotTimeout != 17*time.Minute {
		t.Fatal("synthetic pilot bounds changed")
	}
	for _, mode := range []string{pilotSmokeMode, pilotSoakMode} {
		got, err := pilotModeDuration(mode)
		if err != nil || (mode == pilotSmokeMode && got != 5*time.Second) || (mode == pilotSoakMode && got != 15*time.Minute) {
			t.Fatalf("mode %s: duration=%s err=%v", mode, got, err)
		}
	}
	if _, err := pilotModeDuration("custom"); err == nil {
		t.Fatal("unbounded custom mode accepted")
	}
	baseline := opsStageReport{
		Expected: 4500, Attempted: 4500, Confirmed: 4500, Committed: 4500,
		ConfirmedInWindow: 4500, ObservedInWindow: 4500, IntegrityPassed: true,
		ObservedP95Millis: 100, DrainSeconds: 1,
	}
	if got := syntheticPilotReasons(baseline); len(got) != 0 {
		t.Fatalf("baseline rejected: %v", got)
	}
	for _, test := range []struct {
		name string
		edit func(*opsStageReport)
	}{
		{name: "integrity", edit: func(s *opsStageReport) { s.IntegrityPassed = false }},
		{name: "admission", edit: func(s *opsStageReport) { s.Attempted-- }},
		{name: "confirmation", edit: func(s *opsStageReport) { s.Confirmed-- }},
		{name: "effect", edit: func(s *opsStageReport) { s.Committed-- }},
		{name: "drop", edit: func(s *opsStageReport) { s.GeneratorDropped++ }},
		{name: "puback window", edit: func(s *opsStageReport) { s.ConfirmedInWindow = 4454 }},
		{name: "commit window", edit: func(s *opsStageReport) { s.ObservedInWindow = 4454 }},
		{name: "latency boundary", edit: func(s *opsStageReport) { s.ObservedP95Millis = 500 }},
		{name: "drain", edit: func(s *opsStageReport) { s.DrainSeconds = 30.01 }},
		{name: "lag", edit: func(s *opsStageReport) { s.LagGrowthPerSecond = 0.051 }},
		{name: "handler", edit: func(s *opsStageReport) { s.HandleErrors = 1 }},
		{name: "ack", edit: func(s *opsStageReport) { s.AckErrors = 1 }},
		{name: "redelivery", edit: func(s *opsStageReport) { s.MaxRedelivered = 1 }},
		{name: "duplicate", edit: func(s *opsStageReport) { s.ObservedDuplicates = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := baseline
			test.edit(&candidate)
			if len(syntheticPilotReasons(candidate)) == 0 {
				t.Fatal("failed criterion was accepted")
			}
		})
	}
}
