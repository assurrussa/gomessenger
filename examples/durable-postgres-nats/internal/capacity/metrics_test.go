//nolint:testpackage // Tests exercise package-local metric boundaries and sustainability classification.
package capacity

import (
	"math"
	"testing"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

const loadPhase = "load"

func TestBuildStageReportExcludesDrainFromEffectiveThroughput(t *testing.T) {
	t.Parallel()
	config := Config{StageDuration: 10 * time.Second, E2EP95SLO: 2 * time.Second}
	initial := sampleAt(0, 0, 0, 10)
	loadEnd := sampleAt(10, 100, 2*bytesPerMiB, 110)
	final := sampleAt(20, 120, 3*bytesPerMiB, 130)
	k6 := K6Result{
		Iterations: 120, OfferedIterations: 120, AcceptedOrders: 120,
		HTTPRequests: 120, AcceptedRate: 1,
	}
	integrity := IntegrityResult{
		Orders: 120, DistinctOrderMessages: 120, Measurements: 120,
		BrokerConfirmed: 120, Projections: 120, DistinctProjectionMsgs: 120,
	}
	report := buildStageReport(stageReportInput{
		config: config, stageID: "r000010", rate: 10,
		loadStartedAt: time.Unix(0, 0), loadEndedAt: time.Unix(10, 0),
		drainDuration: 10 * time.Second, drainCompleted: true,
		initial: initial, loadEnd: loadEnd, final: final, samples: []Sample{initial, loadEnd}, k6: k6,
		latency: LatencyStats{P95Millis: 100}, envelopes: EnvelopeStats{Count: 120}, integrity: integrity,
	})
	if report.EffectiveMessagesPerSec != 10 {
		t.Fatalf("effective msg/s = %f, want 10", report.EffectiveMessagesPerSec)
	}
	if math.Abs(report.EffectiveMiBPerSec-0.2) > 1e-9 {
		t.Fatalf("effective MiB/s = %f, want 0.2", report.EffectiveMiBPerSec)
	}
	if report.AfterDrain.Committed != 120 || report.LoadWindow.Committed != 100 {
		t.Fatalf("committed counts = load %d final %d", report.LoadWindow.Committed, report.AfterDrain.Committed)
	}
	if !report.Integrity.Passed || !report.Sustainable {
		t.Fatalf("report should pass: %#v", report)
	}
}

func TestBacklogSlopeUsesFinalHalfOfLoadWindow(t *testing.T) {
	t.Parallel()
	samples := []Sample{
		{Phase: loadPhase, ElapsedSeconds: 0, Business: BusinessSnapshot{Accepted: 100, Committed: 0}},
		{Phase: loadPhase, ElapsedSeconds: 5, Business: BusinessSnapshot{Accepted: 10, Committed: 10}},
		{Phase: loadPhase, ElapsedSeconds: 7, Business: BusinessSnapshot{Accepted: 24, Committed: 20}},
		{Phase: loadPhase, ElapsedSeconds: 10, Business: BusinessSnapshot{Accepted: 40, Committed: 30}},
		{Phase: "drain", ElapsedSeconds: 11, Business: BusinessSnapshot{Accepted: 40, Committed: 40}},
	}
	slope := backlogSlope(samples, 10)
	if math.Abs(slope-2) > 1e-9 {
		t.Fatalf("backlog slope = %f, want 2", slope)
	}
}

func TestSustainabilityReconcilesLateBoundaryCompletionAfterDrain(t *testing.T) {
	t.Parallel()
	config := Config{StageDuration: 5 * time.Second, E2EP95SLO: 2 * time.Second}
	initial := sampleAt(0, 0, 0, 10)
	loadEnd := sampleAt(5, 25, 25_000, 35)
	final := sampleAt(6, 26, 26_000, 36)
	k6 := K6Result{
		Iterations: 26, OfferedIterations: 26, AcceptedOrders: 26,
		HTTPRequests: 26, AcceptedRate: 1,
	}
	integrity := IntegrityResult{
		Orders: 26, DistinctOrderMessages: 26, Measurements: 26,
		BrokerConfirmed: 26, Projections: 26, DistinctProjectionMsgs: 26,
	}
	report := buildStageReport(stageReportInput{
		config: config, stageID: "r000005", rate: 5,
		loadStartedAt: time.Unix(0, 0), loadEndedAt: time.Unix(5, 0),
		drainDuration: time.Second, drainCompleted: true,
		initial: initial, loadEnd: loadEnd, final: final, samples: []Sample{initial, loadEnd}, k6: k6,
		latency: LatencyStats{P95Millis: 100}, envelopes: EnvelopeStats{Count: 26}, integrity: integrity,
	})
	if !report.Sustainable || !report.Integrity.Passed {
		t.Fatalf("late boundary completion should reconcile after drain: %#v", report)
	}
}

func TestEvaluateIntegrityReportsEveryBoundaryMismatch(t *testing.T) {
	t.Parallel()
	result := evaluateIntegrity(IntegrityResult{
		Orders: 2, DistinctOrderMessages: 1, Measurements: 1,
		BrokerConfirmed: 0, Projections: 1, DistinctProjectionMsgs: 1,
		InvalidMeasurements: 1, MissingOrderLinks: 1, MissingProjectionLinks: 1,
	}, StageCounts{StreamPublished: 1}, 1)
	if result.Passed || len(result.Reasons) < 8 {
		t.Fatalf("integrity result = %#v", result)
	}
}

func sampleAt(elapsed float64, count int64, committedBytes int64, streamMessages uint64) Sample {
	return Sample{
		ElapsedSeconds: elapsed,
		Phase:          loadPhase,
		Business: BusinessSnapshot{
			Accepted: count, Staged: count, Published: count, Committed: count,
			CommittedBytes: committedBytes,
		},
		Broker:      BrokerSnapshot{StreamMessages: streamMessages},
		Application: demo.AppStats{Ready: true},
	}
}
