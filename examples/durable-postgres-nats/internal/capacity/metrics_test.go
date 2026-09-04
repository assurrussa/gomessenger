//nolint:testpackage // Tests exercise package-local metric boundaries and sustainability classification.
package capacity

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

const loadPhase = "load"

func TestBuildStageReportSeparatesRelayAndConsumerAndExcludesDrain(t *testing.T) {
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
	if report.RelayMessagesPerSec != 10 {
		t.Fatalf("relay msg/s = %f, want 10", report.RelayMessagesPerSec)
	}
	if report.ConsumerMessagesPerSec != 10 {
		t.Fatalf("consumer msg/s = %f, want 10", report.ConsumerMessagesPerSec)
	}
	if math.Abs(report.ConsumerMiBPerSec-0.2) > 1e-9 {
		t.Fatalf("consumer MiB/s = %f, want 0.2", report.ConsumerMiBPerSec)
	}
	if report.OutboxLag != 0 || report.ConsumerLag != 0 {
		t.Fatalf("load-window lags = Outbox %d consumer %d, want zero", report.OutboxLag, report.ConsumerLag)
	}
	if report.AfterDrain.Committed != 120 || report.LoadWindow.Committed != 100 {
		t.Fatalf("committed counts = load %d final %d", report.LoadWindow.Committed, report.AfterDrain.Committed)
	}
	if !report.Integrity.Passed || !report.Sustainable {
		t.Fatalf("report should pass: %#v", report)
	}
}

func TestBuildStageReportClassifiesUnavailableLoadTelemetryAsUnsustainable(t *testing.T) {
	t.Parallel()
	report := buildStageReport(stageReportInput{
		config:  Config{StageDuration: time.Second, E2EP95SLO: 2 * time.Second},
		stageID: "r000100", rate: 100, drainCompleted: true,
		k6:                 K6Result{AcceptedRate: 1},
		loadSampleFailures: 2,
	})
	if report.Sustainable {
		t.Fatal("report with unavailable load telemetry must be unsustainable")
	}
	if !containsReason(report.UnsustainableReasons, "application telemetry was unavailable for 2 load samples") {
		t.Fatalf("reasons = %q", report.UnsustainableReasons)
	}
}

func TestBuildStageReportCalculatesDistinctPipelineBoundaries(t *testing.T) {
	t.Parallel()
	config := Config{StageDuration: 10 * time.Second, E2EP95SLO: 2 * time.Second}
	initial := sampleWithBoundaries(0, 0, 0, 0, 0)
	loadEnd := sampleWithBoundaries(10, 100, 90, 80, 2*bytesPerMiB)
	final := sampleWithBoundaries(20, 120, 120, 120, 3*bytesPerMiB)
	report := buildStageReport(stageReportInput{
		config: config, stageID: "r000010", rate: 10,
		loadStartedAt: time.Unix(0, 0), loadEndedAt: time.Unix(10, 0),
		drainDuration: 10 * time.Second, drainCompleted: true,
		initial: initial, loadEnd: loadEnd, final: final, samples: []Sample{initial, loadEnd},
		k6:      K6Result{Iterations: 120, OfferedIterations: 120, AcceptedOrders: 120, HTTPRequests: 120, AcceptedRate: 1},
		latency: LatencyStats{P95Millis: 100}, envelopes: EnvelopeStats{Count: 120},
		integrity: IntegrityResult{
			Orders: 120, DistinctOrderMessages: 120, Measurements: 120,
			BrokerConfirmed: 120, Projections: 120, DistinctProjectionMsgs: 120,
		},
	})
	if report.RelayMessagesPerSec != 9 || report.ConsumerMessagesPerSec != 8 {
		t.Fatalf("pipeline rates = relay %.2f consumer %.2f", report.RelayMessagesPerSec, report.ConsumerMessagesPerSec)
	}
	if report.OutboxLag != 10 || report.ConsumerLag != 10 {
		t.Fatalf("pipeline lags = Outbox %d consumer %d", report.OutboxLag, report.ConsumerLag)
	}
	if math.Abs(report.ConsumerMiBPerSec-0.2) > 1e-9 {
		t.Fatalf("consumer MiB/s = %f, want 0.2", report.ConsumerMiBPerSec)
	}
	if report.AfterDrain.Staged != 120 || report.AfterDrain.Published != 120 || report.AfterDrain.Committed != 120 {
		t.Fatalf("post-drain counts = %#v", report.AfterDrain)
	}
}

func TestBacklogSlopeUsesFinalHalfOfLoadWindow(t *testing.T) {
	t.Parallel()
	samples := []Sample{
		{Phase: loadPhase, ElapsedSeconds: 0, Business: BusinessSnapshot{Accepted: 100, Committed: 0}},
		{Phase: loadPhase, ElapsedSeconds: 5, Business: BusinessSnapshot{Accepted: 10, Committed: 10}},
		{Phase: loadPhase, ElapsedSeconds: 7, Business: BusinessSnapshot{Accepted: 24, Committed: 20}},
		{Phase: loadPhase, ElapsedSeconds: 10, Business: BusinessSnapshot{Accepted: 40, Committed: 30}},
		// k6 may use its graceful-stop allowance after the exact load window while
		// the sampler still labels the point as load. That completion must not
		// flatten the bounded load-window regression.
		{Phase: loadPhase, ElapsedSeconds: 12, Business: BusinessSnapshot{Accepted: 40, Committed: 40}},
		{Phase: "drain", ElapsedSeconds: 11, Business: BusinessSnapshot{Accepted: 40, Committed: 40}},
	}
	slope := backlogSlope(samples, 10)
	if math.Abs(slope-2) > 1e-9 {
		t.Fatalf("backlog slope = %f, want 2", slope)
	}
}

func TestPipelineLagSlopesUseSeparateBoundaries(t *testing.T) {
	t.Parallel()
	samples := []Sample{
		{Phase: loadPhase, ElapsedSeconds: 5, Business: BusinessSnapshot{Staged: 25, Published: 15}},
		{Phase: loadPhase, ElapsedSeconds: 7, Business: BusinessSnapshot{Staged: 35, Published: 21}},
		{Phase: loadPhase, ElapsedSeconds: 10, Business: BusinessSnapshot{Staged: 50, Published: 30}},
	}
	if slope := outboxLagSlope(samples, 10); math.Abs(slope-2) > 1e-9 {
		t.Fatalf("Outbox lag slope = %f, want 2", slope)
	}
	if slope := consumerLagSlope(samples, 10); math.Abs(slope-3) > 1e-9 {
		t.Fatalf("consumer lag slope = %f, want 3", slope)
	}
}

func TestSustainabilityReasonsSeparateRelayConsumerAndLagGrowth(t *testing.T) {
	t.Parallel()
	reasons := sustainabilityReasons(Config{E2EP95SLO: 2 * time.Second}, StageReport{
		TargetRate:              100,
		AcceptedMessagesPerSec:  100,
		RelayMessagesPerSec:     90,
		ConsumerMessagesPerSec:  80,
		OutboxLagGrowthPerSec:   5,
		ConsumerLagGrowthPerSec: 6,
		BacklogSlopePerSec:      7,
		DrainCompleted:          true,
		K6: K6Result{
			AcceptedRate: 1,
		},
	})
	for _, fragment := range []string{
		"relay throughput 90.00 msg/s",
		"consumer throughput 80.00 msg/s",
		"Outbox lag growth 5.00 msg/s",
		"consumer lag growth 6.00 msg/s",
		"end-to-end backlog growth 7.00 msg/s",
	} {
		if !containsReason(reasons, fragment) {
			t.Fatalf("reasons %q do not contain %q", reasons, fragment)
		}
	}
}

func TestSustainabilityRejectsOutboxExecutionErrorsAndNonSuccessOutcomes(t *testing.T) {
	t.Parallel()
	reasons := sustainabilityReasons(Config{E2EP95SLO: 2 * time.Second}, StageReport{
		TargetRate:             100,
		AcceptedMessagesPerSec: 100,
		RelayMessagesPerSec:    100,
		ConsumerMessagesPerSec: 100,
		DrainCompleted:         true,
		K6:                     K6Result{AcceptedRate: 1},
		OutboxExecution: demo.OutboxExecutionStats{
			Handler:      demo.BatchHandlerStats{Handler: demo.OperationStats{Errors: 1}},
			Publish:      demo.BatchHandlerStats{Handler: demo.OperationStats{Errors: 2}},
			Finalization: demo.BatchHandlerStats{Handler: demo.OperationStats{Errors: 3}},
			Outcomes:     demo.BatchOutcomeStats{Retry: 4, Defer: 5, DLQ: 6},
		},
	})
	for _, fragment := range []string{
		"Outbox observations contain errors (handler=1 publish=2 finalization=3)",
		"unexpected Outbox outcomes observed (retry=4 defer=5 dlq=6)",
	} {
		if !containsReason(reasons, fragment) {
			t.Fatalf("reasons %q do not contain %q", reasons, fragment)
		}
	}
}

func TestPGXPoolStageStatsDiscountsOnlyObservedInitialExpansion(t *testing.T) {
	t.Parallel()

	initial := demo.PGXPoolStats{
		MaxConnections: 9, TotalConnections: 1, NewConnectionsCount: 1,
	}
	final := demo.PGXPoolStats{
		MaxConnections: 9, TotalConnections: 9, MaxAcquiredConnections: 9,
		NewConnectionsCount: 9,
	}
	stats := pgxPoolStageStats(initial, final)
	if stats.NewConnections != 8 || stats.ReplacementConnections != 0 {
		t.Fatalf("initial expansion stats = %#v, want 8 new and zero replacements", stats)
	}

	final.NewConnectionsCount = 11
	final.CanceledAcquireCount = 2
	final.UnusableReleaseCount = 3
	stats = pgxPoolStageStats(initial, final)
	if stats.NewConnections != 10 || stats.ReplacementConnections != 2 ||
		stats.CanceledAcquires != 2 || stats.UnusableReleases != 3 {
		t.Fatalf("replacement stats = %#v", stats)
	}
}

func TestSustainabilityRejectsOutboxPoolConnectionChurn(t *testing.T) {
	t.Parallel()

	reasons := sustainabilityReasons(Config{E2EP95SLO: 2 * time.Second}, StageReport{
		TargetRate:             100,
		AcceptedMessagesPerSec: 100,
		RelayMessagesPerSec:    100,
		ConsumerMessagesPerSec: 100,
		DrainCompleted:         true,
		K6:                     K6Result{AcceptedRate: 1},
		OutboxDatabase: OutboxDatabaseStats{
			Producer: PGXPoolStageStats{
				MaxConnections: 9, MaxAcquiredConnections: 10,
				ReplacementConnections: 1, CanceledAcquires: 2, UnusableReleases: 3,
			},
			Relay: PGXPoolStageStats{MaxConnections: 1, MaxAcquiredConnections: 1},
		},
	})
	for _, fragment := range []string{
		"producer pool replaced 1 connections",
		"producer pool canceled 2 acquires",
		"producer pool released 3 unusable connections",
		"producer pool acquired high-water 10 exceeds max 9",
	} {
		if !containsReason(reasons, fragment) {
			t.Fatalf("reasons %q do not contain %q", reasons, fragment)
		}
	}
}

func TestReportSpec21JSONAndMarkdownExposeBatchExecutionAndNormalizedCost(t *testing.T) {
	t.Parallel()
	stage := StageReport{
		RelayMessagesPerSec: 2_000, ConsumerMessagesPerSec: 1_990, ConsumerMiBPerSec: 1.5,
		OutboxLag: 10, ConsumerLag: 20,
		ConsumerBatch: demo.BatchHandlerStats{
			Invocations: 20, Messages: 1_990, AverageMessages: 99.5, MaxMessages: 100,
			Handler: demo.OperationStats{P95Millis: 8.5},
		},
		OutboxExecution: demo.OutboxExecutionStats{
			Handler: demo.BatchHandlerStats{
				Invocations: 20, Messages: 1_990, AverageMessages: 99.5, MaxMessages: 100,
				Handler: demo.OperationStats{P95Millis: 9.5},
			},
			Publish: demo.BatchHandlerStats{
				Invocations: 20, Messages: 1_990, AverageMessages: 99.5, MaxMessages: 100,
				Handler: demo.OperationStats{P95Millis: 7.5},
			},
			Finalization: demo.BatchHandlerStats{
				Invocations: 20, Messages: 1_990, AverageMessages: 99.5, MaxMessages: 100,
				Handler: demo.OperationStats{P95Millis: 6.5},
			},
			Outcomes: demo.BatchOutcomeStats{Success: 1_990},
		},
		PostgreSQLNormalized: PostgreSQLNormalizedStats{
			SQLCalls: 70, Transactions: 30, TransactionsPerMessage: 0.015,
			WALBytes: 640_000, WALBytesPerMessage: 321.61, CompletedCheckpoints: 1,
		},
		OutboxDatabase: OutboxDatabaseStats{
			Producer: PGXPoolStageStats{MaxConnections: 9, MaxAcquiredConnections: 9, NewConnections: 8},
			Relay:    PGXPoolStageStats{MaxConnections: 1, MaxAcquiredConnections: 1},
		},
	}
	encoded, err := json.Marshal(stage)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"relayMessagesPerSecond", "consumerMessagesPerSecond", "consumerMiBPerSecond", "outboxLag", "consumerLag",
		"consumerBatch", "outboxExecution", "postgresqlNormalized", "outboxDatabase",
	} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("missing JSON field %q in %s", field, encoded)
		}
	}
	for _, field := range []string{"effectiveMessagesPerSecond", "effectiveMiBPerSecond"} {
		if _, ok := fields[field]; ok {
			t.Fatalf("legacy JSON field %q remains in %s", field, encoded)
		}
	}
	environment := Environment{
		OutboxVersion: testOutboxVersion, PostgreSQLSettings: map[string]string{},
		ConsumerMode: "batch", ConsumerConcurrency: 2,
		ConsumerBatchMaxMessages: 100, ConsumerBatchMaxBytes: 4 << 20,
		ConsumerBatchMaxWaitMillis: 25,
	}
	environmentJSON, err := json.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"consumerMode":"batch"`, `"consumerBatchMaxMessages":100`,
		`"consumerBatchMaxBytes":4194304`, `"consumerBatchMaxWaitMillis":25`,
	} {
		if !strings.Contains(string(environmentJSON), fragment) {
			t.Fatalf("environment JSON does not contain %q: %s", fragment, environmentJSON)
		}
	}

	markdown := renderMarkdown(RunReport{
		SpecVersion: reportSpecVersion,
		RunID:       "spec-2.1",
		Stages:      []StageReport{stage},
		Environment: environment,
	})
	for _, fragment := range []string{
		"Relay msg/s", "Consumer msg/s", "Consumer MiB/s", "Outbox lag", "Consumer lag",
		"Batch calls", "Avg batch", "Max batch", "Batch handler p95", "| 20 | 99.50 | 100 | 8.50 ms |",
		"relay msg/s = published delta", "consumer msg/s = committed projection delta",
		"Outbox module: `" + testOutboxVersion + "`",
		"consumer mode `batch`", "batch max messages/bytes/wait `100` / `4194304` / `25.000 ms`",
		"Outbox handler", "Outbox publish", "Outbox finalization", "9.50 ms", "7.50 ms", "6.50 ms",
		"SQL calls `70`", "transactions/message `0.0150`", "WAL/message `321.61 B`", "checkpoints `1`",
		"success `1990`, retry `0`, defer `0`, DLQ `0`",
		"Outbox pool health: producer new/replaced/canceled/unusable/max-acquired `8/0/0/0/9`; relay `0/0/0/0/1`",
	} {
		if !strings.Contains(markdown, fragment) {
			t.Fatalf("Markdown does not contain %q:\n%s", fragment, markdown)
		}
	}
	for _, fragment := range []string{"Effective msg/s", "Effective MiB/s", "effective msg/s", "effective MiB/s"} {
		if strings.Contains(markdown, fragment) {
			t.Fatalf("Markdown still contains legacy metric %q:\n%s", fragment, markdown)
		}
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

func sampleWithBoundaries(elapsed float64, staged, published, committed, committedBytes int64) Sample {
	if published < 0 {
		panic("published count must not be negative")
	}

	return Sample{
		ElapsedSeconds: elapsed,
		Phase:          loadPhase,
		Business: BusinessSnapshot{
			Accepted: staged, Staged: staged, Published: published, Committed: committed,
			CommittedBytes: committedBytes,
		},
		Broker:      BrokerSnapshot{StreamMessages: uint64(published)},
		Application: demo.AppStats{Ready: true},
	}
}

func containsReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}
