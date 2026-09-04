package capacity

import (
	"fmt"
	"math"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

const (
	minimumThroughputRatio = 0.99
	maximumLagSlopeRatio   = 0.01
	bytesPerMiB            = 1 << 20
)

type stageReportInput struct {
	config             Config
	stageID            string
	warmup             bool
	rate               int
	loadStartedAt      time.Time
	loadEndedAt        time.Time
	drainDuration      time.Duration
	drainCompleted     bool
	initial            Sample
	loadEnd            Sample
	final              Sample
	samples            []Sample
	k6                 K6Result
	latency            LatencyStats
	envelopes          EnvelopeStats
	integrity          IntegrityResult
	postgres           pgtelemetry.Timeline
	loadSampleFailures int
}

func buildStageReport(input stageReportInput) StageReport {
	loadSeconds := input.config.StageDuration.Seconds()
	if input.warmup {
		loadSeconds = input.config.WarmupDuration.Seconds()
	}
	loadCounts := StageCounts{
		Offered:          input.k6.OfferedIterations,
		HTTPAccepted:     input.k6.AcceptedOrders,
		BusinessAccepted: nonNegativeDelta(input.loadEnd.Business.Accepted, input.initial.Business.Accepted),
		Staged:           nonNegativeDelta(input.loadEnd.Business.Staged, input.initial.Business.Staged),
		Published:        nonNegativeDelta(input.loadEnd.Business.Published, input.initial.Business.Published),
		// published_at is set only after the real JetStream PubAck, so it is
		// the exact timestamped broker boundary for the half-open load window.
		StreamPublished: nonNegativeDelta(input.loadEnd.Business.Published, input.initial.Business.Published),
		Committed:       nonNegativeDelta(input.loadEnd.Business.Committed, input.initial.Business.Committed),
		CommittedBytes:  nonNegativeDelta(input.loadEnd.Business.CommittedBytes, input.initial.Business.CommittedBytes),
	}
	afterDrain := StageCounts{
		Offered:          input.k6.OfferedIterations,
		HTTPAccepted:     input.k6.AcceptedOrders,
		BusinessAccepted: nonNegativeDelta(input.final.Business.Accepted, input.initial.Business.Accepted),
		Staged:           nonNegativeDelta(input.final.Business.Staged, input.initial.Business.Staged),
		Published:        nonNegativeDelta(input.final.Business.Published, input.initial.Business.Published),
		StreamPublished:  safeUintDelta(input.final.Broker.StreamMessages, input.initial.Broker.StreamMessages),
		Committed:        nonNegativeDelta(input.final.Business.Committed, input.initial.Business.Committed),
		CommittedBytes:   nonNegativeDelta(input.final.Business.CommittedBytes, input.initial.Business.CommittedBytes),
	}
	report := StageReport{
		StageID: input.stageID, Warmup: input.warmup, TargetRate: input.rate,
		LoadStartedAt: input.loadStartedAt, LoadEndedAt: input.loadEndedAt, LoadWindowSeconds: loadSeconds,
		DrainSeconds: input.drainDuration.Seconds(), DrainCompleted: input.drainCompleted,
		LoadWindow: loadCounts, AfterDrain: afterDrain,
		RelayMessagesPerSec:     ratePerSecond(loadCounts.Published, loadSeconds),
		ConsumerMessagesPerSec:  ratePerSecond(loadCounts.Committed, loadSeconds),
		ConsumerMiBPerSec:       float64(loadCounts.CommittedBytes) / bytesPerMiB / loadSeconds,
		AcceptedMessagesPerSec:  ratePerSecond(loadCounts.BusinessAccepted, loadSeconds),
		OutboxLag:               loadCounts.Staged - loadCounts.Published,
		ConsumerLag:             loadCounts.Published - loadCounts.Committed,
		OutboxLagGrowthPerSec:   outboxLagSlope(input.samples, loadSeconds),
		ConsumerLagGrowthPerSec: consumerLagSlope(input.samples, loadSeconds),
		BacklogSlopePerSec:      backlogSlope(input.samples, loadSeconds),
		Latency:                 input.latency,
		InboxHandle:             input.final.Application.Consumer.InboxHandle,
		BrokerAck:               input.final.Application.Consumer.BrokerAck,
		ConsumerBatch:           input.final.Application.Consumer.Batch,
		OutboxExecution:         input.final.Application.OutboxExecution,
		Envelopes:               input.envelopes,
		PostgreSQL:              input.postgres,
		OutboxDatabase: outboxDatabaseStats(
			input.initial.Application,
			input.final.Application,
		),
		K6:        input.k6,
		Integrity: input.integrity,
		InboxDuplicates: nonNegativeDelta(
			input.final.Application.InboxDuplicates,
			input.initial.Application.InboxDuplicates,
		),
		DLQMessages: safeUintDelta(input.final.Broker.DLQMessages, input.initial.Broker.DLQMessages),
	}
	report.PostgreSQLNormalized = normalizePostgreSQLCost(report.PostgreSQL.LoadDelta, loadCounts.Committed)
	for _, sample := range input.samples {
		backlog := sample.Business.Accepted - sample.Business.Committed
		if backlog > report.MaxBusinessBacklog {
			report.MaxBusinessBacklog = backlog
		}
		if sample.Application.Outbox.Total > report.MaxOutboxBacklog {
			report.MaxOutboxBacklog = sample.Application.Outbox.Total
		}
		if sample.Broker.ConsumerPending > report.MaxConsumerPending {
			report.MaxConsumerPending = sample.Broker.ConsumerPending
		}
		if sample.Broker.Redelivered > report.MaxBrokerRedelivered {
			report.MaxBrokerRedelivered = sample.Broker.Redelivered
		}
	}
	report.Integrity = evaluateIntegrity(report.Integrity, report.AfterDrain, report.DLQMessages)
	report.UnsustainableReasons = sustainabilityReasons(input.config, report)
	if input.loadSampleFailures != 0 {
		report.UnsustainableReasons = append(report.UnsustainableReasons, fmt.Sprintf(
			"application telemetry was unavailable for %d load samples", input.loadSampleFailures,
		))
	}
	report.Sustainable = report.Integrity.Passed && len(report.UnsustainableReasons) == 0
	return report
}

func outboxDatabaseStats(initial, final demo.AppStats) OutboxDatabaseStats {
	return OutboxDatabaseStats{
		Producer: pgxPoolStageStats(initial.ProducerDB, final.ProducerDB),
		Relay:    pgxPoolStageStats(initial.RelayDB, final.RelayDB),
	}
}

func pgxPoolStageStats(initial, final demo.PGXPoolStats) PGXPoolStageStats {
	newConnections := nonNegativeDelta(final.NewConnectionsCount, initial.NewConnectionsCount)
	normalExpansion := int64(final.MaxAcquiredConnections - initial.TotalConnections)
	if normalExpansion < 0 {
		normalExpansion = 0
	}
	if maximum := int64(final.MaxConnections - initial.TotalConnections); normalExpansion > maximum && maximum >= 0 {
		normalExpansion = maximum
	}
	replacements := newConnections - normalExpansion
	if replacements < 0 {
		replacements = 0
	}
	return PGXPoolStageStats{
		MaxConnections:         final.MaxConnections,
		MaxAcquiredConnections: final.MaxAcquiredConnections,
		NewConnections:         newConnections,
		ReplacementConnections: replacements,
		CanceledAcquires:       nonNegativeDelta(final.CanceledAcquireCount, initial.CanceledAcquireCount),
		UnusableReleases:       nonNegativeDelta(final.UnusableReleaseCount, initial.UnusableReleaseCount),
	}
}

func normalizePostgreSQLCost(delta pgtelemetry.Delta, committed int64) PostgreSQLNormalizedStats {
	var calls int64
	for _, statement := range delta.Statements {
		if statement.Classification != "probe" {
			calls += statement.Calls
		}
	}
	transactions := int64(delta.Database["xact_commit"] + delta.Database["xact_rollback"])
	walBytes := delta.WAL["wal_bytes"]
	checkpoints := int64(delta.Checkpointer["num_timed"] + delta.Checkpointer["num_requested"])
	result := PostgreSQLNormalizedStats{
		SQLCalls: calls, Transactions: transactions, WALBytes: walBytes,
		CompletedCheckpoints: checkpoints,
	}
	if committed > 0 {
		result.TransactionsPerMessage = float64(transactions) / float64(committed)
		result.WALBytesPerMessage = walBytes / float64(committed)
	}
	return result
}

func evaluateIntegrity(result IntegrityResult, counts StageCounts, dlqMessages int64) IntegrityResult {
	expected := result.Orders
	checks := []struct {
		ok     bool
		reason string
	}{
		{counts.HTTPAccepted == expected, "HTTP-accepted orders differ from committed business orders"},
		{counts.BusinessAccepted == expected, "post-drain business order count is inconsistent"},
		{result.DistinctOrderMessages == expected, "orders do not have one unique message identity each"},
		{result.Measurements == expected, "staged envelope count differs from accepted business orders"},
		{result.BrokerConfirmed == expected, "broker-confirmed envelope count differs from accepted business orders"},
		{result.Projections == expected, "committed projection count differs from accepted business orders"},
		{result.DistinctProjectionMsgs == expected, "projection message identities are not unique"},
		{result.InvalidMeasurements == 0, "invalid envelope size or SHA-256 measurement detected"},
		{result.MissingOrderLinks == 0, "an envelope measurement has no business order"},
		{result.MissingProjectionLinks == 0, "an envelope measurement has no committed projection"},
		{counts.StreamPublished == expected, "JetStream message delta differs from accepted business orders"},
		{dlqMessages == 0, "accepted capacity traffic reached the DLQ"},
	}
	for _, check := range checks {
		if !check.ok {
			result.Reasons = append(result.Reasons, check.reason)
		}
	}
	result.Passed = len(result.Reasons) == 0
	return result
}

func sustainabilityReasons(config Config, report StageReport) []string {
	reasons := make([]string, 0, 10)
	if report.K6.ExitCode != 0 {
		reasons = append(reasons, fmt.Sprintf("k6 exited with code %d", report.K6.ExitCode))
	}
	if report.K6.DroppedIterations != 0 {
		reasons = append(reasons, fmt.Sprintf("k6 dropped %d scheduled iterations", report.K6.DroppedIterations))
	}
	if report.K6.HTTPFailureRate != 0 {
		reasons = append(reasons, fmt.Sprintf("HTTP failure rate is %.4f", report.K6.HTTPFailureRate))
	}
	if report.K6.AcceptedRate < 1 {
		reasons = append(reasons, fmt.Sprintf("HTTP 202 acceptance rate is %.4f", report.K6.AcceptedRate))
	}
	minimumRate := float64(report.TargetRate) * minimumThroughputRatio
	if report.AcceptedMessagesPerSec < minimumRate {
		reasons = append(reasons, fmt.Sprintf(
			"business acceptance %.2f msg/s is below %.2f msg/s", report.AcceptedMessagesPerSec, minimumRate,
		))
	}
	if report.RelayMessagesPerSec < minimumRate {
		reasons = append(reasons, fmt.Sprintf(
			"relay throughput %.2f msg/s is below %.2f msg/s", report.RelayMessagesPerSec, minimumRate,
		))
	}
	if report.ConsumerMessagesPerSec < minimumRate {
		reasons = append(reasons, fmt.Sprintf(
			"consumer throughput %.2f msg/s is below %.2f msg/s", report.ConsumerMessagesPerSec, minimumRate,
		))
	}
	maximumSlope := float64(report.TargetRate) * maximumLagSlopeRatio
	if report.OutboxLagGrowthPerSec > maximumSlope {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox lag growth %.2f msg/s exceeds %.2f msg/s", report.OutboxLagGrowthPerSec, maximumSlope,
		))
	}
	if report.ConsumerLagGrowthPerSec > maximumSlope {
		reasons = append(reasons, fmt.Sprintf(
			"consumer lag growth %.2f msg/s exceeds %.2f msg/s", report.ConsumerLagGrowthPerSec, maximumSlope,
		))
	}
	if report.BacklogSlopePerSec > maximumSlope {
		reasons = append(reasons, fmt.Sprintf(
			"end-to-end backlog growth %.2f msg/s exceeds %.2f msg/s", report.BacklogSlopePerSec, maximumSlope,
		))
	}
	if report.Latency.P95Millis > float64(config.E2EP95SLO.Milliseconds()) {
		reasons = append(reasons, fmt.Sprintf(
			"business p95 %.2fms exceeds %.2fms",
			report.Latency.P95Millis, float64(config.E2EP95SLO.Milliseconds()),
		))
	}
	if !report.DrainCompleted {
		reasons = append(reasons, "pipeline did not drain within the configured timeout")
	}
	if report.MaxBrokerRedelivered != 0 || report.InboxDuplicates != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"unexpected redelivery observed (broker max=%d inbox duplicates=%d)",
			report.MaxBrokerRedelivered, report.InboxDuplicates,
		))
	}
	if report.DLQMessages != 0 {
		reasons = append(reasons, fmt.Sprintf("DLQ received %d messages", report.DLQMessages))
	}
	if report.InboxHandle.Errors != 0 || report.BrokerAck.Errors != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"consumer observations contain errors (handle=%d broker_ack=%d)",
			report.InboxHandle.Errors, report.BrokerAck.Errors,
		))
	}
	if report.OutboxExecution.Handler.Handler.Errors != 0 ||
		report.OutboxExecution.Publish.Handler.Errors != 0 ||
		report.OutboxExecution.Finalization.Handler.Errors != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox observations contain errors (handler=%d publish=%d finalization=%d)",
			report.OutboxExecution.Handler.Handler.Errors,
			report.OutboxExecution.Publish.Handler.Errors,
			report.OutboxExecution.Finalization.Handler.Errors,
		))
	}
	if report.OutboxExecution.Outcomes.Retry != 0 ||
		report.OutboxExecution.Outcomes.Defer != 0 ||
		report.OutboxExecution.Outcomes.DLQ != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"unexpected Outbox outcomes observed (retry=%d defer=%d dlq=%d)",
			report.OutboxExecution.Outcomes.Retry,
			report.OutboxExecution.Outcomes.Defer,
			report.OutboxExecution.Outcomes.DLQ,
		))
	}
	reasons = appendPoolSustainabilityReasons(reasons, "producer", report.OutboxDatabase.Producer)
	reasons = appendPoolSustainabilityReasons(reasons, "relay", report.OutboxDatabase.Relay)
	return reasons
}

func appendPoolSustainabilityReasons(
	reasons []string,
	role string,
	stats PGXPoolStageStats,
) []string {
	if stats.ReplacementConnections != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox %s pool replaced %d connections", role, stats.ReplacementConnections,
		))
	}
	if stats.CanceledAcquires != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox %s pool canceled %d acquires", role, stats.CanceledAcquires,
		))
	}
	if stats.UnusableReleases != 0 {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox %s pool released %d unusable connections", role, stats.UnusableReleases,
		))
	}
	if stats.MaxAcquiredConnections > stats.MaxConnections {
		reasons = append(reasons, fmt.Sprintf(
			"Outbox %s pool acquired high-water %d exceeds max %d",
			role, stats.MaxAcquiredConnections, stats.MaxConnections,
		))
	}
	return reasons
}

func backlogSlope(samples []Sample, loadSeconds float64) float64 {
	return boundaryLagSlope(samples, loadSeconds, func(snapshot BusinessSnapshot) int64 {
		return snapshot.Accepted - snapshot.Committed
	})
}

func outboxLagSlope(samples []Sample, loadSeconds float64) float64 {
	return boundaryLagSlope(samples, loadSeconds, func(snapshot BusinessSnapshot) int64 {
		return snapshot.Staged - snapshot.Published
	})
}

func consumerLagSlope(samples []Sample, loadSeconds float64) float64 {
	return boundaryLagSlope(samples, loadSeconds, func(snapshot BusinessSnapshot) int64 {
		return snapshot.Published - snapshot.Committed
	})
}

func boundaryLagSlope(
	samples []Sample,
	loadSeconds float64,
	lag func(BusinessSnapshot) int64,
) float64 {
	points := make([]Sample, 0, len(samples))
	for _, sample := range samples {
		if sample.Phase == "load" &&
			sample.ElapsedSeconds >= loadSeconds/2 && sample.ElapsedSeconds <= loadSeconds {
			points = append(points, sample)
		}
	}
	if len(points) < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumXX float64
	for _, point := range points {
		x := point.ElapsedSeconds
		y := float64(lag(point.Business))
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	n := float64(len(points))
	denominator := n*sumXX - sumX*sumX
	if math.Abs(denominator) < 1e-9 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denominator
}

func ratePerSecond(count int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(count) / seconds
}

func nonNegativeDelta(end, start int64) int64 {
	if end <= start {
		return 0
	}
	return end - start
}

func nonNegativeUintDelta(end, start uint64) uint64 {
	if end <= start {
		return 0
	}
	return end - start
}

func safeUintDelta(end, start uint64) int64 {
	delta := nonNegativeUintDelta(end, start)
	if delta > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(delta)
}
