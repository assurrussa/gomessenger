package capacity

import (
	"fmt"
	"math"
	"time"
)

const (
	minimumThroughputRatio = 0.99
	maximumLagSlopeRatio   = 0.01
	bytesPerMiB            = 1 << 20
)

type stageReportInput struct {
	config         Config
	stageID        string
	warmup         bool
	rate           int
	loadStartedAt  time.Time
	loadEndedAt    time.Time
	drainDuration  time.Duration
	drainCompleted bool
	initial        Sample
	loadEnd        Sample
	final          Sample
	samples        []Sample
	k6             K6Result
	latency        LatencyStats
	envelopes      EnvelopeStats
	integrity      IntegrityResult
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
		EffectiveMessagesPerSec: ratePerSecond(loadCounts.Committed, loadSeconds),
		EffectiveMiBPerSec:      float64(loadCounts.CommittedBytes) / bytesPerMiB / loadSeconds,
		AcceptedMessagesPerSec:  ratePerSecond(loadCounts.BusinessAccepted, loadSeconds),
		BacklogSlopePerSec:      backlogSlope(input.samples, loadSeconds),
		Latency:                 input.latency,
		Envelopes:               input.envelopes,
		K6:                      input.k6,
		Integrity:               input.integrity,
		InboxDuplicates: nonNegativeDelta(
			input.final.Application.InboxDuplicates,
			input.initial.Application.InboxDuplicates,
		),
		DLQMessages: safeUintDelta(input.final.Broker.DLQMessages, input.initial.Broker.DLQMessages),
	}
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
	report.Sustainable = report.Integrity.Passed && len(report.UnsustainableReasons) == 0
	return report
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
	reasons := make([]string, 0, 8)
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
	if report.EffectiveMessagesPerSec < minimumRate {
		reasons = append(reasons, fmt.Sprintf(
			"committed throughput %.2f msg/s is below %.2f msg/s", report.EffectiveMessagesPerSec, minimumRate,
		))
	}
	maximumSlope := float64(report.TargetRate) * maximumLagSlopeRatio
	if report.BacklogSlopePerSec > maximumSlope {
		reasons = append(reasons, fmt.Sprintf(
			"backlog slope %.2f msg/s exceeds %.2f msg/s", report.BacklogSlopePerSec, maximumSlope,
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
	return reasons
}

func backlogSlope(samples []Sample, loadSeconds float64) float64 {
	points := make([]Sample, 0, len(samples))
	for _, sample := range samples {
		if sample.Phase == "load" && sample.ElapsedSeconds >= loadSeconds/2 {
			points = append(points, sample)
		}
	}
	if len(points) < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumXX float64
	for _, point := range points {
		x := point.ElapsedSeconds
		y := float64(point.Business.Accepted - point.Business.Committed)
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
