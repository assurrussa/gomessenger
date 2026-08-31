// Package batchproof validates matched capacity evidence for true Outbox batches.
package batchproof

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	manifestSpecVersion            = "2.1-batch-proof-manifest-1"
	proofSpecVersion               = "2.1-batch-proof-1"
	frontierSpecVersion            = "2.1-frontier-1"
	reportSpecVersion              = "2.1"
	minimumSamples                 = 30
	variantLegacy                  = "legacy"
	variantConsumer                = "consumer-batch"
	variantRelay                   = "relay-batch"
	variantFull                    = "full-batch"
	modeSingle                     = "single"
	modeBatch                      = "batch"
	unknownValue                   = "unknown"
	evidenceScopeCheckoutWorkspace = "checkout-workspace"
	postgresProfileStock           = "stock"
	resourcePostgres               = "postgres"
	resourceNATS                   = "nats"
	resourceAPI                    = "capacity-api"
)

var (
	requiredPayloads = []string{"small", "mixed"}
	requiredVariants = []string{variantLegacy, variantConsumer, variantRelay, variantFull}
)

// Manifest identifies every raw artifact and the fail-closed proof thresholds.
type Manifest struct {
	SpecVersion              string         `json:"specVersion"`
	ProofID                  string         `json:"proofId"`
	EvidenceScope            string         `json:"evidenceScope"`
	GitCommit                string         `json:"gitCommit"`
	OutboxGitCommit          string         `json:"outboxGitCommit"`
	Topology                 string         `json:"topology"`
	PostgresProfile          string         `json:"postgresProfile"`
	WarmupSeconds            float64        `json:"warmupSeconds"`
	MeasuredSeconds          float64        `json:"measuredSeconds"`
	DrainTimeoutSeconds      float64        `json:"drainTimeoutSeconds"`
	Confirmations            int            `json:"confirmations"`
	FrontierStep             int            `json:"frontierStep"`
	AdvantageThreshold       float64        `json:"advantageThreshold"`
	MaximumP95Ratio          float64        `json:"maximumP95Ratio"`
	MinimumAverageBatch      float64        `json:"minimumAverageBatch"`
	MaximumMemoryFraction    float64        `json:"maximumMemoryFraction"`
	MaximumMemoryGrowthRatio float64        `json:"maximumMemoryGrowthRatio"`
	MaximumMemoryGrowthBytes float64        `json:"maximumMemoryGrowthBytes"`
	Frontiers                []FrontierRef  `json:"frontiers"`
	CommonRuns               []CommonRunRef `json:"commonRuns"`
}

// FrontierRef points to one retained frontier search summary.
type FrontierRef struct {
	PayloadProfile string `json:"payloadProfile"`
	Variant        string `json:"variant"`
	Path           string `json:"path"`
}

// CommonRunRef points to one matched-rate report and its resource timelines.
type CommonRunRef struct {
	PayloadProfile string `json:"payloadProfile"`
	Variant        string `json:"variant"`
	Repetition     int    `json:"repetition"`
	Rate           int    `json:"rate"`
	RunID          string `json:"runId"`
	CommandStatus  int    `json:"commandStatus"`
	Report         string `json:"report"`
	Resources      string `json:"resources"`
	Samples        string `json:"samples"`
}

type frontierSummary struct {
	SpecVersion              string        `json:"specVersion"`
	FrontierID               string        `json:"frontierId"`
	Variant                  string        `json:"variant"`
	Topology                 string        `json:"topology"`
	PayloadProfile           string        `json:"payloadProfile"`
	PostgresProfile          string        `json:"postgresProfile"`
	FrontierRate             int           `json:"frontierRate"`
	OutboxBatchMaxMessages   int           `json:"outboxBatchMaxMessages"`
	ConsumerBatchMaxMessages int           `json:"consumerBatchMaxMessages"`
	Runs                     []frontierRun `json:"runs"`
}

type frontierRun struct {
	RunID             string `json:"runId"`
	Phase             string `json:"phase"`
	State             string `json:"state"`
	Rate              int    `json:"rate"`
	OutboxBatchSize   int    `json:"outboxBatchSize"`
	ConsumerBatchSize int    `json:"consumerBatchSize"`
	CommandStatus     int    `json:"commandStatus"`
	Report            string `json:"report"`
}

type runReport struct {
	SpecVersion     string        `json:"specVersion"`
	RunID           string        `json:"runId"`
	Config          reportConfig  `json:"config"`
	Environment     environment   `json:"environment"`
	Stages          []stageReport `json:"stages"`
	IntegrityPassed bool          `json:"integrityPassed"`
	Failure         string        `json:"failure"`
}

type reportConfig struct {
	WarmupSeconds       float64 `json:"warmupSeconds"`
	StageSeconds        float64 `json:"stageSeconds"`
	DrainTimeoutSeconds float64 `json:"drainTimeoutSeconds"`
	PayloadProfile      string  `json:"payloadProfile"`
	PostgreSQLProfile   string  `json:"postgresqlProfile"`
}

type environment struct {
	GoVersion                  string            `json:"goVersion"`
	OutboxVersion              string            `json:"outboxVersion"`
	ContainerOS                string            `json:"containerOs"`
	ContainerArch              string            `json:"containerArch"`
	ContainerCPUs              int               `json:"containerLogicalCpus"`
	HostOS                     string            `json:"hostOs"`
	HostArch                   string            `json:"hostArch"`
	HostCPUs                   string            `json:"hostLogicalCpus"`
	GitCommit                  string            `json:"gitCommit"`
	GitDirty                   string            `json:"gitDirty"`
	OutboxGitCommit            string            `json:"outboxGitCommit"`
	OutboxGitDirty             string            `json:"outboxGitDirty"`
	PostgreSQLVersion          string            `json:"postgresqlVersion"`
	PostgreSQLProfile          string            `json:"postgresqlProfile"`
	PostgreSQLImage            string            `json:"postgresqlImage"`
	PostgreSQLImageDigest      string            `json:"postgresqlImageDigest"`
	NATSServerVersion          string            `json:"natsServerVersion"`
	NATSImage                  string            `json:"natsImage"`
	NATSImageDigest            string            `json:"natsImageDigest"`
	K6Version                  string            `json:"k6Version"`
	OutboxWorkers              int               `json:"outboxWorkers"`
	OutboxReservationBatchSize int               `json:"outboxReservationBatchSize"`
	OutboxProducerMaxConns     int               `json:"outboxProducerMaxConnections"`
	OutboxRelayMaxConns        int               `json:"outboxRelayMaxConnections"`
	OutboxIngressMode          string            `json:"outboxIngressMode"`
	OutboxRelayMode            string            `json:"outboxRelayMode"`
	OutboxBatchMaxMessages     int               `json:"outboxBatchMaxMessages"`
	ConsumerConcurrency        int               `json:"consumerConcurrency"`
	ConsumerMode               string            `json:"consumerMode"`
	ConsumerBatchMaxMessages   int               `json:"consumerBatchMaxMessages"`
	DBMaxOpenConns             int               `json:"dbMaxOpenConnections"`
	PostgreSQLSettings         map[string]string `json:"postgresqlSettings"`
	SUTCPUSet                  string            `json:"sutCpuSet"`
	PostgreSQLMemoryBytes      int64             `json:"postgresqlMemoryBytes"`
	NATSMemoryBytes            int64             `json:"natsMemoryBytes"`
	APIMemoryBytes             int64             `json:"apiMemoryBytes"`
	SwapDisabled               bool              `json:"swapDisabled"`
}

type stageReport struct {
	StageID                 string                    `json:"stageId"`
	TargetRate              int                       `json:"targetRate"`
	LoadStartedAt           time.Time                 `json:"loadStartedAt"`
	LoadEndedAt             time.Time                 `json:"loadEndedAt"`
	LoadWindowSeconds       float64                   `json:"loadWindowSeconds"`
	DrainSeconds            float64                   `json:"drainSeconds"`
	DrainCompleted          bool                      `json:"drainCompleted"`
	LoadWindow              stageCounts               `json:"loadWindow"`
	AfterDrain              stageCounts               `json:"afterDrain"`
	RelayMessagesPerSec     float64                   `json:"relayMessagesPerSecond"`
	ConsumerMessagesPerSec  float64                   `json:"consumerMessagesPerSecond"`
	AcceptedMessagesPerSec  float64                   `json:"acceptedMessagesPerSecond"`
	OutboxLagGrowthPerSec   float64                   `json:"outboxLagGrowthPerSecond"`
	ConsumerLagGrowthPerSec float64                   `json:"consumerLagGrowthPerSecond"`
	BacklogSlopePerSec      float64                   `json:"backlogSlopePerSecond"`
	MaxBrokerRedelivered    int                       `json:"maxBrokerRedelivered"`
	InboxDuplicates         int64                     `json:"inboxDuplicates"`
	DLQMessages             int64                     `json:"dlqMessages"`
	Latency                 latencyStats              `json:"latency"`
	InboxHandle             operationStats            `json:"inboxHandle"`
	BrokerAck               operationStats            `json:"brokerAck"`
	ConsumerBatch           batchHandlerStats         `json:"consumerBatch"`
	OutboxExecution         outboxExecutionStats      `json:"outboxExecution"`
	PostgreSQL              postgreSQLTimeline        `json:"postgresql"`
	PostgreSQLNormalized    postgreSQLNormalizedStats `json:"postgresqlNormalized"`
	OutboxDatabase          outboxDatabaseStats       `json:"outboxDatabase"`
	K6                      k6Result                  `json:"k6"`
	Sustainable             bool                      `json:"sustainable"`
	UnsustainableReasons    []string                  `json:"unsustainableReasons"`
	Integrity               integrityResult           `json:"integrity"`
}

type stageCounts struct {
	Offered          int64 `json:"offered"`
	HTTPAccepted     int64 `json:"httpAccepted"`
	BusinessAccepted int64 `json:"businessAccepted"`
	Staged           int64 `json:"staged"`
	Published        int64 `json:"published"`
	StreamPublished  int64 `json:"streamPublished"`
	Committed        int64 `json:"committed"`
}

type latencyStats struct {
	P95Millis float64 `json:"p95Millis"`
}

type operationStats struct {
	Errors int64 `json:"errors"`
}

type k6Result struct {
	ExitCode          int     `json:"exitCode"`
	DroppedIterations int64   `json:"droppedIterations"`
	HTTPFailureRate   float64 `json:"httpFailureRate"`
	AcceptedRate      float64 `json:"acceptedRate"`
}

type batchHandlerStats struct {
	Invocations     int64          `json:"invocations"`
	Messages        int64          `json:"messages"`
	AverageMessages float64        `json:"averageMessages"`
	MaxMessages     int            `json:"maxMessages"`
	Handler         operationStats `json:"handler"`
}

type batchOutcomeStats struct {
	Success int64 `json:"success"`
	Retry   int64 `json:"retry"`
	Defer   int64 `json:"defer"`
	DLQ     int64 `json:"dlq"`
}

type outboxExecutionStats struct {
	Handler      batchHandlerStats `json:"handler"`
	Publish      batchHandlerStats `json:"publish"`
	Finalization batchHandlerStats `json:"finalization"`
	Outcomes     batchOutcomeStats `json:"outcomes"`
}

type postgreSQLNormalizedStats struct {
	SQLCalls               int64   `json:"sqlCalls"`
	Transactions           int64   `json:"transactions"`
	TransactionsPerMessage float64 `json:"transactionsPerMessage"`
	WALBytes               float64 `json:"walBytes"`
	WALBytesPerMessage     float64 `json:"walBytesPerMessage"`
}

type pgxPoolStageStats struct {
	MaxConnections         int32 `json:"maxConnections"`
	MaxAcquiredConnections int32 `json:"maxAcquiredConnections"`
	NewConnections         int64 `json:"newConnections"`
	ReplacementConnections int64 `json:"replacementConnections"`
	CanceledAcquires       int64 `json:"canceledAcquires"`
	UnusableReleases       int64 `json:"unusableReleases"`
}

type outboxDatabaseStats struct {
	Producer pgxPoolStageStats `json:"producer"`
	Relay    pgxPoolStageStats `json:"relay"`
}

type postgreSQLTimeline struct {
	LoadDelta postgreSQLDelta `json:"loadDelta"`
}

type postgreSQLDelta struct {
	Statements []statement `json:"statements"`
}

type statement struct {
	Query               string  `json:"query"`
	Classification      string  `json:"classification"`
	Calls               int64   `json:"calls"`
	TotalExecTimeMillis float64 `json:"totalExecTimeMillis"`
	Rows                int64   `json:"rows"`
}

type integrityResult struct {
	Passed  bool     `json:"passed"`
	Reasons []string `json:"reasons"`
}

type waitEvent struct {
	WaitEvent string `json:"waitEvent"`
	Sessions  int64  `json:"sessions"`
}

// Proof is the machine-readable decision produced from a complete manifest.
type Proof struct {
	SpecVersion   string           `json:"specVersion"`
	ProofID       string           `json:"proofId"`
	EvidenceScope string           `json:"evidenceScope"`
	Passed        bool             `json:"passed"`
	Reasons       []string         `json:"reasons,omitempty"`
	Provenance    *Provenance      `json:"provenance,omitempty"`
	Payloads      []PayloadVerdict `json:"payloads"`
}

// Provenance is the common immutable execution identity across every report.
type Provenance struct {
	GitCommit             string `json:"gitCommit"`
	OutboxGitCommit       string `json:"outboxGitCommit"`
	GoVersion             string `json:"goVersion"`
	OutboxVersion         string `json:"outboxVersion"`
	PostgreSQLVersion     string `json:"postgresqlVersion"`
	PostgreSQLImageDigest string `json:"postgresqlImageDigest"`
	NATSServerVersion     string `json:"natsServerVersion"`
	NATSImageDigest       string `json:"natsImageDigest"`
	HostOS                string `json:"hostOs"`
	HostArch              string `json:"hostArch"`
	HostCPUs              string `json:"hostLogicalCpus"`
}

// PayloadVerdict contains the frontier and matched-rate result for one payload.
type PayloadVerdict struct {
	PayloadProfile string                    `json:"payloadProfile"`
	CommonRate     int                       `json:"commonRate"`
	Frontiers      map[string]int            `json:"frontiers"`
	Common         map[string]VariantMetrics `json:"common"`
	IsolatedRelay  Comparison                `json:"isolatedRelay"`
	EndToEnd       Comparison                `json:"endToEnd"`
}

// Comparison records one required control/candidate capacity ratio.
type Comparison struct {
	Control       string  `json:"control"`
	Candidate     string  `json:"candidate"`
	ControlRate   int     `json:"controlRate"`
	CandidateRate int     `json:"candidateRate"`
	Ratio         float64 `json:"ratio"`
	Passed        bool    `json:"passed"`
}

// VariantMetrics contains medians from three matched common-rate runs.
type VariantMetrics struct {
	Runs                       int     `json:"runs"`
	P95Millis                  float64 `json:"p95Millis"`
	SQLCallsPerMessage         float64 `json:"sqlCallsPerMessage"`
	TransactionsPerMessage     float64 `json:"transactionsPerMessage"`
	WALBytesPerMessage         float64 `json:"walBytesPerMessage"`
	ClaimCallsPerMessage       float64 `json:"claimCallsPerMessage"`
	ClaimExecMillisPerMessage  float64 `json:"claimExecMillisPerMessage"`
	ClaimedRowsPerCall         float64 `json:"claimedRowsPerCall"`
	WALWaitSamples             float64 `json:"walWaitSamples"`
	MaximumConsecutiveWALWaits int     `json:"maximumConsecutiveWalWaits"`
	OutboxHandlerAverageBatch  float64 `json:"outboxHandlerAverageBatch"`
	OutboxPublishAverageBatch  float64 `json:"outboxPublishAverageBatch"`
	OutboxFinalizeAverageBatch float64 `json:"outboxFinalizeAverageBatch"`
	ConsumerAverageBatch       float64 `json:"consumerAverageBatch"`
	PeakMemoryFraction         float64 `json:"peakMemoryFraction"`
	MaximumMemoryGrowthBytes   float64 `json:"maximumMemoryGrowthBytes"`
}

type runMetrics struct {
	p95Millis                  float64
	sqlCallsPerMessage         float64
	transactionsPerMessage     float64
	walBytesPerMessage         float64
	claimCallsPerMessage       float64
	claimExecMillisPerMessage  float64
	claimedRowsPerCall         float64
	walWaitSamples             int
	maximumConsecutiveWALWaits int
	outboxHandlerAverageBatch  float64
	outboxPublishAverageBatch  float64
	outboxFinalizeAverageBatch float64
	consumerAverageBatch       float64
	peakMemoryFraction         float64
	maximumMemoryGrowthBytes   float64
}

type evaluator struct {
	manifest   Manifest
	proof      Proof
	baseline   *provenanceKey
	frontiers  map[string]int
	commonRuns map[string][]runMetrics
	runIDs     map[string]string
}

type provenanceKey struct {
	Provenance
	ContainerOS                string
	ContainerArch              string
	ContainerCPUs              int
	PostgreSQLProfile          string
	PostgreSQLImage            string
	NATSImage                  string
	K6Version                  string
	OutboxWorkers              int
	OutboxReservationBatchSize int
	OutboxProducerMaxConns     int
	OutboxRelayMaxConns        int
	ConsumerConcurrency        int
	DBMaxOpenConns             int
	SUTCPUSet                  string
	PostgreSQLMemoryBytes      int64
	NATSMemoryBytes            int64
	APIMemoryBytes             int64
	SwapDisabled               bool
	PostgreSQLSettings         map[string]string
}

// EvaluateDir validates dir/manifest.json and returns a complete fail-closed proof.
func EvaluateDir(dir string) (Proof, error) {
	manifestPath := filepath.Join(dir, "manifest.json")
	var manifest Manifest
	if err := readJSON(manifestPath, &manifest); err != nil {
		return Proof{}, fmt.Errorf("read batch proof manifest: %w", err)
	}
	e := &evaluator{
		manifest: manifest,
		proof: Proof{
			SpecVersion:   proofSpecVersion,
			ProofID:       manifest.ProofID,
			EvidenceScope: manifest.EvidenceScope,
		},
		frontiers:  make(map[string]int),
		commonRuns: make(map[string][]runMetrics),
		runIDs:     make(map[string]string),
	}
	e.validateManifest()
	e.evaluateFrontiers()
	e.evaluateCommonRuns()
	e.buildPayloadVerdicts()
	e.proof.Passed = len(e.proof.Reasons) == 0
	return e.proof, nil
}

// Write persists proof.json and proof.md even when the evidence fails.
func Write(dir string, proof Proof) error {
	encoded, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return fmt.Errorf("encode proof: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(filepath.Join(dir, "proof.json"), encoded, 0o600); err != nil {
		return fmt.Errorf("write proof JSON: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "proof.md"), []byte(renderMarkdown(proof)), 0o600); err != nil {
		return fmt.Errorf("write proof Markdown: %w", err)
	}
	return nil
}

func (e *evaluator) validateManifest() {
	m := e.manifest
	checks := []struct {
		ok     bool
		reason string
	}{
		{m.SpecVersion == manifestSpecVersion, "manifest spec version must be " + manifestSpecVersion},
		{m.ProofID != "", "proof ID is empty"},
		{m.EvidenceScope == evidenceScopeCheckoutWorkspace, "evidence scope must be " + evidenceScopeCheckoutWorkspace},
		{m.GitCommit != "" && m.GitCommit != unknownValue, "GoMessenger commit is missing"},
		{m.OutboxGitCommit != "" && m.OutboxGitCommit != unknownValue, "Outbox commit is missing"},
		{m.Topology == "o2-c2", "proof topology must be o2-c2"},
		{m.PostgresProfile == postgresProfileStock, "PostgreSQL profile must be stock"},
		{m.WarmupSeconds == 60, "warm-up must be 60 seconds"},
		{m.MeasuredSeconds == 120, "measured stage must be 120 seconds"},
		{m.DrainTimeoutSeconds == 60, "drain timeout must be 60 seconds"},
		{m.Confirmations == 3, "three frontier confirmations are required"},
		{m.FrontierStep == 50, "frontier step must be 50 msg/s"},
		{m.AdvantageThreshold >= 1.3, "advantage threshold must be at least 1.3"},
		{m.MaximumP95Ratio > 0 && m.MaximumP95Ratio <= 1.1, "maximum p95 ratio must be at most 1.1"},
		{m.MinimumAverageBatch >= 10, "minimum average batch must be at least 10"},
		{m.MaximumMemoryFraction > 0 && m.MaximumMemoryFraction <= 0.8, "maximum memory fraction must be at most 0.8"},
		{m.MaximumMemoryGrowthRatio >= 0 && m.MaximumMemoryGrowthRatio <= 0.1, "maximum memory growth ratio must be at most 0.1"},
		{m.MaximumMemoryGrowthBytes > 0 && m.MaximumMemoryGrowthBytes <= 32<<20, "maximum memory growth must be at most 32 MiB"},
	}
	for _, check := range checks {
		if !check.ok {
			e.addReason(check.reason)
		}
	}
	e.validateCommonOrder()
}

//nolint:gocognit // This is the bounded manifest-to-confirmation validation pass.
func (e *evaluator) evaluateFrontiers() {
	seen := make(map[string]struct{})
	for _, ref := range e.manifest.Frontiers {
		key := proofKey(ref.PayloadProfile, ref.Variant)
		if !slices.Contains(requiredPayloads, ref.PayloadProfile) || !slices.Contains(requiredVariants, ref.Variant) {
			e.addReason("unexpected frontier reference " + key)
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			e.addReason("duplicate frontier reference for " + key)
			continue
		}
		seen[key] = struct{}{}
		var summary frontierSummary
		if err := readJSON(ref.Path, &summary); err != nil {
			e.addReason(fmt.Sprintf("read frontier %s: %v", key, err))
			continue
		}
		e.validateFrontierSummary(key, ref, summary)
		e.frontiers[key] = summary.FrontierRate

		confirmations := make([]frontierRun, 0, e.manifest.Confirmations)
		for _, run := range summary.Runs {
			if strings.HasPrefix(run.Phase, "confirm-") && run.Rate == summary.FrontierRate && run.State == "pass" {
				confirmations = append(confirmations, run)
			}
		}
		if len(confirmations) != e.manifest.Confirmations {
			e.addReason(fmt.Sprintf(
				"frontier %s has %d passing confirmations, want %d",
				key,
				len(confirmations),
				e.manifest.Confirmations,
			))
		}
		for _, confirmation := range confirmations {
			e.recordRunID(confirmation.RunID, confirmation.Report)
			if confirmation.CommandStatus != 0 {
				e.addReason(fmt.Sprintf("frontier confirmation %s exited with %d", confirmation.RunID, confirmation.CommandStatus))
			}
			if confirmation.OutboxBatchSize != 100 || confirmation.ConsumerBatchSize != 100 {
				e.addReason(fmt.Sprintf(
					"frontier confirmation %s did not use 100/100 batch maximums",
					confirmation.RunID,
				))
			}
			report, err := e.loadAndValidateReport(confirmation.Report, ref.PayloadProfile, ref.Variant, summary.FrontierRate)
			if err != nil {
				e.addReason(fmt.Sprintf("frontier confirmation %s: %v", confirmation.RunID, err))
				continue
			}
			if report.RunID != confirmation.RunID {
				e.addReason(fmt.Sprintf("frontier confirmation %s report run ID is %s", confirmation.RunID, report.RunID))
			}
			e.validateActualBatch(ref.PayloadProfile, ref.Variant, confirmation.RunID, report.Stages[0])
		}
	}
	for _, payload := range requiredPayloads {
		for _, variant := range requiredVariants {
			key := proofKey(payload, variant)
			if _, ok := seen[key]; !ok {
				e.addReason("missing frontier reference for " + key)
			}
		}
	}
}

func (e *evaluator) validateFrontierSummary(key string, ref FrontierRef, summary frontierSummary) {
	alignedRate := e.manifest.FrontierStep > 0 &&
		summary.FrontierRate > 0 &&
		summary.FrontierRate%e.manifest.FrontierStep == 0
	checks := []struct {
		ok     bool
		reason string
	}{
		{summary.SpecVersion == frontierSpecVersion, "invalid frontier spec version"},
		{summary.PayloadProfile == ref.PayloadProfile, "payload does not match manifest"},
		{summary.Variant == ref.Variant, "variant does not match manifest"},
		{summary.Topology == e.manifest.Topology, "topology does not match manifest"},
		{summary.PostgresProfile == e.manifest.PostgresProfile, "PostgreSQL profile does not match manifest"},
		{alignedRate, "frontier rate is not a positive aligned step"},
		{summary.OutboxBatchMaxMessages == 100, "Outbox frontier batch maximum is not 100"},
		{summary.ConsumerBatchMaxMessages == 100, "consumer frontier batch maximum is not 100"},
	}
	for _, check := range checks {
		if !check.ok {
			e.addReason(fmt.Sprintf("frontier %s: %s", key, check.reason))
		}
	}
}

//nolint:gocognit // This is the bounded matched-series validation pass.
func (e *evaluator) evaluateCommonRuns() {
	seen := make(map[string]struct{})
	commonRate := 0
	for _, ref := range e.manifest.CommonRuns {
		key := fmt.Sprintf("%s/%s/%d", ref.PayloadProfile, ref.Variant, ref.Repetition)
		if !slices.Contains(requiredPayloads, ref.PayloadProfile) || !slices.Contains(requiredVariants, ref.Variant) {
			e.addReason("unexpected common run " + key)
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			e.addReason("duplicate common run " + key)
			continue
		}
		seen[key] = struct{}{}
		e.recordRunID(ref.RunID, ref.Report)
		if ref.CommandStatus != 0 {
			e.addReason(fmt.Sprintf("common run %s exited with %d", ref.RunID, ref.CommandStatus))
		}
		if ref.Repetition < 1 || ref.Repetition > e.manifest.Confirmations {
			e.addReason(fmt.Sprintf("common run %s has invalid repetition %d", ref.RunID, ref.Repetition))
		}
		if commonRate != 0 && commonRate != ref.Rate {
			e.addReason(fmt.Sprintf("common runs use mismatched rates %d and %d", commonRate, ref.Rate))
		} else if commonRate == 0 {
			commonRate = ref.Rate
		}

		report, err := e.loadAndValidateReport(ref.Report, ref.PayloadProfile, ref.Variant, ref.Rate)
		if err != nil {
			e.addReason(fmt.Sprintf("common run %s: %v", ref.RunID, err))
			continue
		}
		if report.RunID != ref.RunID {
			e.addReason(fmt.Sprintf("common run %s report run ID is %s", ref.RunID, report.RunID))
		}
		stage := report.Stages[0]
		e.validateActualBatch(ref.PayloadProfile, ref.Variant, ref.RunID, stage)
		metrics, metricErr := collectRunMetrics(e.manifest, ref, report)
		if metricErr != nil {
			e.addReason(fmt.Sprintf("common run %s: %v", ref.RunID, metricErr))
			continue
		}
		e.commonRuns[proofKey(ref.PayloadProfile, ref.Variant)] = append(
			e.commonRuns[proofKey(ref.PayloadProfile, ref.Variant)], metrics,
		)
	}

	frontierMinimum := 0
	for _, payload := range requiredPayloads {
		for _, variant := range requiredVariants {
			frontier := e.frontiers[proofKey(payload, variant)]
			if frontierMinimum == 0 || frontier < frontierMinimum {
				frontierMinimum = frontier
			}
			for repetition := 1; repetition <= e.manifest.Confirmations; repetition++ {
				key := fmt.Sprintf("%s/%s/%d", payload, variant, repetition)
				if _, ok := seen[key]; !ok {
					e.addReason("missing common run " + key)
				}
			}
		}
	}
	if e.manifest.FrontierStep > 0 {
		wantRate := (frontierMinimum * 80 / 100 / e.manifest.FrontierStep) * e.manifest.FrontierStep
		if commonRate != wantRate {
			e.addReason(fmt.Sprintf("common rate is %d, want %d", commonRate, wantRate))
		}
	}
}

func (e *evaluator) loadAndValidateReport(
	path string,
	payload string,
	variant string,
	rate int,
) (runReport, error) {
	var report runReport
	if err := readJSON(path, &report); err != nil {
		return report, err
	}
	if err := validateReportContract(e.manifest, report, payload, variant, rate); err != nil {
		return report, err
	}
	key := provenanceFromReport(report)
	if e.baseline == nil {
		e.baseline = &key
		e.proof.Provenance = &key.Provenance
	} else if !reflect.DeepEqual(*e.baseline, key) {
		return report, errors.New("provenance differs from the first accepted report")
	}
	return report, nil
}

func (e *evaluator) validateActualBatch(payload, variant, runID string, stage stageReport) {
	if variant == variantRelay || variant == variantFull {
		batches := []struct {
			name  string
			stats batchHandlerStats
		}{
			{"handler", stage.OutboxExecution.Handler},
			{"publish", stage.OutboxExecution.Publish},
			{"finalization", stage.OutboxExecution.Finalization},
		}
		for _, batch := range batches {
			if batch.stats.Invocations == 0 || batch.stats.Messages == 0 || batch.stats.MaxMessages < 2 {
				e.addReason(fmt.Sprintf(
					"%s %s/%s Outbox %s did not exercise a real batch",
					runID,
					payload,
					variant,
					batch.name,
				))
			}
			if batch.stats.AverageMessages < e.manifest.MinimumAverageBatch {
				e.addReason(fmt.Sprintf(
					"%s %s/%s Outbox %s average batch %.2f is below %.2f",
					runID,
					payload,
					variant,
					batch.name,
					batch.stats.AverageMessages,
					e.manifest.MinimumAverageBatch,
				))
			}
		}
	}
	if variant != variantLegacy &&
		(stage.ConsumerBatch.Invocations == 0 ||
			stage.ConsumerBatch.Messages == 0 ||
			stage.ConsumerBatch.MaxMessages < 2) {
		e.addReason(fmt.Sprintf(
			"%s %s/%s consumer did not exercise a real batch",
			runID,
			payload,
			variant,
		))
	}
	if variant != variantLegacy && stage.ConsumerBatch.AverageMessages < e.manifest.MinimumAverageBatch {
		e.addReason(fmt.Sprintf(
			"%s %s/%s consumer average batch %.2f is below %.2f",
			runID,
			payload,
			variant,
			stage.ConsumerBatch.AverageMessages,
			e.manifest.MinimumAverageBatch,
		))
	}
}

func (e *evaluator) buildPayloadVerdicts() {
	for _, payload := range requiredPayloads {
		verdict := PayloadVerdict{
			PayloadProfile: payload,
			Frontiers:      make(map[string]int, len(requiredVariants)),
			Common:         make(map[string]VariantMetrics, len(requiredVariants)),
		}
		for _, variant := range requiredVariants {
			key := proofKey(payload, variant)
			verdict.Frontiers[variant] = e.frontiers[key]
			metrics := e.commonRuns[key]
			if len(metrics) != e.manifest.Confirmations {
				e.addReason(fmt.Sprintf("common series %s has %d valid runs, want %d", key, len(metrics), e.manifest.Confirmations))
			}
			verdict.Common[variant] = aggregateMetrics(metrics)
		}
		verdict.CommonRate = commonRate(e.manifest.CommonRuns, payload)
		verdict.IsolatedRelay = e.compareFrontiers(payload, variantConsumer, variantRelay)
		verdict.EndToEnd = e.compareFrontiers(payload, variantLegacy, variantFull)
		e.validateMatchedGuardrails(payload, variantConsumer, variantRelay, verdict.Common)
		e.validateMatchedGuardrails(payload, variantLegacy, variantFull, verdict.Common)
		e.proof.Payloads = append(e.proof.Payloads, verdict)
	}
}

func (e *evaluator) compareFrontiers(payload, control, candidate string) Comparison {
	controlRate := e.frontiers[proofKey(payload, control)]
	candidateRate := e.frontiers[proofKey(payload, candidate)]
	ratio := 0.0
	if controlRate > 0 {
		ratio = float64(candidateRate) / float64(controlRate)
	}
	passed := ratio >= e.manifest.AdvantageThreshold
	if !passed {
		e.addReason(fmt.Sprintf(
			"%s %s/%s frontier ratio %.4f is below %.4f",
			payload,
			control,
			candidate,
			ratio,
			e.manifest.AdvantageThreshold,
		))
	}
	return Comparison{
		Control: control, Candidate: candidate, ControlRate: controlRate,
		CandidateRate: candidateRate, Ratio: ratio, Passed: passed,
	}
}

func (e *evaluator) validateMatchedGuardrails(
	payload string,
	controlName string,
	candidateName string,
	metrics map[string]VariantMetrics,
) {
	control := metrics[controlName]
	candidate := metrics[candidateName]
	if control.Runs != e.manifest.Confirmations || candidate.Runs != e.manifest.Confirmations {
		return
	}
	checks := []struct {
		ok     bool
		metric string
		got    float64
		limit  float64
	}{
		{candidate.P95Millis <= 2000, "p95 SLO", candidate.P95Millis, 2000},
		{
			candidate.P95Millis <= control.P95Millis*e.manifest.MaximumP95Ratio,
			"p95 ratio",
			candidate.P95Millis,
			control.P95Millis * e.manifest.MaximumP95Ratio,
		},
		{
			candidate.WALBytesPerMessage <= control.WALBytesPerMessage,
			"WAL/message",
			candidate.WALBytesPerMessage,
			control.WALBytesPerMessage,
		},
		{
			candidate.TransactionsPerMessage <= control.TransactionsPerMessage,
			"transactions/message",
			candidate.TransactionsPerMessage,
			control.TransactionsPerMessage,
		},
		{
			candidate.ClaimExecMillisPerMessage <= control.ClaimExecMillisPerMessage,
			"claim DB ms/message",
			candidate.ClaimExecMillisPerMessage,
			control.ClaimExecMillisPerMessage,
		},
	}
	for _, check := range checks {
		if !check.ok {
			e.addReason(fmt.Sprintf(
				"%s %s/%s %s %.6f exceeds %.6f",
				payload,
				controlName,
				candidateName,
				check.metric,
				check.got,
				check.limit,
			))
		}
	}
}

//nolint:gocyclo // Keeping every immutable proof boundary in one validator prevents partial acceptance.
func validateReportContract(manifest Manifest, report runReport, payload, variant string, rate int) error {
	if report.SpecVersion != reportSpecVersion {
		return fmt.Errorf("report spec version %q, want %q", report.SpecVersion, reportSpecVersion)
	}
	if report.Failure != "" || !report.IntegrityPassed || len(report.Stages) != 1 {
		return fmt.Errorf(
			"report is incomplete or failed (failure=%q integrity=%t stages=%d)",
			report.Failure,
			report.IntegrityPassed,
			len(report.Stages),
		)
	}
	stage := report.Stages[0]
	if !stage.Sustainable || !stage.Integrity.Passed {
		return fmt.Errorf("stage is not sustainable and reconciled: %v", stage.UnsustainableReasons)
	}
	if err := validateRawStageGates(stage, rate); err != nil {
		return err
	}
	if !stage.DrainCompleted || stage.DrainSeconds > manifest.DrainTimeoutSeconds {
		return fmt.Errorf("drain %.3fs did not complete within %.3fs", stage.DrainSeconds, manifest.DrainTimeoutSeconds)
	}
	if stage.TargetRate != rate {
		return fmt.Errorf("target rate %d, want %d", stage.TargetRate, rate)
	}
	if report.Config.PayloadProfile != payload || report.Config.PostgreSQLProfile != manifest.PostgresProfile {
		return errors.New("payload or PostgreSQL profile does not match the manifest")
	}
	if report.Config.WarmupSeconds != manifest.WarmupSeconds ||
		report.Config.StageSeconds != manifest.MeasuredSeconds ||
		report.Config.DrainTimeoutSeconds != manifest.DrainTimeoutSeconds {
		return errors.New("run durations do not match the manifest")
	}
	if report.Environment.GitDirty != "false" || report.Environment.OutboxGitDirty != "false" {
		return fmt.Errorf(
			"dirty checkout provenance: gomessenger=%s outbox=%s",
			report.Environment.GitDirty,
			report.Environment.OutboxGitDirty,
		)
	}
	if report.Environment.GitCommit != manifest.GitCommit ||
		report.Environment.OutboxGitCommit != manifest.OutboxGitCommit {
		return errors.New("report commits do not match the proof manifest")
	}
	if !strings.Contains(strings.ToLower(report.Environment.OutboxVersion), "devel") {
		return errors.New("checkout-workspace proof requires a locally replaced Outbox module")
	}
	wantIngress, wantRelay, wantConsumer, ok := variantModes(variant)
	if !ok {
		return fmt.Errorf("unknown variant %q", variant)
	}
	if report.Environment.OutboxIngressMode != wantIngress ||
		report.Environment.OutboxRelayMode != wantRelay ||
		report.Environment.ConsumerMode != wantConsumer {
		return fmt.Errorf(
			"runtime modes %s/%s/%s, want %s/%s/%s",
			report.Environment.OutboxIngressMode,
			report.Environment.OutboxRelayMode,
			report.Environment.ConsumerMode,
			wantIngress,
			wantRelay,
			wantConsumer,
		)
	}
	if report.Environment.OutboxBatchMaxMessages != 100 || report.Environment.ConsumerBatchMaxMessages != 100 {
		return errors.New("outbox and consumer batch maximums must both be 100")
	}
	if report.Environment.OutboxWorkers != 2 || report.Environment.ConsumerConcurrency != 2 ||
		report.Environment.OutboxProducerMaxConns != 6 || report.Environment.OutboxRelayMaxConns != 2 ||
		report.Environment.DBMaxOpenConns != 8 {
		return errors.New("runtime resources do not match topology o2-c2")
	}
	if report.Environment.PostgreSQLProfile != manifest.PostgresProfile ||
		report.Environment.SUTCPUSet != "0-1" ||
		report.Environment.PostgreSQLMemoryBytes != 1<<30 ||
		report.Environment.NATSMemoryBytes != 512<<20 ||
		report.Environment.APIMemoryBytes != 512<<20 ||
		!report.Environment.SwapDisabled {
		return errors.New("container resources do not match the normalized proof profile")
	}
	if report.Environment.GitCommit == "" || report.Environment.GitCommit == unknownValue ||
		report.Environment.OutboxGitCommit == "" || report.Environment.OutboxGitCommit == unknownValue ||
		report.Environment.PostgreSQLImageDigest == "" ||
		report.Environment.PostgreSQLImageDigest == unknownValue ||
		report.Environment.NATSImageDigest == "" || report.Environment.NATSImageDigest == unknownValue {
		return errors.New("commit or image provenance is missing")
	}
	return nil
}

func validateRawStageGates(stage stageReport, rate int) error {
	minimumThroughput := float64(rate) * 0.98
	maximumLagGrowth := float64(rate) * 0.01
	checks := []struct {
		ok     bool
		reason string
	}{
		{stage.K6.ExitCode == 0, "k6 exit code is non-zero"},
		{stage.K6.DroppedIterations == 0, "k6 dropped iterations are non-zero"},
		{stage.K6.HTTPFailureRate == 0, "HTTP failure rate is non-zero"},
		{stage.K6.AcceptedRate == 1, "HTTP accepted rate is not 1"},
		{stage.AcceptedMessagesPerSec >= minimumThroughput, "accepted throughput is below 98%"},
		{stage.RelayMessagesPerSec >= minimumThroughput, "relay throughput is below 98%"},
		{stage.ConsumerMessagesPerSec >= minimumThroughput, "consumer throughput is below 98%"},
		{stage.OutboxLagGrowthPerSec <= maximumLagGrowth, "Outbox lag is not stable"},
		{stage.ConsumerLagGrowthPerSec <= maximumLagGrowth, "consumer lag is not stable"},
		{stage.BacklogSlopePerSec <= maximumLagGrowth, "end-to-end backlog is not stable"},
		{stage.Latency.P95Millis > 0 && stage.Latency.P95Millis <= 2000, "business p95 is outside (0, 2s]"},
		{stage.MaxBrokerRedelivered == 0, "broker redeliveries are non-zero"},
		{stage.InboxDuplicates == 0, "Inbox duplicates are non-zero"},
		{stage.DLQMessages == 0, "consumer DLQ messages are non-zero"},
		{stage.InboxHandle.Errors == 0, "Inbox handler errors are non-zero"},
		{stage.BrokerAck.Errors == 0, "broker ACK errors are non-zero"},
		{stage.ConsumerBatch.Handler.Errors == 0, "consumer batch handler errors are non-zero"},
		{stage.OutboxExecution.Handler.Handler.Errors == 0, "Outbox handler errors are non-zero"},
		{stage.OutboxExecution.Publish.Handler.Errors == 0, "Outbox publish errors are non-zero"},
		{stage.OutboxExecution.Finalization.Handler.Errors == 0, "Outbox finalization errors are non-zero"},
		{stage.OutboxExecution.Outcomes.Retry == 0, "Outbox retries are non-zero"},
		{stage.OutboxExecution.Outcomes.Defer == 0, "Outbox defers are non-zero"},
		{stage.OutboxExecution.Outcomes.DLQ == 0, "Outbox DLQ outcomes are non-zero"},
		{validPoolHealth(stage.OutboxDatabase.Producer), "Outbox producer connection health is missing or failed"},
		{validPoolHealth(stage.OutboxDatabase.Relay), "Outbox relay connection health is missing or failed"},
		{len(stage.Integrity.Reasons) == 0, "integrity report contains failure reasons"},
		{reconciledCounts(stage.AfterDrain), "post-drain durable counts do not reconcile"},
	}
	for _, check := range checks {
		if !check.ok {
			return errors.New(check.reason)
		}
	}
	return nil
}

func validPoolHealth(stats pgxPoolStageStats) bool {
	return stats.MaxConnections > 0 &&
		stats.MaxAcquiredConnections > 0 &&
		stats.MaxAcquiredConnections <= stats.MaxConnections &&
		stats.ReplacementConnections == 0 &&
		stats.CanceledAcquires == 0 &&
		stats.UnusableReleases == 0
}

func reconciledCounts(counts stageCounts) bool {
	if counts.Offered <= 0 {
		return false
	}
	boundaries := []int64{
		counts.HTTPAccepted,
		counts.BusinessAccepted,
		counts.Staged,
		counts.Published,
		counts.StreamPublished,
		counts.Committed,
	}
	for _, count := range boundaries {
		if count != counts.Offered {
			return false
		}
	}
	return true
}

func variantModes(variant string) (ingress, relay, consumer string, ok bool) {
	switch variant {
	case variantLegacy:
		return modeSingle, modeSingle, modeSingle, true
	case variantConsumer:
		return modeSingle, modeSingle, modeBatch, true
	case variantRelay:
		return modeSingle, modeBatch, modeBatch, true
	case variantFull:
		return modeBatch, modeBatch, modeBatch, true
	default:
		return "", "", "", false
	}
}

func provenanceFromReport(report runReport) provenanceKey {
	environment := report.Environment
	return provenanceKey{
		Provenance: Provenance{
			GitCommit: environment.GitCommit, OutboxGitCommit: environment.OutboxGitCommit,
			GoVersion: environment.GoVersion, OutboxVersion: environment.OutboxVersion,
			PostgreSQLVersion:     environment.PostgreSQLVersion,
			PostgreSQLImageDigest: environment.PostgreSQLImageDigest,
			NATSServerVersion:     environment.NATSServerVersion, NATSImageDigest: environment.NATSImageDigest,
			HostOS: environment.HostOS, HostArch: environment.HostArch, HostCPUs: environment.HostCPUs,
		},
		ContainerOS: environment.ContainerOS, ContainerArch: environment.ContainerArch,
		ContainerCPUs: environment.ContainerCPUs, PostgreSQLProfile: environment.PostgreSQLProfile,
		PostgreSQLImage: environment.PostgreSQLImage, NATSImage: environment.NATSImage,
		K6Version: environment.K6Version, OutboxWorkers: environment.OutboxWorkers,
		OutboxReservationBatchSize: environment.OutboxReservationBatchSize,
		OutboxProducerMaxConns:     environment.OutboxProducerMaxConns,
		OutboxRelayMaxConns:        environment.OutboxRelayMaxConns,
		ConsumerConcurrency:        environment.ConsumerConcurrency, DBMaxOpenConns: environment.DBMaxOpenConns,
		SUTCPUSet: environment.SUTCPUSet, PostgreSQLMemoryBytes: environment.PostgreSQLMemoryBytes,
		NATSMemoryBytes: environment.NATSMemoryBytes, APIMemoryBytes: environment.APIMemoryBytes,
		SwapDisabled: environment.SwapDisabled, PostgreSQLSettings: environment.PostgreSQLSettings,
	}
}

func collectRunMetrics(manifest Manifest, ref CommonRunRef, report runReport) (runMetrics, error) {
	stage := report.Stages[0]
	committed := stage.LoadWindow.Committed
	if committed <= 0 {
		return runMetrics{}, errors.New("load window committed no messages")
	}
	if stage.Latency.P95Millis <= 0 {
		return runMetrics{}, errors.New("business p95 telemetry is missing")
	}
	if stage.PostgreSQLNormalized.SQLCalls <= 0 ||
		stage.PostgreSQLNormalized.Transactions <= 0 ||
		stage.PostgreSQLNormalized.WALBytes <= 0 ||
		stage.PostgreSQLNormalized.TransactionsPerMessage <= 0 ||
		stage.PostgreSQLNormalized.WALBytesPerMessage <= 0 {
		return runMetrics{}, errors.New("normalized PostgreSQL cost telemetry is missing")
	}
	claim := claimStats(stage.PostgreSQL.LoadDelta.Statements)
	if claim.calls == 0 {
		return runMetrics{}, errors.New("PostgreSQL claim statement telemetry is missing")
	}
	memory, err := analyzeResources(ref.Resources, stage, report.Environment, manifest)
	if err != nil {
		return runMetrics{}, err
	}
	walWaits, err := analyzeWALWaits(ref.Samples, stage)
	if err != nil {
		return runMetrics{}, err
	}
	messages := float64(committed)
	return runMetrics{
		p95Millis:                  stage.Latency.P95Millis,
		sqlCallsPerMessage:         float64(stage.PostgreSQLNormalized.SQLCalls) / messages,
		transactionsPerMessage:     stage.PostgreSQLNormalized.TransactionsPerMessage,
		walBytesPerMessage:         stage.PostgreSQLNormalized.WALBytesPerMessage,
		claimCallsPerMessage:       float64(claim.calls) / messages,
		claimExecMillisPerMessage:  claim.execMillis / messages,
		claimedRowsPerCall:         float64(claim.rows) / float64(claim.calls),
		walWaitSamples:             walWaits.waitSamples,
		maximumConsecutiveWALWaits: walWaits.maximumConsecutive,
		outboxHandlerAverageBatch:  stage.OutboxExecution.Handler.AverageMessages,
		outboxPublishAverageBatch:  stage.OutboxExecution.Publish.AverageMessages,
		outboxFinalizeAverageBatch: stage.OutboxExecution.Finalization.AverageMessages,
		consumerAverageBatch:       stage.ConsumerBatch.AverageMessages,
		peakMemoryFraction:         memory.peakFraction,
		maximumMemoryGrowthBytes:   memory.maximumGrowthBytes,
	}, nil
}

type statementStats struct {
	calls      int64
	rows       int64
	execMillis float64
}

func claimStats(statements []statement) statementStats {
	var result statementStats
	for _, statement := range statements {
		query := strings.ToLower(statement.Query)
		if statement.Classification != "outbox" ||
			!strings.Contains(query, "with requested(name, schema_version)") ||
			!strings.Contains(query, "for update of j skip locked") {
			continue
		}
		result.calls += statement.Calls
		result.rows += statement.Rows
		result.execMillis += statement.TotalExecTimeMillis
	}
	return result
}

func aggregateMetrics(runs []runMetrics) VariantMetrics {
	result := VariantMetrics{
		Runs:                       len(runs),
		P95Millis:                  medianMetric(runs, func(run runMetrics) float64 { return run.p95Millis }),
		SQLCallsPerMessage:         medianMetric(runs, func(run runMetrics) float64 { return run.sqlCallsPerMessage }),
		TransactionsPerMessage:     medianMetric(runs, func(run runMetrics) float64 { return run.transactionsPerMessage }),
		WALBytesPerMessage:         medianMetric(runs, func(run runMetrics) float64 { return run.walBytesPerMessage }),
		ClaimCallsPerMessage:       medianMetric(runs, func(run runMetrics) float64 { return run.claimCallsPerMessage }),
		ClaimExecMillisPerMessage:  medianMetric(runs, func(run runMetrics) float64 { return run.claimExecMillisPerMessage }),
		ClaimedRowsPerCall:         medianMetric(runs, func(run runMetrics) float64 { return run.claimedRowsPerCall }),
		WALWaitSamples:             medianMetric(runs, func(run runMetrics) float64 { return float64(run.walWaitSamples) }),
		OutboxHandlerAverageBatch:  medianMetric(runs, func(run runMetrics) float64 { return run.outboxHandlerAverageBatch }),
		OutboxPublishAverageBatch:  medianMetric(runs, func(run runMetrics) float64 { return run.outboxPublishAverageBatch }),
		OutboxFinalizeAverageBatch: medianMetric(runs, func(run runMetrics) float64 { return run.outboxFinalizeAverageBatch }),
		ConsumerAverageBatch:       medianMetric(runs, func(run runMetrics) float64 { return run.consumerAverageBatch }),
		PeakMemoryFraction:         medianMetric(runs, func(run runMetrics) float64 { return run.peakMemoryFraction }),
		MaximumMemoryGrowthBytes:   medianMetric(runs, func(run runMetrics) float64 { return run.maximumMemoryGrowthBytes }),
	}
	for _, run := range runs {
		result.MaximumConsecutiveWALWaits = max(
			result.MaximumConsecutiveWALWaits,
			run.maximumConsecutiveWALWaits,
		)
	}
	return result
}

func medianMetric(runs []runMetrics, value func(runMetrics) float64) float64 {
	values := make([]float64, len(runs))
	for index, run := range runs {
		values[index] = value(run)
	}
	slices.Sort(values)
	if len(values) == 0 {
		return 0
	}
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

type resourceSummary struct {
	peakFraction       float64
	maximumGrowthBytes float64
}

//nolint:tagliatelle // Docker stats emits PascalCase keys inside the container object.
type resourceSample struct {
	ObservedAt time.Time `json:"observedAt"`
	Container  struct {
		Name     string `json:"Name"`
		MemUsage string `json:"MemUsage"`
	} `json:"container"`
}

func analyzeResources(
	path string,
	stage stageReport,
	environment environment,
	manifest Manifest,
) (resourceSummary, error) {
	file, err := os.Open(path)
	if err != nil {
		return resourceSummary{}, fmt.Errorf("open resources: %w", err)
	}
	defer file.Close()
	limits := map[string]float64{
		resourcePostgres: float64(environment.PostgreSQLMemoryBytes),
		resourceNATS:     float64(environment.NATSMemoryBytes),
		resourceAPI:      float64(environment.APIMemoryBytes),
	}
	values := map[string][]float64{resourcePostgres: {}, resourceNATS: {}, resourceAPI: {}}
	windowEnd := stage.LoadEndedAt.Add(time.Duration(stage.DrainSeconds * float64(time.Second)))
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var sample resourceSample
		if err := json.Unmarshal(scanner.Bytes(), &sample); err != nil {
			return resourceSummary{}, fmt.Errorf("decode resource sample: %w", err)
		}
		if sample.ObservedAt.Before(stage.LoadStartedAt) || sample.ObservedAt.After(windowEnd) {
			continue
		}
		service := resourceService(sample.Container.Name)
		if service == "" {
			continue
		}
		used, err := parseDockerBytes(strings.Split(sample.Container.MemUsage, "/")[0])
		if err != nil {
			return resourceSummary{}, fmt.Errorf("parse %s memory: %w", service, err)
		}
		values[service] = append(values[service], used)
	}
	if err := scanner.Err(); err != nil {
		return resourceSummary{}, fmt.Errorf("read resources: %w", err)
	}
	var result resourceSummary
	for service, samples := range values {
		if len(samples) < minimumSamples {
			return resourceSummary{}, fmt.Errorf(
				"%s has %d measured resource samples, want at least %d",
				service,
				len(samples),
				minimumSamples,
			)
		}
		peak := slices.Max(samples)
		peakFraction := peak / limits[service]
		result.peakFraction = max(result.peakFraction, peakFraction)
		if peakFraction >= manifest.MaximumMemoryFraction {
			return resourceSummary{}, fmt.Errorf(
				"%s peak memory fraction %.4f reaches limit %.4f",
				service,
				peakFraction,
				manifest.MaximumMemoryFraction,
			)
		}
		third := len(samples) / 3
		firstMedian := medianFloat(samples[:third])
		lastMedian := medianFloat(samples[len(samples)-third:])
		growth := max(lastMedian-firstMedian, 0)
		result.maximumGrowthBytes = max(result.maximumGrowthBytes, growth)
		allowedGrowth := max(manifest.MaximumMemoryGrowthBytes, firstMedian*manifest.MaximumMemoryGrowthRatio)
		if growth > allowedGrowth {
			return resourceSummary{}, fmt.Errorf(
				"%s median memory grew %.0f bytes, limit %.0f bytes",
				service,
				growth,
				allowedGrowth,
			)
		}
	}
	return result, nil
}

func resourceService(name string) string {
	switch {
	case strings.Contains(name, resourceAPI):
		return resourceAPI
	case strings.Contains(name, resourcePostgres):
		return resourcePostgres
	case strings.Contains(name, resourceNATS):
		return resourceNATS
	default:
		return ""
	}
}

func parseDockerBytes(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	units := []struct {
		suffix string
		factor float64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"GB", 1e9},
		{"MB", 1e6},
		{"kB", 1e3},
		{"B", 1},
	}
	for _, unit := range units {
		if !strings.HasSuffix(raw, unit.suffix) {
			continue
		}
		var value float64
		if _, err := fmt.Sscanf(strings.TrimSuffix(raw, unit.suffix), "%f", &value); err != nil {
			return 0, err
		}
		return value * unit.factor, nil
	}
	return 0, fmt.Errorf("unsupported Docker byte value %q", raw)
}

func medianFloat(values []float64) float64 {
	copyValues := append([]float64(nil), values...)
	slices.Sort(copyValues)
	middle := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[middle]
	}
	return (copyValues[middle-1] + copyValues[middle]) / 2
}

type waitSample struct {
	ObservedAt      time.Time   `json:"observedAt"`
	StageID         string      `json:"stageId"`
	Phase           string      `json:"phase"`
	PostgreSQLWaits []waitEvent `json:"postgresqlWaits"`
}

type walWaitSummary struct {
	waitSamples        int
	maximumConsecutive int
}

func analyzeWALWaits(path string, stage stageReport) (walWaitSummary, error) {
	file, err := os.Open(path)
	if err != nil {
		return walWaitSummary{}, fmt.Errorf("open samples: %w", err)
	}
	defer file.Close()
	samples := make([]waitSample, 0, minimumSamples)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var sample waitSample
		if err := json.Unmarshal(scanner.Bytes(), &sample); err != nil {
			return walWaitSummary{}, fmt.Errorf("decode capacity sample: %w", err)
		}
		if sample.StageID == stage.StageID && sample.Phase == "load" &&
			!sample.ObservedAt.Before(stage.LoadStartedAt) && !sample.ObservedAt.After(stage.LoadEndedAt) {
			samples = append(samples, sample)
		}
	}
	if err := scanner.Err(); err != nil {
		return walWaitSummary{}, fmt.Errorf("read capacity samples: %w", err)
	}
	if len(samples) < minimumSamples {
		return walWaitSummary{}, fmt.Errorf(
			"measured stage has %d wait samples, want at least %d",
			len(samples),
			minimumSamples,
		)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].ObservedAt.Before(samples[j].ObservedAt) })
	result := walWaitSummary{}
	consecutive := 0
	for _, sample := range samples {
		if hasWALWait(sample.PostgreSQLWaits) {
			result.waitSamples++
			consecutive++
			result.maximumConsecutive = max(result.maximumConsecutive, consecutive)
			if consecutive >= 3 {
				return walWaitSummary{}, errors.New(
					"WALWrite or WALSync appeared in three consecutive measured samples",
				)
			}
			continue
		}
		consecutive = 0
	}
	return result, nil
}

func hasWALWait(waits []waitEvent) bool {
	for _, wait := range waits {
		if wait.Sessions > 0 && (wait.WaitEvent == "WALWrite" || wait.WaitEvent == "WALSync") {
			return true
		}
	}
	return false
}

func commonRate(runs []CommonRunRef, payload string) int {
	for _, run := range runs {
		if run.PayloadProfile == payload {
			return run.Rate
		}
	}
	return 0
}

func proofKey(payload, variant string) string { return payload + "/" + variant }

func (e *evaluator) validateCommonOrder() {
	orders := [][]string{
		{variantLegacy, variantConsumer, variantRelay, variantFull},
		{variantFull, variantRelay, variantConsumer, variantLegacy},
		{variantConsumer, variantLegacy, variantFull, variantRelay},
	}
	expected := make([]string, 0, len(requiredPayloads)*len(requiredVariants)*len(orders))
	for _, payload := range requiredPayloads {
		for repetition, order := range orders {
			for _, variant := range order {
				expected = append(expected, fmt.Sprintf("%s/%s/%d", payload, variant, repetition+1))
			}
		}
	}
	if len(e.manifest.CommonRuns) != len(expected) {
		return
	}
	for index, ref := range e.manifest.CommonRuns {
		actual := fmt.Sprintf("%s/%s/%d", ref.PayloadProfile, ref.Variant, ref.Repetition)
		if actual != expected[index] {
			e.addReason(fmt.Sprintf(
				"common run order at position %d is %s, want %s",
				index+1,
				actual,
				expected[index],
			))
			return
		}
	}
}

func (e *evaluator) recordRunID(runID, path string) {
	if runID == "" {
		e.addReason("evidence run ID is empty")
		return
	}
	if previous, duplicate := e.runIDs[runID]; duplicate {
		e.addReason(fmt.Sprintf("run ID %s is reused by %s and %s", runID, previous, path))
		return
	}
	e.runIDs[runID] = path
}

func (e *evaluator) addReason(reason string) {
	if !slices.Contains(e.proof.Reasons, reason) {
		e.proof.Reasons = append(e.proof.Reasons, reason)
	}
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func renderMarkdown(proof Proof) string {
	var builder strings.Builder
	status := "FAIL"
	if proof.Passed {
		status = "PASS"
	}
	_, _ = fmt.Fprintf(&builder, "# True-batch capacity proof: %s\n\n", status)
	_, _ = fmt.Fprintf(&builder, "Proof ID: `%s`\n\n", proof.ProofID)
	_, _ = fmt.Fprintf(&builder, "Evidence scope: `%s`\n\n", proof.EvidenceScope)
	if proof.Provenance != nil {
		_, _ = fmt.Fprintf(&builder, "GoMessenger commit `%s`; Outbox commit `%s` (module `%s`); PostgreSQL `%s`; NATS `%s`.\n\n",
			proof.Provenance.GitCommit, proof.Provenance.OutboxGitCommit,
			proof.Provenance.OutboxVersion,
			proof.Provenance.PostgreSQLImageDigest, proof.Provenance.NATSImageDigest)
	}
	for _, payload := range proof.Payloads {
		_, _ = fmt.Fprintf(&builder, "## %s payload\n\n", payload.PayloadProfile)
		_, _ = fmt.Fprintf(&builder, "Common matched rate: `%d msg/s`.\n\n", payload.CommonRate)
		_, _ = builder.WriteString("| Comparison | Control | Candidate | Ratio | Verdict |\n")
		_, _ = builder.WriteString("|---|---:|---:|---:|---|\n")
		writeComparisonRow(&builder, "Isolated Outbox relay", payload.IsolatedRelay)
		writeComparisonRow(&builder, "End to end", payload.EndToEnd)
		_, _ = builder.WriteString("\n| Variant | p95 ms | SQL/msg | tx/msg | WAL B/msg | claim calls/msg | ")
		_, _ = builder.WriteString("claim ms/msg | WAL wait samples | max WAL wait streak | Outbox avg batch | RSS peak |\n")
		_, _ = builder.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, variant := range requiredVariants {
			metrics := payload.Common[variant]
			_, _ = fmt.Fprintf(&builder, "| %s | %.2f | %.4f | %.4f | %.2f | %.4f | %.6f | %.1f | %d | %.2f | %.2f%% |\n",
				variant, metrics.P95Millis, metrics.SQLCallsPerMessage, metrics.TransactionsPerMessage,
				metrics.WALBytesPerMessage, metrics.ClaimCallsPerMessage, metrics.ClaimExecMillisPerMessage,
				metrics.WALWaitSamples, metrics.MaximumConsecutiveWALWaits,
				metrics.OutboxHandlerAverageBatch, metrics.PeakMemoryFraction*100)
		}
		_ = builder.WriteByte('\n')
	}
	if len(proof.Reasons) > 0 {
		_, _ = builder.WriteString("## Failed gates\n\n")
		for _, reason := range proof.Reasons {
			_, _ = fmt.Fprintf(&builder, "- %s\n", reason)
		}
		_ = builder.WriteByte('\n')
	}
	_, _ = builder.WriteString("This is checkout-workspace pre-publication evidence, " +
		"not a published-release or production-capacity claim.\n")
	return builder.String()
}

func writeComparisonRow(builder *strings.Builder, label string, comparison Comparison) {
	status := "FAIL"
	if comparison.Passed {
		status = "PASS"
	}
	_, _ = fmt.Fprintf(builder, "| %s | %d | %d | %.3fx | %s |\n",
		label, comparison.ControlRate, comparison.CandidateRate, comparison.Ratio, status)
}
