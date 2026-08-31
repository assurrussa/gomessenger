//nolint:testpackage // White-box fixtures exercise fail-closed parsing helpers and internal schemas.
package batchproof

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEvaluateDirPassesCompleteMatchedProof(t *testing.T) {
	fixture := newProofFixture(t)
	proof, err := EvaluateDir(fixture.dir)
	if err != nil {
		t.Fatal(err)
	}
	if !proof.Passed || len(proof.Reasons) != 0 {
		t.Fatalf("proof = %#v, want PASS", proof)
	}
	if proof.EvidenceScope != evidenceScopeCheckoutWorkspace {
		t.Fatalf("evidence scope = %q, want %q", proof.EvidenceScope, evidenceScopeCheckoutWorkspace)
	}
	if err := Write(fixture.dir, proof); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"proof.json", "proof.md"} {
		if _, err := os.Stat(filepath.Join(fixture.dir, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
	encoded, err := os.ReadFile(filepath.Join(fixture.dir, "proof.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Proof
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.EvidenceScope != evidenceScopeCheckoutWorkspace {
		t.Fatalf("persisted evidence scope = %q, want %q", persisted.EvidenceScope, evidenceScopeCheckoutWorkspace)
	}
	markdown, err := os.ReadFile(filepath.Join(fixture.dir, "proof.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Evidence scope: `checkout-workspace`",
		"Outbox commit `outbox-commit` (module `(devel)`)",
		"not a published-release or production-capacity claim",
	} {
		if !strings.Contains(string(markdown), want) {
			t.Fatalf("proof.md does not contain %q", want)
		}
	}
}

//nolint:thelper // Table mutators receive testing.T to use fixture helpers, not as standalone helpers.
func TestEvaluateDirFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *proofFixture)
		want   string
	}{
		{
			name: "wrong evidence scope",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.manifest.EvidenceScope = "published-module-graph"
				fixture.writeManifest(t)
			},
			want: "evidence scope must be checkout-workspace",
		},
		{
			name: "incomplete common series",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.manifest.CommonRuns = fixture.manifest.CommonRuns[:len(fixture.manifest.CommonRuns)-1]
				fixture.writeManifest(t)
			},
			want: "missing common run",
		},
		{
			name: "non interleaved common order",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.manifest.CommonRuns[0], fixture.manifest.CommonRuns[1] =
					fixture.manifest.CommonRuns[1], fixture.manifest.CommonRuns[0]
				fixture.writeManifest(t)
			},
			want: "common run order at position 1",
		},
		{
			name: "mismatched global common rate",
			mutate: func(t *testing.T, fixture *proofFixture) {
				for index := range fixture.manifest.CommonRuns {
					ref := &fixture.manifest.CommonRuns[index]
					if ref.PayloadProfile != "mixed" {
						continue
					}
					ref.Rate = 850
					fixture.mutateReport(t, ref.Report, false, func(report *runReport) {
						report.Stages[0].TargetRate = 850
					})
				}
				fixture.writeManifest(t)
			},
			want: "common runs use mismatched rates 800 and 850",
		},
		{
			name: "dirty checkout",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstReport(t, func(report *runReport) {
					report.Environment.GitDirty = "true"
				})
			},
			want: "dirty checkout provenance",
		},
		{
			name: "published outbox module in checkout proof",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstReport(t, func(report *runReport) {
					report.Environment.OutboxVersion = "v0.13.0"
				})
			},
			want: "checkout-workspace proof requires a locally replaced Outbox module",
		},
		{
			name: "commit drift",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "mixed", variantLegacy, func(report *runReport) {
					report.Environment.GitCommit = "different"
				})
			},
			want: "report commits do not match the proof manifest",
		},
		{
			name: "image drift",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantFull, func(report *runReport) {
					report.Environment.NATSImageDigest = "nats@sha256:different"
				})
			},
			want: "provenance differs",
		},
		{
			name: "topology drift",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantLegacy, func(report *runReport) {
					report.Environment.DBMaxOpenConns = 7
				})
			},
			want: "runtime resources do not match topology",
		},
		{
			name: "duration drift",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstReport(t, func(report *runReport) {
					report.Config.StageSeconds = 119
				})
			},
			want: "run durations do not match",
		},
		{
			name: "threshold below contract",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.manifest.AdvantageThreshold = 1.2
				fixture.writeManifest(t)
			},
			want: "advantage threshold must be at least 1.3",
		},
		{
			name: "frontier advantage below threshold",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.manifest.AdvantageThreshold = 1.6
				fixture.writeManifest(t)
			},
			want: "frontier ratio 1.5000 is below 1.6000",
		},
		{
			name: "unsustainable report",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantRelay, func(report *runReport) {
					report.Stages[0].Sustainable = false
					report.Stages[0].UnsustainableReasons = []string{"synthetic overload"}
				})
			},
			want: "stage is not sustainable",
		},
		{
			name: "raw throughput SLO violation",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantLegacy, func(report *runReport) {
					report.Stages[0].ConsumerMessagesPerSec = 700
				})
			},
			want: "consumer throughput is below 98%",
		},
		{
			name: "raw p95 SLO violation",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantLegacy, func(report *runReport) {
					report.Stages[0].Latency.P95Millis = 2001
				})
			},
			want: "business p95 is outside (0, 2s]",
		},
		{
			name: "missing connection health",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantLegacy, func(report *runReport) {
					report.Stages[0].OutboxDatabase = outboxDatabaseStats{}
				})
			},
			want: "Outbox producer connection health is missing or failed",
		},
		{
			name: "connection replacement",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantFull, func(report *runReport) {
					report.Stages[0].OutboxDatabase.Relay.ReplacementConnections = 1
				})
			},
			want: "Outbox relay connection health is missing or failed",
		},
		{
			name: "raw reconciliation violation",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "mixed", variantFull, func(report *runReport) {
					report.Stages[0].AfterDrain.Committed--
				})
			},
			want: "post-drain durable counts do not reconcile",
		},
		{
			name: "batch not exercised",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantRelay, func(report *runReport) {
					report.Stages[0].OutboxExecution.Handler.AverageMessages = 5
				})
			},
			want: "average batch 5.00 is below 10.00",
		},
		{
			name: "matched p95 regression",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateCommonReports(t, "mixed", variantFull, func(report *runReport) {
					report.Stages[0].Latency.P95Millis = 120
				})
			},
			want: "p95 ratio 120.000000 exceeds 110.000000",
		},
		{
			name: "missing p95 telemetry",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantRelay, func(report *runReport) {
					report.Stages[0].Latency.P95Millis = 0
				})
			},
			want: "business p95 is outside (0, 2s]",
		},
		{
			name: "matched WAL regression",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateCommonReports(t, "small", variantRelay, func(report *runReport) {
					report.Stages[0].PostgreSQLNormalized.WALBytesPerMessage = 101
				})
			},
			want: "WAL/message 101.000000 exceeds 100.000000",
		},
		{
			name: "matched transaction regression",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateCommonReports(t, "mixed", variantFull, func(report *runReport) {
					report.Stages[0].PostgreSQLNormalized.TransactionsPerMessage = 1.01
				})
			},
			want: "transactions/message 1.010000 exceeds 1.000000",
		},
		{
			name: "matched claim time regression",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateCommonReports(t, "small", variantRelay, func(report *runReport) {
					report.Stages[0].PostgreSQL.LoadDelta.Statements[0].TotalExecTimeMillis = 9700
				})
			},
			want: "claim DB ms/message 0.101042 exceeds 0.100000",
		},
		{
			name: "missing claim telemetry",
			mutate: func(t *testing.T, fixture *proofFixture) {
				fixture.mutateFirstCommonReport(t, "small", variantLegacy, func(report *runReport) {
					report.Stages[0].PostgreSQL.LoadDelta.Statements = nil
				})
			},
			want: "claim statement telemetry is missing",
		},
		{
			name: "memory limit",
			mutate: func(t *testing.T, fixture *proofFixture) {
				ref := fixture.firstCommon("small", variantFull)
				writeResourceSamples(t, ref.Resources, fixture.startedAt, 500, 100)
			},
			want: "capacity-api peak memory fraction",
		},
		{
			name: "memory growth",
			mutate: func(t *testing.T, fixture *proofFixture) {
				ref := fixture.firstCommon("small", variantRelay)
				writeGrowingResourceSamples(t, ref.Resources, fixture.startedAt)
			},
			want: "capacity-api median memory grew",
		},
		{
			name: "consecutive WAL waits",
			mutate: func(t *testing.T, fixture *proofFixture) {
				ref := fixture.firstCommon("mixed", variantRelay)
				writeWaitSamples(t, ref.Samples, fixture.startedAt, true)
			},
			want: "WALWrite or WALSync appeared in three consecutive",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProofFixture(t)
			test.mutate(t, fixture)
			proof, err := EvaluateDir(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			if proof.Passed {
				t.Fatalf("proof unexpectedly passed: %#v", proof)
			}
			if !containsProofReason(proof.Reasons, test.want) {
				t.Fatalf("reasons %q do not contain %q", proof.Reasons, test.want)
			}
		})
	}
}

func TestParseDockerBytes(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{"0B", 0},
		{"1KiB", 1 << 10},
		{"2.5MiB", 2.5 * (1 << 20)},
		{"1GiB", 1 << 30},
		{"3MB", 3e6},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseDockerBytes(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("parseDockerBytes(%q) = %f, want %f", test.input, got, test.want)
			}
		})
	}
}

type proofFixture struct {
	dir       string
	startedAt time.Time
	manifest  Manifest
}

func newProofFixture(t *testing.T) *proofFixture {
	t.Helper()
	fixture := &proofFixture{
		dir:       t.TempDir(),
		startedAt: time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC),
		manifest: Manifest{
			SpecVersion: manifestSpecVersion, ProofID: "test-proof",
			EvidenceScope: evidenceScopeCheckoutWorkspace, Topology: "o2-c2",
			GitCommit: "gomessenger-commit", OutboxGitCommit: "outbox-commit",
			PostgresProfile: postgresProfileStock, WarmupSeconds: 60, MeasuredSeconds: 120,
			DrainTimeoutSeconds: 60, Confirmations: 3, FrontierStep: 50,
			AdvantageThreshold: 1.3, MaximumP95Ratio: 1.1, MinimumAverageBatch: 10,
			MaximumMemoryFraction: 0.8, MaximumMemoryGrowthRatio: 0.1,
			MaximumMemoryGrowthBytes: 32 << 20,
		},
	}
	for _, payload := range requiredPayloads {
		for _, variant := range requiredVariants {
			frontierRate := 1000
			if variant == variantRelay || variant == variantFull {
				frontierRate = 1500
			}
			frontier := frontierSummary{
				SpecVersion: frontierSpecVersion, FrontierID: "frontier-" + payload + "-" + variant,
				Variant: variant, Topology: "o2-c2", PayloadProfile: payload,
				PostgresProfile: postgresProfileStock, FrontierRate: frontierRate,
				OutboxBatchMaxMessages: 100, ConsumerBatchMaxMessages: 100,
			}
			for repetition := 1; repetition <= 3; repetition++ {
				runID := frontier.FrontierID + "-confirm-" + string(rune('0'+repetition))
				reportPath := filepath.Join(fixture.dir, runID+"-report.json")
				writeJSON(t, reportPath, fixture.report(runID, payload, variant, frontierRate))
				frontier.Runs = append(frontier.Runs, frontierRun{
					RunID: runID, Phase: "confirm-" + string(rune('0'+repetition)), State: "pass",
					Rate: frontierRate, OutboxBatchSize: 100, ConsumerBatchSize: 100,
					Report: reportPath,
				})
			}
			frontierPath := filepath.Join(fixture.dir, frontier.FrontierID+".json")
			writeJSON(t, frontierPath, frontier)
			fixture.manifest.Frontiers = append(fixture.manifest.Frontiers, FrontierRef{
				PayloadProfile: payload, Variant: variant, Path: frontierPath,
			})
		}
	}
	orders := [][]string{
		{variantLegacy, variantConsumer, variantRelay, variantFull},
		{variantFull, variantRelay, variantConsumer, variantLegacy},
		{variantConsumer, variantLegacy, variantFull, variantRelay},
	}
	for _, payload := range requiredPayloads {
		for repetition, order := range orders {
			for _, variant := range order {
				ordinal := repetition + 1
				runID := "common-" + payload + "-" + variant + "-" + string(rune('0'+ordinal))
				reportPath := filepath.Join(fixture.dir, runID+"-report.json")
				resourcesPath := filepath.Join(fixture.dir, runID+"-resources.jsonl")
				samplesPath := filepath.Join(fixture.dir, runID+"-samples.jsonl")
				writeJSON(t, reportPath, fixture.report(runID, payload, variant, 800))
				writeResourceSamples(t, resourcesPath, fixture.startedAt, 100, 100)
				writeWaitSamples(t, samplesPath, fixture.startedAt, false)
				fixture.manifest.CommonRuns = append(fixture.manifest.CommonRuns, CommonRunRef{
					PayloadProfile: payload, Variant: variant, Repetition: ordinal,
					Rate: 800, RunID: runID, Report: reportPath,
					Resources: resourcesPath, Samples: samplesPath,
				})
			}
		}
	}
	fixture.writeManifest(t)
	return fixture
}

func (f *proofFixture) report(runID, payload, variant string, rate int) runReport {
	ingress, relay, consumer, _ := variantModes(variant)
	messages := int64(rate * 120)
	consumerBatch := batchHandlerStats{}
	if consumer == "batch" {
		consumerBatch = batchHandlerStats{
			Invocations: messages / 50, Messages: messages, AverageMessages: 50, MaxMessages: 100,
		}
	}
	outboxExecution := outboxExecutionStats{Outcomes: batchOutcomeStats{Success: messages}}
	if relay == "batch" {
		batch := batchHandlerStats{
			Invocations: messages / 50, Messages: messages, AverageMessages: 50, MaxMessages: 100,
		}
		outboxExecution.Handler = batch
		outboxExecution.Publish = batch
		outboxExecution.Finalization = batch
	}
	stage := stageReport{
		StageID: "r000800", TargetRate: rate,
		LoadStartedAt: f.startedAt, LoadEndedAt: f.startedAt.Add(120 * time.Second),
		LoadWindowSeconds: 120, DrainSeconds: 5, DrainCompleted: true,
		LoadWindow: stageCounts{Committed: messages},
		AfterDrain: stageCounts{
			Offered: messages, HTTPAccepted: messages, BusinessAccepted: messages,
			Staged: messages, Published: messages, StreamPublished: messages, Committed: messages,
		},
		RelayMessagesPerSec: float64(rate), ConsumerMessagesPerSec: float64(rate),
		AcceptedMessagesPerSec: float64(rate), Latency: latencyStats{P95Millis: 100},
		K6:            k6Result{AcceptedRate: 1},
		ConsumerBatch: consumerBatch, OutboxExecution: outboxExecution,
		PostgreSQLNormalized: postgreSQLNormalizedStats{
			SQLCalls: messages, Transactions: messages, TransactionsPerMessage: 1,
			WALBytes: float64(messages * 100), WALBytesPerMessage: 100,
		},
		OutboxDatabase: outboxDatabaseStats{
			Producer: pgxPoolStageStats{MaxConnections: 6, MaxAcquiredConnections: 6, NewConnections: 5},
			Relay:    pgxPoolStageStats{MaxConnections: 2, MaxAcquiredConnections: 2, NewConnections: 1},
		},
		PostgreSQL: postgreSQLTimeline{LoadDelta: postgreSQLDelta{Statements: []statement{{
			Classification: "outbox", Calls: 1000, Rows: messages,
			TotalExecTimeMillis: float64(messages) * 0.1,
			Query:               "with requested(name, schema_version) as (...) for update of j skip locked",
		}}}},
		Sustainable: true, Integrity: integrityResult{Passed: true},
	}
	return runReport{
		SpecVersion: reportSpecVersion, RunID: runID,
		Config: reportConfig{
			WarmupSeconds: 60, StageSeconds: 120, DrainTimeoutSeconds: 60,
			PayloadProfile: payload, PostgreSQLProfile: postgresProfileStock,
		},
		Environment: environment{
			GoVersion: "go1.27", OutboxVersion: "(devel)", ContainerOS: "linux",
			ContainerArch: "arm64", ContainerCPUs: 10, HostOS: "darwin", HostArch: "arm64",
			HostCPUs: "12", GitCommit: "gomessenger-commit", GitDirty: "false",
			OutboxGitCommit: "outbox-commit", OutboxGitDirty: "false",
			PostgreSQLVersion: "18", PostgreSQLProfile: postgresProfileStock,
			PostgreSQLImage: "postgres:18-alpine", PostgreSQLImageDigest: "postgres@sha256:test",
			NATSServerVersion: "2.12.3", NATSImage: "nats:2.12.3-alpine", NATSImageDigest: "nats@sha256:test",
			K6Version: "1", OutboxWorkers: 2, OutboxReservationBatchSize: 1,
			OutboxProducerMaxConns: 6, OutboxRelayMaxConns: 2,
			OutboxIngressMode: ingress, OutboxRelayMode: relay, OutboxBatchMaxMessages: 100,
			ConsumerConcurrency: 2, ConsumerMode: consumer, ConsumerBatchMaxMessages: 100,
			DBMaxOpenConns: 8, PostgreSQLSettings: map[string]string{"fsync": "on"},
			SUTCPUSet: "0-1", PostgreSQLMemoryBytes: 1 << 30,
			NATSMemoryBytes: 512 << 20, APIMemoryBytes: 512 << 20, SwapDisabled: true,
		},
		Stages: []stageReport{stage}, IntegrityPassed: true,
	}
}

func (f *proofFixture) writeManifest(t *testing.T) {
	t.Helper()
	writeJSON(t, filepath.Join(f.dir, "manifest.json"), f.manifest)
}

func (f *proofFixture) mutateFirstReport(t *testing.T, mutate func(*runReport)) {
	t.Helper()
	f.mutateReport(t, f.manifest.Frontiers[0].Path, true, mutate)
}

func (f *proofFixture) mutateReport(
	t *testing.T,
	path string,
	frontier bool,
	mutate func(*runReport),
) {
	t.Helper()
	reportPath := path
	if frontier {
		var summary frontierSummary
		readJSONForTest(t, path, &summary)
		reportPath = summary.Runs[0].Report
	}
	var report runReport
	readJSONForTest(t, reportPath, &report)
	mutate(&report)
	writeJSON(t, reportPath, report)
}

func (f *proofFixture) mutateFirstCommonReport(
	t *testing.T,
	payload string,
	variant string,
	mutate func(*runReport),
) {
	t.Helper()
	ref := f.firstCommon(payload, variant)
	f.mutateReport(t, ref.Report, false, mutate)
}

func (f *proofFixture) mutateCommonReports(
	t *testing.T,
	payload string,
	variant string,
	mutate func(*runReport),
) {
	t.Helper()
	for _, ref := range f.manifest.CommonRuns {
		if ref.PayloadProfile == payload && ref.Variant == variant {
			f.mutateReport(t, ref.Report, false, mutate)
		}
	}
}

func (f *proofFixture) firstCommon(payload, variant string) CommonRunRef {
	for _, ref := range f.manifest.CommonRuns {
		if ref.PayloadProfile == payload && ref.Variant == variant {
			return ref
		}
	}
	return CommonRunRef{}
}

func writeResourceSamples(t *testing.T, path string, startedAt time.Time, apiMiB, otherMiB float64) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for index := 0; index < 36; index++ {
		for _, item := range []struct {
			name string
			mib  float64
		}{
			{"gomessenger-capacity-nats-capacity-api-1", apiMiB},
			{"gomessenger-capacity-nats-postgres-1", otherMiB},
			{"gomessenger-capacity-nats-nats-1", otherMiB},
		} {
			sample := resourceSample{ObservedAt: startedAt.Add(time.Duration(index) * time.Second)}
			sample.Container.Name = item.name
			sample.Container.MemUsage = strconv.FormatFloat(item.mib, 'f', -1, 64) + "MiB / 4GiB"
			if err := encoder.Encode(sample); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func writeWaitSamples(t *testing.T, path string, startedAt time.Time, repeatedWAL bool) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for index := 0; index < 36; index++ {
		sample := waitSample{
			ObservedAt: startedAt.Add(time.Duration(index) * time.Second),
			StageID:    "r000800", Phase: "load",
		}
		if repeatedWAL && index < 3 {
			sample.PostgreSQLWaits = []waitEvent{{WaitEvent: "WALWrite", Sessions: 1}}
		}
		if err := encoder.Encode(sample); err != nil {
			t.Fatal(err)
		}
	}
}

func writeGrowingResourceSamples(t *testing.T, path string, startedAt time.Time) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for index := 0; index < 36; index++ {
		apiMiB := 100.0
		if index >= 24 {
			apiMiB = 150
		}
		for _, item := range []struct {
			name string
			mib  float64
		}{
			{"gomessenger-capacity-nats-capacity-api-1", apiMiB},
			{"gomessenger-capacity-nats-postgres-1", 100},
			{"gomessenger-capacity-nats-nats-1", 100},
		} {
			sample := resourceSample{ObservedAt: startedAt.Add(time.Duration(index) * time.Second)}
			sample.Container.Name = item.name
			sample.Container.MemUsage = strconv.FormatFloat(item.mib, 'f', -1, 64) + "MiB / 4GiB"
			if err := encoder.Encode(sample); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readJSONForTest(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func containsProofReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}
