package inboxcapacity

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	_ "github.com/jackc/pgx/v5/stdlib" // Register the database/sql driver used by the harness.

	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

const reportSpecVersion = "1.0"

// LatencyStats reports ProcessAttempt service-time percentiles.
type LatencyStats struct {
	P50Millis float64 `json:"p50Millis"`
	P95Millis float64 `json:"p95Millis"`
	P99Millis float64 `json:"p99Millis"`
}

// IntegrityResult reconciles fresh attempts with committed business effects.
type IntegrityResult struct {
	Expected            int64 `json:"expected"`
	Passed              bool  `json:"passed"`
	BusinessEffects     int64 `json:"businessEffects"`
	DistinctMessages    int64 `json:"distinctMessages"`
	CompletedIdentities int64 `json:"completedIdentities"`
	AttemptRows         int64 `json:"attemptRows"`
}

// CaseReport is one measured concurrency/repetition result.
type CaseReport struct {
	Concurrency      int                  `json:"concurrency"`
	Repetition       int                  `json:"repetition"`
	Operations       int                  `json:"operations"`
	DurationSeconds  float64              `json:"durationSeconds"`
	ThroughputPerSec float64              `json:"throughputPerSecond"`
	Latency          LatencyStats         `json:"latency"`
	Integrity        IntegrityResult      `json:"integrity"`
	PostgreSQL       pgtelemetry.Timeline `json:"postgresql"`
}

// Environment records the exact checkout and PostgreSQL runtime.
type Environment struct {
	GoVersion          string            `json:"goVersion"`
	ContainerOS        string            `json:"containerOs"`
	ContainerArch      string            `json:"containerArch"`
	ContainerCPUs      int               `json:"containerLogicalCpus"`
	HostOS             string            `json:"hostOs"`
	HostArch           string            `json:"hostArch"`
	HostCPUs           string            `json:"hostLogicalCpus"`
	GitCommit          string            `json:"gitCommit"`
	GitDirty           string            `json:"gitDirty"`
	PostgreSQLVersion  string            `json:"postgresqlVersion"`
	PostgreSQLSettings map[string]string `json:"postgresqlSettings"`
	DBMaxOpenConns     int               `json:"dbMaxOpenConnections"`
}

// ReportConfig is the stable PostgreSQL-only workload definition.
type ReportConfig struct {
	Warmup        int   `json:"warmupOperations"`
	Operations    int   `json:"measuredOperations"`
	Concurrencies []int `json:"concurrencies"`
	Repetitions   int   `json:"repetitions"`
}

// RunReport is the complete machine-readable PostgreSQL-only artifact.
type RunReport struct {
	SpecVersion     string       `json:"specVersion"`
	RunID           string       `json:"runId"`
	StartedAt       time.Time    `json:"startedAt"`
	CompletedAt     time.Time    `json:"completedAt"`
	Config          ReportConfig `json:"config"`
	Environment     Environment  `json:"environment"`
	Cases           []CaseReport `json:"cases"`
	IntegrityPassed bool         `json:"integrityPassed"`
}

type workloadItem struct {
	key         inbox.Key
	fingerprint inbox.Fingerprint
}

// Run executes the isolated real-PostgreSQL ProcessAttempt matrix.
func Run(ctx context.Context, config Config, log *slog.Logger) (report RunReport, runErr error) {
	if log == nil {
		log = slog.Default()
	}
	artifacts, err := newArtifacts(config.ResultDir())
	if err != nil {
		return RunReport{}, err
	}
	report = RunReport{
		SpecVersion: reportSpecVersion, RunID: config.RunID, StartedAt: time.Now().UTC(),
		Config: ReportConfig{
			Warmup: config.Warmup, Operations: config.Operations,
			Concurrencies: append([]int(nil), config.Concurrencies...), Repetitions: config.Repetitions,
		},
		Cases: make([]CaseReport, 0, len(config.Concurrencies)*config.Repetitions), IntegrityPassed: true,
	}

	readyCtx, cancel := context.WithTimeout(ctx, config.ReadyTimeout)
	defer cancel()
	workloadDB, probeDB, err := openDatabases(readyCtx, config)
	if err != nil {
		return report, err
	}
	defer func() { runErr = errors.Join(runErr, workloadDB.Close(), probeDB.Close()) }()
	if err := prepareSchema(readyCtx, workloadDB); err != nil {
		return report, err
	}
	store, err := inboxpgsql.New(workloadDB,
		inboxpgsql.WithSchema("inbox_capacity"), inboxpgsql.WithTablePrefix("gm_"))
	if err != nil {
		return report, fmt.Errorf("create PostgreSQL Inbox benchmark store: %w", err)
	}
	telemetry, err := pgtelemetry.New(probeDB)
	if err != nil {
		return report, err
	}
	settings, err := telemetry.Ensure(readyCtx)
	if err != nil {
		return report, err
	}
	var postgresVersion string
	if err := probeDB.QueryRowContext(readyCtx,
		`/* gomessenger-capacity-probe */ SHOW server_version`).Scan(&postgresVersion); err != nil {
		return report, fmt.Errorf("read PostgreSQL version: %w", err)
	}
	report.Environment = Environment{
		GoVersion: runtime.Version(), ContainerOS: runtime.GOOS, ContainerArch: runtime.GOARCH,
		ContainerCPUs: runtime.NumCPU(), HostOS: config.HostOS, HostArch: config.HostArch,
		HostCPUs: config.HostCPUs, GitCommit: config.GitCommit, GitDirty: config.GitDirty,
		PostgreSQLVersion: postgresVersion, PostgreSQLSettings: settings, DBMaxOpenConns: config.DBMaxOpenConns,
	}
	if err := artifacts.writeEnvironment(report.Environment); err != nil {
		return report, err
	}

	for _, concurrency := range config.Concurrencies {
		for repetition := 1; repetition <= config.Repetitions; repetition++ {
			caseReport, err := runCase(ctx, workloadDB, store, telemetry, config, concurrency, repetition)
			if err != nil {
				return report, err
			}
			report.Cases = append(report.Cases, caseReport)
			if !caseReport.Integrity.Passed {
				report.IntegrityPassed = false
			}
			report.CompletedAt = time.Now().UTC()
			if err := artifacts.writeReport(report); err != nil {
				return report, err
			}
			log.Info("PostgreSQL Inbox capacity case complete",
				"concurrency", concurrency, "repetition", repetition,
				"throughput_per_second", caseReport.ThroughputPerSec,
				"p95_millis", caseReport.Latency.P95Millis,
				"integrity", caseReport.Integrity.Passed,
			)
			if !caseReport.Integrity.Passed {
				return report, fmt.Errorf("PostgreSQL Inbox capacity integrity failed for C%d repetition %d",
					concurrency, repetition)
			}
		}
	}
	report.CompletedAt = time.Now().UTC()
	if err := artifacts.writeReport(report); err != nil {
		return report, err
	}
	return report, nil
}

func runCase(
	ctx context.Context,
	db *sql.DB,
	store *inbox.Store,
	telemetry *pgtelemetry.Snapshotter,
	config Config,
	concurrency int,
	repetition int,
) (CaseReport, error) {
	if err := resetFixtures(ctx, db); err != nil {
		return CaseReport{}, err
	}
	warmup, err := prepareItems(config.Warmup, concurrency, repetition, "warmup")
	if err != nil {
		return CaseReport{}, err
	}
	if _, _, err := executeWorkload(ctx, store, warmup, concurrency); err != nil {
		return CaseReport{}, fmt.Errorf("warm up ProcessAttempt C%d repetition %d: %w", concurrency, repetition, err)
	}
	if err := resetFixtures(ctx, db); err != nil {
		return CaseReport{}, err
	}
	items, err := prepareItems(config.Operations, concurrency, repetition, "measured")
	if err != nil {
		return CaseReport{}, err
	}
	before, err := telemetry.Snapshot(ctx)
	if err != nil {
		return CaseReport{}, err
	}
	durations, elapsed, err := executeWorkload(ctx, store, items, concurrency)
	if err != nil {
		return CaseReport{}, fmt.Errorf("measure ProcessAttempt C%d repetition %d: %w", concurrency, repetition, err)
	}
	loadEnd, err := telemetry.Snapshot(ctx)
	if err != nil {
		return CaseReport{}, err
	}
	integrity, err := readIntegrity(ctx, db, int64(config.Operations))
	if err != nil {
		return CaseReport{}, err
	}
	return CaseReport{
		Concurrency: concurrency, Repetition: repetition, Operations: config.Operations,
		DurationSeconds: elapsed.Seconds(), ThroughputPerSec: float64(config.Operations) / elapsed.Seconds(),
		Latency: latencyStats(durations), Integrity: integrity,
		PostgreSQL: pgtelemetry.BuildTimeline(before, loadEnd, loadEnd),
	}, nil
}

func executeWorkload(
	ctx context.Context,
	store *inbox.Store,
	items []workloadItem,
	concurrency int,
) ([]time.Duration, time.Duration, error) {
	durations := make([]time.Duration, len(items))
	errorsByIndex := make([]error, len(items))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	startedAt := time.Now()
	for range concurrency {
		go func() {
			defer workers.Done()
			for index := range jobs {
				item := items[index]
				operationStartedAt := time.Now()
				result, err := store.ProcessAttempt(ctx, item.key, item.fingerprint, 3, func(handlerCtx context.Context) error {
					tx, ok := inbox.SQLTxFromContext(handlerCtx)
					if !ok {
						return errors.New("PostgreSQL Inbox benchmark handler has no transaction")
					}
					_, execErr := tx.ExecContext(handlerCtx, `INSERT INTO inbox_capacity.business_effects
						(message_id, committed_at) VALUES ($1, clock_timestamp())`, item.key.MessageID.String())
					if execErr != nil {
						return fmt.Errorf("insert benchmark business effect: %w", execErr)
					}
					return nil
				})
				durations[index] = time.Since(operationStartedAt)
				if err != nil {
					errorsByIndex[index] = err
				} else if result.Duplicate || result.Attempt != 1 {
					errorsByIndex[index] = fmt.Errorf("unexpected ProcessAttempt result: %#v", result)
				}
			}
		}()
	}
	for index := range items {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, 0, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	elapsed := time.Since(startedAt)
	var joined error
	for _, err := range errorsByIndex {
		joined = errors.Join(joined, err)
	}
	return durations, elapsed, joined
}

func prepareItems(count, concurrency, repetition int, phase string) ([]workloadItem, error) {
	generator := messenger.UUIDv7Generator()
	items := make([]workloadItem, count)
	for index := range items {
		messageID, err := generator.New()
		if err != nil {
			return nil, fmt.Errorf("generate benchmark message ID: %w", err)
		}
		fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d/%d", phase, concurrency, repetition, index)))
		items[index] = workloadItem{
			key: inbox.Key{
				ConsumerID: fmt.Sprintf("inbox-capacity-c%d-r%d", concurrency, repetition),
				Source:     "urn:gomessenger:inbox-capacity", MessageID: messageID,
			},
			fingerprint: inbox.Fingerprint(fingerprint),
		}
	}
	return items, nil
}

func openDatabases(ctx context.Context, config Config) (
	workloadDB *sql.DB,
	probeDB *sql.DB,
	err error,
) {
	workloadDSN, err := withApplicationName(config.PostgresDSN, "gomessenger-inbox-benchmark")
	if err != nil {
		return nil, nil, err
	}
	probeDSN, err := pgtelemetry.ProbeDSN(config.PostgresDSN)
	if err != nil {
		return nil, nil, err
	}
	workloadDB, err = waitForDatabase(ctx, workloadDSN)
	if err != nil {
		return nil, nil, err
	}
	workloadDB.SetMaxOpenConns(config.DBMaxOpenConns)
	workloadDB.SetMaxIdleConns(config.DBMaxOpenConns)
	probeDB, err = waitForDatabase(ctx, probeDSN)
	if err != nil {
		_ = workloadDB.Close()
		return nil, nil, err
	}
	return workloadDB, probeDB, nil
}

func waitForDatabase(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL Inbox capacity database: %w", err)
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		lastErr = db.PingContext(probeCtx)
		cancel()
		if lastErr == nil {
			return db, nil
		}
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, fmt.Errorf("wait for PostgreSQL Inbox capacity database: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func prepareSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS inbox_capacity`); err != nil {
		return fmt.Errorf("create Inbox capacity schema: %w", err)
	}
	if err := inboxpgsql.Migrate(ctx, db,
		inboxpgsql.WithSchema("inbox_capacity"), inboxpgsql.WithTablePrefix("gm_")); err != nil {
		return fmt.Errorf("migrate Inbox capacity schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS inbox_capacity.business_effects (
		message_id UUID PRIMARY KEY,
		committed_at TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return fmt.Errorf("create Inbox capacity business table: %w", err)
	}
	return nil
}

func resetFixtures(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `TRUNCATE
		inbox_capacity.gm_inbox_attempt_generations,
		inbox_capacity.gm_inbox_attempts,
		inbox_capacity.gm_inbox,
		inbox_capacity.business_effects`); err != nil {
		return fmt.Errorf("reset Inbox capacity fixtures: %w", err)
	}
	return nil
}

func readIntegrity(ctx context.Context, db *sql.DB, expected int64) (IntegrityResult, error) {
	result := IntegrityResult{Expected: expected}
	err := db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT
		(SELECT COUNT(*) FROM inbox_capacity.business_effects),
		(SELECT COUNT(DISTINCT message_id) FROM inbox_capacity.business_effects),
		(SELECT COUNT(*) FROM inbox_capacity.gm_inbox WHERE completed_at IS NOT NULL),
		(SELECT COUNT(*) FROM inbox_capacity.gm_inbox_attempts)`).Scan(
		&result.BusinessEffects, &result.DistinctMessages, &result.CompletedIdentities, &result.AttemptRows,
	)
	if err != nil {
		return IntegrityResult{}, fmt.Errorf("reconcile Inbox capacity case: %w", err)
	}
	result.Passed = result.BusinessEffects == expected && result.DistinctMessages == expected &&
		result.CompletedIdentities == expected && result.AttemptRows == expected
	return result, nil
}

func latencyStats(values []time.Duration) LatencyStats {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return LatencyStats{
		P50Millis: percentileMillis(sorted, 0.50),
		P95Millis: percentileMillis(sorted, 0.95),
		P99Millis: percentileMillis(sorted, 0.99),
	}
}

func percentileMillis(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	position := percentile * float64(len(values)-1)
	lower := int(position)
	upper := min(lower+1, len(values)-1)
	fraction := position - float64(lower)
	value := float64(values[lower]) + (float64(values[upper])-float64(values[lower]))*fraction
	return value / float64(time.Millisecond)
}
