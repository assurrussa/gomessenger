package capacity

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"

	// Register the pgx database/sql driver used by the independent verifier.
	_ "github.com/jackc/pgx/v5/stdlib"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

type probe struct {
	db               *sql.DB
	nats             *natsio.Conn
	stream           jetstream.Stream
	dlq              jetstream.Stream
	consumer         jetstream.Consumer
	appURL           string
	http             *http.Client
	postgres         *pgtelemetry.Snapshotter
	postgresSettings map[string]string
}

func openProbe(ctx context.Context, config Config) (*probe, error) {
	probeDSN, err := pgtelemetry.ProbeDSN(config.PostgresDSN)
	if err != nil {
		return nil, err
	}
	db, err := waitProbePostgres(ctx, probeDSN)
	if err != nil {
		return nil, err
	}
	connection, err := waitProbeNATS(ctx, config.NATSURL)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	result := &probe{
		db: db, nats: connection, appURL: config.AppURL,
		http: &http.Client{Timeout: 3 * time.Second},
	}
	result.postgres, err = pgtelemetry.New(db)
	if err != nil {
		_ = result.close()
		return nil, err
	}
	result.postgresSettings, err = result.postgres.Ensure(ctx)
	if err != nil {
		_ = result.close()
		return nil, err
	}
	return result, nil
}

func (p *probe) close() error {
	if p.nats != nil {
		p.nats.Close()
	}
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *probe) waitReady(ctx context.Context) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		ready, err := p.ready(ctx)
		if ready {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for capacity application readiness: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func (p *probe) ready(ctx context.Context) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.appURL+"/healthz", nil)
	if err != nil {
		return false, fmt.Errorf("build readiness request: %w", err)
	}
	response, err := p.http.Do(request)
	if err != nil {
		return false, err
	}
	body, copyErr := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	closeErr := response.Body.Close()
	if copyErr != nil || closeErr != nil {
		return false, errors.Join(copyErr, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf(
			"readiness returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)),
		)
	}
	if err := p.resolveJetStream(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (p *probe) resolveJetStream(ctx context.Context) error {
	js, err := jetstream.New(p.nats)
	if err != nil {
		return fmt.Errorf("create JetStream probe: %w", err)
	}
	p.stream, err = js.Stream(ctx, demo.Stream)
	if err != nil {
		return fmt.Errorf("open capacity stream: %w", err)
	}
	p.dlq, err = js.Stream(ctx, demo.DLQStream)
	if err != nil {
		return fmt.Errorf("open capacity DLQ stream: %w", err)
	}
	p.consumer, err = js.Consumer(ctx, demo.Stream, demo.ConsumerID)
	if err != nil {
		return fmt.Errorf("open capacity consumer: %w", err)
	}
	return nil
}

func (p *probe) snapshot(
	ctx context.Context,
	labels demo.BenchmarkLabels,
	phase string,
	elapsed time.Duration,
) (Sample, error) {
	business, err := p.businessSnapshot(ctx, labels)
	if err != nil {
		return Sample{}, err
	}
	broker, err := p.brokerSnapshot(ctx)
	if err != nil {
		return Sample{}, err
	}
	application, err := p.applicationStats(ctx)
	if err != nil {
		return Sample{}, err
	}
	waits, err := p.postgres.Waits(ctx)
	if err != nil {
		return Sample{}, err
	}
	return Sample{
		ObservedAt: time.Now().UTC(), RunID: labels.RunID, StageID: labels.StageID,
		Phase: phase, ElapsedSeconds: elapsed.Seconds(),
		Business: business, Broker: broker, Application: application, PostgreSQLWaits: waits,
	}, nil
}

func (p *probe) businessSnapshot(
	ctx context.Context,
	labels demo.BenchmarkLabels,
) (BusinessSnapshot, error) {
	var result BusinessSnapshot
	err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT
		(SELECT COUNT(*) FROM demo.orders WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(*) FROM demo.envelope_measurements WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(*) FROM demo.envelope_measurements
			WHERE run_id = $1 AND stage_id = $2 AND published_at IS NOT NULL),
		(SELECT COUNT(DISTINCT message_id) FROM demo.order_projection
			WHERE run_id = $1 AND stage_id = $2),
		(SELECT COALESCE(SUM(measurement.envelope_bytes), 0)
			FROM demo.order_projection projection
			JOIN demo.envelope_measurements measurement USING (message_id)
			WHERE projection.run_id = $1 AND projection.stage_id = $2)`,
		labels.RunID, labels.StageID,
	).Scan(
		&result.Accepted, &result.Staged, &result.Published, &result.Committed, &result.CommittedBytes,
	)
	if err != nil {
		return BusinessSnapshot{}, fmt.Errorf("read business snapshot for %s: %w", labels.StageID, err)
	}
	return result, nil
}

func (p *probe) loadWindowBusinessSnapshot(
	ctx context.Context,
	labels demo.BenchmarkLabels,
	duration time.Duration,
) (BusinessSnapshot, time.Time, error) {
	startedAt, err := p.loadWindowStartedAt(ctx, labels)
	if err != nil || startedAt.IsZero() {
		return BusinessSnapshot{}, startedAt, err
	}
	endedAt := startedAt.Add(duration)
	var result BusinessSnapshot
	err = p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT
		(SELECT COUNT(*) FROM demo.orders
			WHERE run_id = $1 AND stage_id = $2 AND accepted_at < $3),
		(SELECT COUNT(*) FROM demo.envelope_measurements
			WHERE run_id = $1 AND stage_id = $2 AND staged_at < $3),
		(SELECT COUNT(*) FROM demo.envelope_measurements
			WHERE run_id = $1 AND stage_id = $2 AND published_at < $3),
		(SELECT COUNT(DISTINCT message_id) FROM demo.order_projection
			WHERE run_id = $1 AND stage_id = $2 AND handled_at < $3),
		(SELECT COALESCE(SUM(measurement.envelope_bytes), 0)
			FROM demo.order_projection projection
			JOIN demo.envelope_measurements measurement USING (message_id)
			WHERE projection.run_id = $1 AND projection.stage_id = $2 AND projection.handled_at < $3)`,
		labels.RunID, labels.StageID, endedAt,
	).Scan(
		&result.Accepted, &result.Staged, &result.Published, &result.Committed, &result.CommittedBytes,
	)
	if err != nil {
		return BusinessSnapshot{}, time.Time{}, fmt.Errorf("read exact load-window snapshot: %w", err)
	}
	return result, startedAt, nil
}

func (p *probe) loadWindowStartedAt(
	ctx context.Context,
	labels demo.BenchmarkLabels,
) (time.Time, error) {
	var offeredAt sql.NullTime
	if err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT MIN(offered_at)
		FROM demo.orders WHERE run_id = $1 AND stage_id = $2`,
		labels.RunID, labels.StageID,
	).Scan(&offeredAt); err != nil {
		return time.Time{}, fmt.Errorf("read load-window start: %w", err)
	}
	if !offeredAt.Valid {
		return time.Time{}, nil
	}
	return offeredAt.Time.UTC(), nil
}

func (p *probe) brokerSnapshot(ctx context.Context) (BrokerSnapshot, error) {
	streamInfo, err := p.stream.Info(ctx)
	if err != nil {
		return BrokerSnapshot{}, fmt.Errorf("read capacity stream state: %w", err)
	}
	dlqInfo, err := p.dlq.Info(ctx)
	if err != nil {
		return BrokerSnapshot{}, fmt.Errorf("read capacity DLQ state: %w", err)
	}
	consumerInfo, err := p.consumer.Info(ctx)
	if err != nil {
		return BrokerSnapshot{}, fmt.Errorf("read capacity consumer state: %w", err)
	}
	return BrokerSnapshot{
		StreamMessages:  streamInfo.State.Msgs,
		StreamBytes:     streamInfo.State.Bytes,
		ConsumerPending: consumerInfo.NumPending,
		AckPending:      consumerInfo.NumAckPending,
		Redelivered:     consumerInfo.NumRedelivered,
		DLQMessages:     dlqInfo.State.Msgs,
	}, nil
}

func (p *probe) applicationStats(ctx context.Context, labels ...demo.BenchmarkLabels) (demo.AppStats, error) {
	endpoint, err := url.Parse(p.appURL + "/benchmark/stats")
	if err != nil {
		return demo.AppStats{}, fmt.Errorf("parse application stats URL: %w", err)
	}
	if len(labels) != 0 && labels[0] != (demo.BenchmarkLabels{}) {
		query := endpoint.Query()
		query.Set("runId", labels[0].RunID)
		query.Set("stageId", labels[0].StageID)
		endpoint.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return demo.AppStats{}, fmt.Errorf("build application stats request: %w", err)
	}
	response, err := p.http.Do(request)
	if err != nil {
		return demo.AppStats{}, fmt.Errorf("read application stats: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return demo.AppStats{}, fmt.Errorf("application stats returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(body)))
	}
	var result demo.AppStats
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return demo.AppStats{}, fmt.Errorf("decode application stats: %w", err)
	}
	return result, nil
}

func (p *probe) flushPublications(ctx context.Context) error {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.appURL+"/benchmark/publications/flush",
		nil,
	)
	if err != nil {
		return fmt.Errorf("build publication flush request: %w", err)
	}
	response, err := p.http.Do(request)
	if err != nil {
		return fmt.Errorf("flush publication recorder: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return fmt.Errorf(
			"flush publication recorder returned HTTP %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}
	return nil
}

func (p *probe) latencyStats(
	ctx context.Context,
	labels demo.BenchmarkLabels,
) (LatencyStats, error) {
	var p50, p95, p99 sql.NullFloat64
	err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT
		percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (projection.handled_at - business.offered_at))),
		percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (projection.handled_at - business.offered_at))),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (projection.handled_at - business.offered_at)))
		FROM demo.order_projection projection
		JOIN demo.orders business USING (message_id)
		WHERE projection.run_id = $1 AND projection.stage_id = $2`,
		labels.RunID, labels.StageID,
	).Scan(&p50, &p95, &p99)
	if err != nil {
		return LatencyStats{}, fmt.Errorf("read stage latency percentiles: %w", err)
	}
	return LatencyStats{
		P50Millis: secondsToMillis(p50),
		P95Millis: secondsToMillis(p95),
		P99Millis: secondsToMillis(p99),
	}, nil
}

func (p *probe) envelopeStats(
	ctx context.Context,
	labels demo.BenchmarkLabels,
) (EnvelopeStats, error) {
	var result EnvelopeStats
	var p50, p95 sql.NullFloat64
	err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT COUNT(*), COALESCE(SUM(envelope_bytes), 0),
		percentile_cont(0.50) WITHIN GROUP (ORDER BY envelope_bytes),
		percentile_cont(0.95) WITHIN GROUP (ORDER BY envelope_bytes),
		COALESCE(MAX(envelope_bytes), 0)
		FROM demo.envelope_measurements WHERE run_id = $1 AND stage_id = $2`,
		labels.RunID, labels.StageID,
	).Scan(&result.Count, &result.TotalBytes, &p50, &p95, &result.MaxBytes)
	if err != nil {
		return EnvelopeStats{}, fmt.Errorf("read envelope size percentiles: %w", err)
	}
	if p50.Valid {
		result.P50Bytes = p50.Float64
	}
	if p95.Valid {
		result.P95Bytes = p95.Float64
	}
	return result, nil
}

func (p *probe) integrity(
	ctx context.Context,
	labels demo.BenchmarkLabels,
) (IntegrityResult, error) {
	var result IntegrityResult
	err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SELECT
		(SELECT COUNT(*) FROM demo.orders WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(DISTINCT message_id) FROM demo.orders WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(*) FROM demo.envelope_measurements WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(*) FROM demo.envelope_measurements
			WHERE run_id = $1 AND stage_id = $2 AND published_at IS NOT NULL),
		(SELECT COUNT(*) FROM demo.order_projection WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(DISTINCT message_id) FROM demo.order_projection WHERE run_id = $1 AND stage_id = $2),
		(SELECT COUNT(*) FROM demo.envelope_measurements
			WHERE run_id = $1 AND stage_id = $2
			  AND (envelope_bytes <= 0 OR envelope_sha256 !~ '^[0-9a-f]{64}$')),
		(SELECT COUNT(*) FROM demo.envelope_measurements measurement
			LEFT JOIN demo.orders business USING (message_id)
			WHERE measurement.run_id = $1 AND measurement.stage_id = $2 AND business.message_id IS NULL),
		(SELECT COUNT(*) FROM demo.envelope_measurements measurement
			LEFT JOIN demo.order_projection projection USING (message_id)
			WHERE measurement.run_id = $1 AND measurement.stage_id = $2 AND projection.message_id IS NULL)`,
		labels.RunID, labels.StageID,
	).Scan(
		&result.Orders,
		&result.DistinctOrderMessages,
		&result.Measurements,
		&result.BrokerConfirmed,
		&result.Projections,
		&result.DistinctProjectionMsgs,
		&result.InvalidMeasurements,
		&result.MissingOrderLinks,
		&result.MissingProjectionLinks,
	)
	if err != nil {
		return IntegrityResult{}, fmt.Errorf("reconcile stage integrity: %w", err)
	}
	return result, nil
}

func (p *probe) environment(ctx context.Context, config Config) (Environment, error) {
	var postgresVersion string
	if err := p.db.QueryRowContext(ctx, `/* gomessenger-capacity-probe */ SHOW server_version`).Scan(&postgresVersion); err != nil {
		return Environment{}, fmt.Errorf("read PostgreSQL version: %w", err)
	}
	k6Version := "unknown"
	versionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// The binary is an explicit local capacity-runner setting, never HTTP input.
	//nolint:gosec // Running that configured executable is the controller's purpose.
	if output, err := exec.CommandContext(versionCtx, config.K6Binary, "version").CombinedOutput(); err == nil {
		k6Version = strings.TrimSpace(string(output))
	}
	return Environment{
		GoVersion: runtime.Version(), ContainerOS: runtime.GOOS, ContainerArch: runtime.GOARCH,
		OutboxVersion: outboxModuleVersion(),
		ContainerCPUs: runtime.NumCPU(), HostOS: config.HostOS, HostArch: config.HostArch,
		HostCPUs: config.HostCPUs, GitCommit: config.GitCommit, GitDirty: config.GitDirty,
		PostgreSQLVersion: postgresVersion, NATSServerVersion: p.nats.ConnectedServerVersion(),
		K6Version: k6Version, OutboxWorkers: config.OutboxWorkers,
		OutboxReservationBatchSize: config.OutboxReservationBatchSize,
		OutboxProducerMaxConns:     config.OutboxProducerMaxConns,
		OutboxRelayMaxConns:        config.OutboxRelayMaxConns,
		OutboxPGXConnectionBudget:  config.OutboxProducerMaxConns + config.OutboxRelayMaxConns,
		ConsumerConcurrency:        config.ConsumerConcurrency, DBMaxOpenConns: config.DBMaxOpenConns,
		JetStreamStorage: "file", PostgreSQLSettings: p.postgresSettings,
	}, nil
}

func (p *probe) postgresSnapshot(ctx context.Context) (pgtelemetry.Snapshot, error) {
	snapshot, err := p.postgres.Snapshot(ctx)
	if err != nil {
		return pgtelemetry.Snapshot{}, fmt.Errorf("capture PostgreSQL telemetry: %w", err)
	}
	return snapshot, nil
}

func secondsToMillis(value sql.NullFloat64) float64 {
	if !value.Valid {
		return 0
	}
	return value.Float64 * 1_000
}

func waitProbePostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open capacity PostgreSQL probe: %w", err)
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
			return nil, fmt.Errorf("wait for capacity PostgreSQL probe: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func waitProbeNATS(ctx context.Context, url string) (*natsio.Conn, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		connection, err := natsio.Connect(url,
			natsio.Name("gomessenger-capacity-runner"), natsio.Timeout(time.Second),
		)
		if err == nil {
			return connection, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for capacity NATS probe: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}
