// Package pgtelemetry captures version-tolerant PostgreSQL capacity snapshots.
package pgtelemetry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	// ProbeApplicationName identifies controller-owned sessions and wait samples.
	ProbeApplicationName = "gomessenger-capacity-probe"
	probeMarker          = "gomessenger-capacity-probe"
)

// Counters is a version-tolerant set of numeric PostgreSQL statistics.
type Counters map[string]float64

// Statement is one normalized pg_stat_statements row.
type Statement struct {
	QueryID             string  `json:"queryId"`
	Query               string  `json:"query"`
	Classification      string  `json:"classification"`
	Calls               int64   `json:"calls"`
	TotalExecTimeMillis float64 `json:"totalExecTimeMillis"`
	Rows                int64   `json:"rows"`
	WALRecords          int64   `json:"walRecords"`
	WALFullPageImages   int64   `json:"walFullPageImages"`
	WALBytes            float64 `json:"walBytes"`
	SharedBlocksHit     int64   `json:"sharedBlocksHit"`
	SharedBlocksRead    int64   `json:"sharedBlocksRead"`
	SharedBlocksDirtied int64   `json:"sharedBlocksDirtied"`
	SharedBlocksWritten int64   `json:"sharedBlocksWritten"`
}

// IOCounters is one pg_stat_io backend/object/context row.
type IOCounters struct {
	BackendType string   `json:"backendType"`
	Object      string   `json:"object"`
	Context     string   `json:"context"`
	Counters    Counters `json:"counters"`
}

// WaitEvent is a sampled active PostgreSQL wait grouped by application and type.
type WaitEvent struct {
	ApplicationName string `json:"applicationName"`
	BackendType     string `json:"backendType"`
	State           string `json:"state"`
	WaitEventType   string `json:"waitEventType"`
	WaitEvent       string `json:"waitEvent"`
	Sessions        int64  `json:"sessions"`
}

// Snapshot is one cumulative PostgreSQL statistics boundary.
type Snapshot struct {
	ObservedAt   time.Time    `json:"observedAt"`
	Statements   []Statement  `json:"statements"`
	Database     Counters     `json:"database"`
	WAL          Counters     `json:"wal"`
	Checkpointer Counters     `json:"checkpointer"`
	IO           []IOCounters `json:"io"`
	Waits        []WaitEvent  `json:"waits"`
}

// Delta is the numeric difference between two cumulative snapshots.
type Delta struct {
	Statements   []Statement  `json:"statements"`
	Database     Counters     `json:"database"`
	WAL          Counters     `json:"wal"`
	Checkpointer Counters     `json:"checkpointer"`
	IO           []IOCounters `json:"io"`
}

// Timeline stores the three required capacity boundaries and their deltas.
type Timeline struct {
	Before     Snapshot `json:"before"`
	LoadEnd    Snapshot `json:"loadEnd"`
	AfterDrain Snapshot `json:"afterDrain"`
	LoadDelta  Delta    `json:"loadDelta"`
	DrainDelta Delta    `json:"drainDelta"`
}

// Snapshotter reads PostgreSQL statistics through a probe-tagged database pool.
type Snapshotter struct{ db *sql.DB }

// New returns a PostgreSQL telemetry reader.
func New(db *sql.DB) (*Snapshotter, error) {
	if db == nil {
		return nil, errors.New("PostgreSQL telemetry requires a database")
	}
	return &Snapshotter{db: db}, nil
}

// ProbeDSN adds the stable application_name used to exclude controller waits
// and keeps verifier queries serial so measurement cannot consume SUT parallel
// workers or Docker shared memory.
func ProbeDSN(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse PostgreSQL probe DSN: %w", err)
	}
	query := parsed.Query()
	query.Set("application_name", ProbeApplicationName)
	query.Set("max_parallel_workers_per_gather", "0")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// Ensure installs pg_stat_statements and verifies the isolated stack settings.
func (s *Snapshotter) Ensure(ctx context.Context) (map[string]string, error) {
	if _, err := s.db.ExecContext(ctx, `/* gomessenger-capacity-probe */
		CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		return nil, fmt.Errorf("install pg_stat_statements: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `/* gomessenger-capacity-probe */
		SELECT name, setting FROM pg_settings WHERE name = ANY($1::text[])
		ORDER BY name`, []string{
		"compute_query_id", "pg_stat_statements.track", "pg_stat_statements.track_utility",
		"shared_preload_libraries", "track_io_timing", "track_wal_io_timing",
		"checkpoint_timeout", "max_wal_size", "shared_buffers", "wal_buffers",
		"max_parallel_workers_per_gather",
		"fsync", "synchronous_commit", "full_page_writes",
	})
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL telemetry settings: %w", err)
	}
	defer rows.Close()
	settings := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL telemetry setting: %w", err)
		}
		settings[name] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL telemetry settings: %w", err)
	}
	required := map[string]string{
		"compute_query_id": "on", "pg_stat_statements.track": "all",
		"pg_stat_statements.track_utility": "on", "track_io_timing": "on", "track_wal_io_timing": "on",
		"fsync": "on", "synchronous_commit": "on", "full_page_writes": "on",
	}
	for name, want := range required {
		if settings[name] != want {
			return nil, fmt.Errorf("PostgreSQL setting %s=%q, want %q", name, settings[name], want)
		}
	}
	if !strings.Contains(settings["shared_preload_libraries"], "pg_stat_statements") {
		return nil, errors.New("shared_preload_libraries does not include pg_stat_statements")
	}
	return settings, nil
}

// Snapshot captures statements, database, WAL, I/O, and current relevant waits.
func (s *Snapshotter) Snapshot(ctx context.Context) (Snapshot, error) {
	statements, err := s.statements(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	database, err := s.singleCounters(ctx, `/* gomessenger-capacity-probe */
		SELECT to_jsonb(database_stats) - 'stats_reset'
		FROM pg_stat_database AS database_stats
		WHERE datname = current_database()`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot pg_stat_database: %w", err)
	}
	wal, err := s.singleCounters(ctx, `/* gomessenger-capacity-probe */
		SELECT to_jsonb(wal_stats) - 'stats_reset' FROM pg_stat_wal AS wal_stats`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot pg_stat_wal: %w", err)
	}
	checkpointer, err := s.singleCounters(ctx, `/* gomessenger-capacity-probe */
		SELECT to_jsonb(checkpointer_stats) - 'stats_reset'
		FROM pg_stat_checkpointer AS checkpointer_stats`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot pg_stat_checkpointer: %w", err)
	}
	ioCounters, err := s.io(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	waits, err := s.Waits(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		ObservedAt: time.Now().UTC(), Statements: statements, Database: database,
		WAL: wal, Checkpointer: checkpointer, IO: ioCounters, Waits: waits,
	}, nil
}

// Waits samples lock, I/O, buffer-pin, and WAL waits while excluding the probe.
func (s *Snapshotter) Waits(ctx context.Context) ([]WaitEvent, error) {
	rows, err := s.db.QueryContext(ctx, `/* gomessenger-capacity-probe */
		SELECT COALESCE(application_name, ''), COALESCE(backend_type, ''), COALESCE(state, ''),
		       COALESCE(wait_event_type, ''), COALESCE(wait_event, ''), COUNT(*)
		FROM pg_stat_activity
		WHERE pid <> pg_backend_pid()
		  AND application_name <> $1
		  AND wait_event IS NOT NULL
		  AND (wait_event_type IN ('Lock', 'LWLock', 'IO', 'BufferPin') OR wait_event IN ('WALWrite', 'WALSync'))
		GROUP BY application_name, backend_type, state, wait_event_type, wait_event
		ORDER BY application_name, backend_type, wait_event_type, wait_event`, ProbeApplicationName)
	if err != nil {
		return nil, fmt.Errorf("sample PostgreSQL waits: %w", err)
	}
	defer rows.Close()
	result := make([]WaitEvent, 0)
	for rows.Next() {
		var item WaitEvent
		if err := rows.Scan(
			&item.ApplicationName, &item.BackendType, &item.State,
			&item.WaitEventType, &item.WaitEvent, &item.Sessions,
		); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL wait: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL waits: %w", err)
	}
	return result, nil
}

func (s *Snapshotter) statements(ctx context.Context) ([]Statement, error) {
	rows, err := s.db.QueryContext(ctx, `/* gomessenger-capacity-probe */
		SELECT queryid::text, query, calls, total_exec_time, rows,
		       wal_records, wal_fpi, wal_bytes::double precision,
		       shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY queryid, query`)
	if err != nil {
		return nil, fmt.Errorf("snapshot pg_stat_statements: %w", err)
	}
	defer rows.Close()
	result := make([]Statement, 0)
	for rows.Next() {
		var item Statement
		if err := rows.Scan(
			&item.QueryID, &item.Query, &item.Calls, &item.TotalExecTimeMillis, &item.Rows,
			&item.WALRecords, &item.WALFullPageImages, &item.WALBytes,
			&item.SharedBlocksHit, &item.SharedBlocksRead, &item.SharedBlocksDirtied, &item.SharedBlocksWritten,
		); err != nil {
			return nil, fmt.Errorf("scan pg_stat_statements: %w", err)
		}
		item.Query = normalizeSQL(item.Query)
		if strings.Contains(strings.ToLower(item.Query), probeMarker) {
			continue
		}
		item.Classification = ClassifyStatement(item.Query)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pg_stat_statements: %w", err)
	}
	return result, nil
}

func (s *Snapshotter) singleCounters(ctx context.Context, query string) (Counters, error) {
	var raw []byte
	if err := s.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, err
	}
	return decodeCounters(raw)
}

func (s *Snapshotter) io(ctx context.Context) ([]IOCounters, error) {
	rows, err := s.db.QueryContext(ctx, `/* gomessenger-capacity-probe */
		SELECT backend_type, object, context,
		       to_jsonb(io_stats) - ARRAY['backend_type', 'object', 'context', 'stats_reset']::text[]
		FROM pg_stat_io AS io_stats
		ORDER BY backend_type, object, context`)
	if err != nil {
		return nil, fmt.Errorf("snapshot pg_stat_io: %w", err)
	}
	defer rows.Close()
	result := make([]IOCounters, 0)
	for rows.Next() {
		var item IOCounters
		var raw []byte
		if err := rows.Scan(&item.BackendType, &item.Object, &item.Context, &raw); err != nil {
			return nil, fmt.Errorf("scan pg_stat_io: %w", err)
		}
		counters, err := decodeCounters(raw)
		if err != nil {
			return nil, fmt.Errorf("decode pg_stat_io: %w", err)
		}
		item.Counters = counters
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pg_stat_io: %w", err)
	}
	return result, nil
}

// BuildTimeline computes load and drain deltas for three ordered snapshots.
func BuildTimeline(before, loadEnd, afterDrain Snapshot) Timeline {
	return Timeline{
		Before: before, LoadEnd: loadEnd, AfterDrain: afterDrain,
		LoadDelta: DeltaSnapshots(before, loadEnd), DrainDelta: DeltaSnapshots(loadEnd, afterDrain),
	}
}

// DeltaSnapshots subtracts cumulative counters without allowing negative reset artifacts.
func DeltaSnapshots(before, after Snapshot) Delta {
	return Delta{
		Statements:   deltaStatements(before.Statements, after.Statements),
		Database:     deltaCounters(before.Database, after.Database),
		WAL:          deltaCounters(before.WAL, after.WAL),
		Checkpointer: deltaCounters(before.Checkpointer, after.Checkpointer),
		IO:           deltaIO(before.IO, after.IO),
	}
}

// ClassifyStatement assigns stable report buckets without relying on query IDs.
func ClassifyStatement(query string) string {
	normalized := strings.ToLower(query)
	switch {
	case strings.Contains(normalized, probeMarker):
		return "probe"
	case strings.Contains(normalized, "gomessenger_handler"),
		strings.Contains(normalized, "savepoint $1"),
		strings.Contains(normalized, "_inbox_attempt"),
		strings.Contains(normalized, "_inbox "),
		strings.Contains(normalized, "_inbox\n"),
		strings.Contains(normalized, ".gm_inbox"),
		strings.Contains(normalized, `"gm_inbox"`):
		return "inbox"
	case strings.Contains(normalized, "demo.orders"),
		strings.Contains(normalized, "demo.order_projection"),
		strings.Contains(normalized, "inbox_capacity.business_effects"),
		strings.Contains(normalized, "batch_capacity.business_effects"):
		return "business"
	case strings.Contains(normalized, "envelope_measurements"):
		return "measurement"
	case strings.Contains(normalized, " jobs "), strings.Contains(normalized, "outbox"):
		return "outbox"
	default:
		return "other"
	}
}

func decodeCounters(raw []byte) (Counters, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var values map[string]any
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	result := make(Counters)
	for name, rawValue := range values {
		number, ok := rawValue.(json.Number)
		if !ok {
			continue
		}
		value, err := number.Float64()
		if err != nil {
			return nil, fmt.Errorf("decode counter %s: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

func deltaCounters(before, after Counters) Counters {
	result := make(Counters)
	for name, end := range after {
		result[name] = nonNegative(end - before[name])
	}
	return result
}

func deltaStatements(before, after []Statement) []Statement {
	start := make(map[string]Statement, len(before))
	for _, item := range before {
		start[statementKey(item)] = item
	}
	result := make([]Statement, 0, len(after))
	for _, end := range after {
		begin := start[statementKey(end)]
		item := end
		item.Calls = nonNegativeInt(end.Calls - begin.Calls)
		item.TotalExecTimeMillis = nonNegative(end.TotalExecTimeMillis - begin.TotalExecTimeMillis)
		item.Rows = nonNegativeInt(end.Rows - begin.Rows)
		item.WALRecords = nonNegativeInt(end.WALRecords - begin.WALRecords)
		item.WALFullPageImages = nonNegativeInt(end.WALFullPageImages - begin.WALFullPageImages)
		item.WALBytes = nonNegative(end.WALBytes - begin.WALBytes)
		item.SharedBlocksHit = nonNegativeInt(end.SharedBlocksHit - begin.SharedBlocksHit)
		item.SharedBlocksRead = nonNegativeInt(end.SharedBlocksRead - begin.SharedBlocksRead)
		item.SharedBlocksDirtied = nonNegativeInt(end.SharedBlocksDirtied - begin.SharedBlocksDirtied)
		item.SharedBlocksWritten = nonNegativeInt(end.SharedBlocksWritten - begin.SharedBlocksWritten)
		if item.Calls != 0 {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Classification != result[j].Classification {
			return result[i].Classification < result[j].Classification
		}
		if result[i].Calls != result[j].Calls {
			return result[i].Calls > result[j].Calls
		}
		return result[i].Query < result[j].Query
	})
	return result
}

func deltaIO(before, after []IOCounters) []IOCounters {
	start := make(map[string]Counters, len(before))
	for _, item := range before {
		start[ioKey(item)] = item.Counters
	}
	result := make([]IOCounters, 0, len(after))
	for _, item := range after {
		item.Counters = deltaCounters(start[ioKey(item)], item.Counters)
		result = append(result, item)
	}
	return result
}

func statementKey(item Statement) string { return item.QueryID + "\x00" + item.Query }
func ioKey(item IOCounters) string {
	return item.BackendType + "\x00" + item.Object + "\x00" + item.Context
}

func normalizeSQL(query string) string { return strings.Join(strings.Fields(query), " ") }

func nonNegative(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}

func nonNegativeInt(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
