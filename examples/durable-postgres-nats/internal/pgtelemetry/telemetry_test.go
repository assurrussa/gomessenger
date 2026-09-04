//nolint:testpackage // Tests exercise package-local delta normalization.
package pgtelemetry

import (
	"net/url"
	"reflect"
	"testing"
)

const (
	testReadsCounter        = "reads"
	testInboxClassification = "inbox"
)

func TestProbeDSNPreservesConnectionOptions(t *testing.T) {
	t.Parallel()
	got, err := ProbeDSN("postgres://user:pass@db:5432/name?sslmode=disable")
	if err != nil {
		t.Fatalf("ProbeDSN() error = %v", err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse ProbeDSN() result: %v", err)
	}
	query := parsed.Query()
	if parsed.Host != "db:5432" || parsed.Path != "/name" || query.Get("sslmode") != "disable" ||
		query.Get("application_name") != ProbeApplicationName ||
		query.Get("max_parallel_workers_per_gather") != "0" {
		t.Fatalf("ProbeDSN() = %q", got)
	}
}

func TestProbeDSNOverridesParallelVerifierQueries(t *testing.T) {
	t.Parallel()
	got, err := ProbeDSN(
		"postgres://user:pass@db:5432/name?max_parallel_workers_per_gather=4",
	)
	if err != nil {
		t.Fatalf("ProbeDSN() error = %v", err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse ProbeDSN() result: %v", err)
	}
	if parsed.Query().Get("max_parallel_workers_per_gather") != "0" {
		t.Fatalf("ProbeDSN() = %q", got)
	}
}

func TestClassifyBatchCapacityStatements(t *testing.T) {
	if got := ClassifyStatement("INSERT INTO batch_capacity.business_effects SELECT * FROM unnest($1)"); got != "business" {
		t.Fatalf("ClassifyStatement() = %q, want business", got)
	}
	if got := ClassifyStatement("UPDATE batch_capacity.gm_inbox SET completed_at = now()"); got != "inbox" {
		t.Fatalf("ClassifyStatement() = %q, want inbox", got)
	}
}

func TestDeltaSnapshotsClassifiesAndSubtractsCounters(t *testing.T) {
	t.Parallel()
	before := Snapshot{
		Statements: []Statement{{QueryID: "1", Query: "SELECT attempts FROM demo.gm_inbox_attempts", Calls: 2}},
		Database:   Counters{"xact_commit": 10}, WAL: Counters{"wal_bytes": 100},
		IO: []IOCounters{{
			BackendType: "client backend", Object: "relation", Context: "normal",
			Counters: Counters{testReadsCounter: 2},
		}},
	}
	after := Snapshot{
		Statements: []Statement{{
			QueryID: "1", Query: "SELECT attempts FROM demo.gm_inbox_attempts", Classification: testInboxClassification, Calls: 7,
		}},
		Database: Counters{"xact_commit": 14}, WAL: Counters{"wal_bytes": 160},
		IO: []IOCounters{{
			BackendType: "client backend", Object: "relation", Context: "normal",
			Counters: Counters{testReadsCounter: 5},
		}},
	}
	delta := DeltaSnapshots(before, after)
	if len(delta.Statements) != 1 || delta.Statements[0].Calls != 5 ||
		delta.Database["xact_commit"] != 4 || delta.WAL["wal_bytes"] != 60 ||
		delta.IO[0].Counters[testReadsCounter] != 3 {
		t.Fatalf("delta = %#v", delta)
	}
}

func TestDecodeCountersIgnoresVersionSpecificNonNumericFields(t *testing.T) {
	t.Parallel()
	got, err := decodeCounters([]byte(`{"reads":12,"read_time":1.5,"stats_reset":"2026-01-01","optional":null}`))
	if err != nil {
		t.Fatalf("decodeCounters() error = %v", err)
	}
	want := Counters{"reads": 12, "read_time": 1.5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeCounters() = %#v, want %#v", got, want)
	}
}

func TestClassifyStatementExcludesProbeAndFindsInbox(t *testing.T) {
	t.Parallel()
	if got := ClassifyStatement("/* gomessenger-capacity-probe */ SELECT 1"); got != "probe" {
		t.Fatalf("probe classification = %q", got)
	}
	if got := ClassifyStatement("SAVEPOINT gomessenger_handler"); got != testInboxClassification {
		t.Fatalf("savepoint classification = %q", got)
	}
	if got := ClassifyStatement("RELEASE SAVEPOINT $1"); got != testInboxClassification {
		t.Fatalf("normalized savepoint classification = %q", got)
	}
	if got := ClassifyStatement("INSERT INTO demo.gm_inbox_attempts VALUES ($1)"); got != testInboxClassification {
		t.Fatalf("Inbox classification = %q", got)
	}
}
