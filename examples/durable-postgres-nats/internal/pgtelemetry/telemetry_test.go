//nolint:testpackage // Tests exercise package-local delta normalization.
package pgtelemetry

import (
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
	//nolint:gosec // This is a parser-only test DSN, never a live credential.
	want := "postgres://user:pass@db:5432/name?application_name=gomessenger-capacity-probe&sslmode=disable"
	if got != want {
		t.Fatalf("ProbeDSN() = %q, want %q", got, want)
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
