//nolint:testpackage // Tests exercise the package-local diagnostic snapshot boundary.
package demo

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	coreoutbox "github.com/assurrussa/outbox/outbox"
)

func TestOutboxStatsFromSnapshotReportsExactCapabilitiesAndGlobalOldestReady(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	supportedOldest := observedAt.Add(-30 * time.Minute)
	unsupportedOldest := observedAt.Add(-90 * time.Minute)
	queue := coreoutbox.QueueStats{
		ObservedAt: observedAt,
		Total:      11,
		Available:  8,
		Processing: 3,
		ByCapability: []coreoutbox.CapabilityQueueStats{
			{
				Name: outboxRelayCapability, SchemaVersion: 1,
				Total: 5, Available: 5, OldestAvailableAt: supportedOldest,
			},
			{
				Name: outboxRelayCapability, SchemaVersion: 2,
				Total: 3, Available: 3, OldestAvailableAt: unsupportedOldest,
			},
			{
				Name: "future.relay", SchemaVersion: 1,
				Total: 3, Processing: 3,
			},
		},
	}
	supports := func(name string, version coreoutbox.SchemaVersion) bool {
		return name == outboxRelayCapability && version == 1
	}

	stats := outboxStatsFromSnapshot(queue, supports)
	if stats.ObservedAt != observedAt || stats.Total != 11 || stats.Available != 8 || stats.Processing != 3 {
		t.Fatalf("aggregate stats = %#v", stats)
	}
	if stats.OldestAvailableAt == nil || !stats.OldestAvailableAt.Equal(unsupportedOldest) {
		t.Fatalf("global oldest available = %v, want %v", stats.OldestAvailableAt, unsupportedOldest)
	}
	if stats.OldestAgeSeconds != 90*60 {
		t.Fatalf("global oldest age = %f, want 5400", stats.OldestAgeSeconds)
	}
	if len(stats.ByCapability) != 3 {
		t.Fatalf("capability groups = %d, want 3", len(stats.ByCapability))
	}
	if !stats.ByCapability[0].Supported || stats.ByCapability[1].Supported || stats.ByCapability[2].Supported {
		t.Fatalf("exact supported flags = %#v", stats.ByCapability)
	}
	if stats.ByCapability[2].OldestAvailableAt != nil || stats.ByCapability[2].OldestAgeSeconds != 0 {
		t.Fatalf("processing-only group has a ready timestamp: %#v", stats.ByCapability[2])
	}
}

func TestOutboxStatsFromSnapshotUsesNullWhenNoReadyJobs(t *testing.T) {
	t.Parallel()
	stats := outboxStatsFromSnapshot(coreoutbox.QueueStats{
		ObservedAt: time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC),
		Total:      2,
		Processing: 2,
		ByCapability: []coreoutbox.CapabilityQueueStats{
			{Name: outboxRelayCapability, SchemaVersion: 1, Total: 2, Processing: 2},
		},
	}, func(string, coreoutbox.SchemaVersion) bool { return true })
	if stats.OldestAvailableAt != nil || stats.OldestAgeSeconds != 0 {
		t.Fatalf("aggregate ready timestamp = %#v", stats)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"oldestAvailableAt":null`) {
		t.Fatalf("JSON does not preserve null ready timestamp: %s", encoded)
	}
}

func TestStatsSourceDoesNotExecuteAdditionalSQL(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	path := filepath.Join(filepath.Dir(testFile), "stats.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range parsed.Imports {
		path, unquoteErr := strconv.Unquote(imported.Path.Value)
		if unquoteErr != nil {
			t.Fatal(unquoteErr)
		}
		if path == "database/sql" {
			t.Fatal("stats.go must not import database/sql")
		}
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			if value.Sel.Name == "QueryRowContext" || value.Sel.Name == "QueryContext" {
				t.Errorf("stats.go executes an additional SQL query through %s", value.Sel.Name)
			}
		case *ast.BasicLit:
			if value.Kind == token.STRING && strings.Contains(strings.ToUpper(value.Value), "SELECT ") {
				t.Error("stats.go contains an additional SELECT statement")
			}
		}
		return true
	})
}

const outboxRelayCapability = "gomessenger.relay"
