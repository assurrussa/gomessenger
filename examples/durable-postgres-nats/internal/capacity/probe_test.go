//nolint:testpackage // Tests exercise the package-local sampling boundary.
package capacity

import (
	"testing"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

func TestBusinessSnapshotFromApplicationUsesLightweightProgress(t *testing.T) {
	t.Parallel()
	got := businessSnapshotFromApplication(demo.AppStats{Benchmark: demo.BenchmarkProgressStats{
		Accepted: 101, Staged: 100, Published: 90, Committed: 80,
	}})
	want := BusinessSnapshot{Accepted: 101, Staged: 100, Published: 90, Committed: 80}
	if got != want {
		t.Fatalf("businessSnapshotFromApplication() = %#v, want %#v", got, want)
	}
}
