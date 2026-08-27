//nolint:testpackage // Tests exercise package-local percentile and DSN helpers.
package inboxcapacity

import (
	"testing"
	"time"
)

func TestLatencyStatsUsesContinuousPercentiles(t *testing.T) {
	t.Parallel()
	stats := latencyStats([]time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond})
	if stats.P50Millis != 2 || stats.P95Millis != 2.9 || stats.P99Millis != 2.98 {
		t.Fatalf("latency stats = %#v", stats)
	}
}

func TestWithApplicationName(t *testing.T) {
	t.Parallel()
	got, err := withApplicationName("postgres://user@db/name?sslmode=disable", "benchmark")
	if err != nil {
		t.Fatalf("withApplicationName() error = %v", err)
	}
	if got != "postgres://user@db/name?application_name=benchmark&sslmode=disable" {
		t.Fatalf("withApplicationName() = %q", got)
	}
}
