//nolint:testpackage // The test exercises the package-local load-boundary coordinator.
package capacity

import (
	"context"
	"testing"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

func TestPostgresSnapshotStartsAtLoadBoundary(t *testing.T) {
	t.Parallel()

	loadStartRequested := make(chan struct{})
	releaseLoadStart := make(chan struct{})
	captureStarted := make(chan struct{})
	releaseCapture := make(chan struct{})
	wantObservedAt := time.Unix(123, 0).UTC()
	result := startPostgresBoundarySnapshot(
		t.Context(), time.Second,
		func(context.Context) (time.Time, error) {
			close(loadStartRequested)
			<-releaseLoadStart
			return time.Now().Add(-time.Second), nil
		},
		func(context.Context) (pgtelemetry.Snapshot, error) {
			close(captureStarted)
			<-releaseCapture
			return pgtelemetry.Snapshot{ObservedAt: wantObservedAt}, nil
		},
	)

	select {
	case <-loadStartRequested:
	case <-time.After(time.Second):
		t.Fatal("PostgreSQL boundary coordinator did not request the load start")
	}
	select {
	case <-captureStarted:
		t.Fatal("PostgreSQL snapshot started before the load boundary was known")
	default:
	}
	close(releaseLoadStart)
	select {
	case <-captureStarted:
	case <-time.After(time.Second):
		t.Fatal("PostgreSQL snapshot did not start at the load boundary")
	}
	close(releaseCapture)
	select {
	case got := <-result:
		if got.err != nil || !got.snapshot.ObservedAt.Equal(wantObservedAt) {
			t.Fatalf("boundary snapshot = %#v, error = %v", got.snapshot, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("PostgreSQL boundary snapshot did not complete")
	}
}

func TestDrainTimingStartsAtLoadBoundary(t *testing.T) {
	t.Parallel()

	loadEndedAt := time.Unix(100, 0).UTC()
	duration, completed := drainTiming(loadEndedAt, loadEndedAt.Add(750*time.Millisecond), time.Second)
	if duration != 750*time.Millisecond || !completed {
		t.Fatalf("drain timing = %s, completed = %t", duration, completed)
	}

	duration, completed = drainTiming(loadEndedAt, loadEndedAt.Add(1100*time.Millisecond), time.Second)
	if duration != 1100*time.Millisecond || completed {
		t.Fatalf("late drain timing = %s, completed = %t", duration, completed)
	}
}
