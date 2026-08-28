//nolint:testpackage,gosec // Tests cover package-local wiring with an isolated fake DSN.
package demo

import (
	"context"
	"testing"
)

func TestWithApplicationName(t *testing.T) {
	t.Parallel()
	got, err := withApplicationName(
		"postgres://user:pass@db:5432/name?sslmode=disable&application_name=shared",
		producerApplicationName,
	)
	if err != nil {
		t.Fatalf("withApplicationName() error = %v", err)
	}
	want := "postgres://user:pass@db:5432/name?application_name=gomessenger-outbox-producer&sslmode=disable"
	if got != want {
		t.Fatalf("withApplicationName() = %q, want %q", got, want)
	}
}

func TestNormalizeConfigRejectsInvalidOutboxPools(t *testing.T) {
	t.Parallel()
	config := CorrectnessConfig(nil)
	config.OutboxProducerMaxConns = 0
	if err := normalizeConfig(&config); err == nil {
		t.Fatal("normalizeConfig() unexpectedly accepted a zero producer pool")
	}

	config = CorrectnessConfig(nil)
	config.OutboxRelayMaxConns = 0
	if err := normalizeConfig(&config); err == nil {
		t.Fatal("normalizeConfig() unexpectedly accepted a zero relay pool")
	}
}

func TestNormalizeConfigRejectsInvalidReservationBatchSize(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 1_001} {
		config := CorrectnessConfig(nil)
		config.OutboxReservationBatchSize = size
		if err := normalizeConfig(&config); err == nil {
			t.Fatalf("normalizeConfig() unexpectedly accepted reservation batch size %d", size)
		}
	}
}

func TestPoolAcquireObserverTracksConcurrentHighWaterMark(t *testing.T) {
	t.Parallel()

	observer := &poolAcquireObserver{}
	accepted, err := observer.prepareConn(context.Background(), nil)
	if err != nil || !accepted {
		t.Fatalf("prepareConn() = (%t, %v), want (true, nil)", accepted, err)
	}
	accepted, err = observer.prepareConn(context.Background(), nil)
	if err != nil || !accepted {
		t.Fatalf("prepareConn() = (%t, %v), want (true, nil)", accepted, err)
	}
	if got := observer.maximum.Load(); got != 2 {
		t.Fatalf("maximum acquisitions = %d, want 2", got)
	}
	if !observer.afterRelease(nil) {
		t.Fatal("afterRelease() unexpectedly discarded a connection")
	}
	if !observer.afterRelease(nil) {
		t.Fatal("afterRelease() unexpectedly discarded a connection")
	}
	if got := observer.current.Load(); got != 0 {
		t.Fatalf("current acquisitions = %d, want 0", got)
	}
}
