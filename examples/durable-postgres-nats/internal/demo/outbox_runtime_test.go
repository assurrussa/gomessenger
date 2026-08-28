//nolint:testpackage,gosec // Tests cover package-local wiring with an isolated fake DSN.
package demo

import (
	"context"
	"testing"
)

func TestObservedPoolConfigPreservesDSNFormats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dsn  string
	}{
		{name: "URL", dsn: "postgres://user:pass@db:5432/name?sslmode=disable&application_name=shared"},
		{name: "keyword", dsn: "host=/var/run/postgresql dbname=gomessenger user=gomessenger"},
		{name: "Unix socket URL", dsn: "postgres://gomessenger@/gomessenger?host=/var/run/postgresql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config, err := observedPoolConfig(
				test.dsn, producerApplicationName, 3, &poolAcquireObserver{},
			)
			if err != nil {
				t.Fatalf("observedPoolConfig() error = %v", err)
			}
			if got := config.ConnConfig.RuntimeParams["application_name"]; got != producerApplicationName {
				t.Fatalf("application_name = %q, want %q", got, producerApplicationName)
			}
			if config.MaxConns != 3 || config.PrepareConn == nil || config.AfterRelease == nil {
				t.Fatalf("pool instrumentation is incomplete: %#v", config)
			}
		})
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
