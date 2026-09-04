//nolint:testpackage,gosec // Tests cover package-local wiring with an isolated fake DSN.
package demo

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
			if config.MaxConns != 3 || config.ConnConfig.Tracer == nil {
				t.Fatalf("pool instrumentation is incomplete: %#v", config)
			}
			if _, ok := config.ConnConfig.Tracer.(pgxpool.AcquireTracer); !ok {
				t.Fatal("pool tracer does not implement acquire tracing")
			}
			if _, ok := config.ConnConfig.Tracer.(pgxpool.ReleaseTracer); !ok {
				t.Fatal("pool tracer does not implement release tracing")
			}
			watcher, ok := config.ConnConfig.BuildContextWatcherHandler(nil).(*pgconn.CancelRequestContextWatcherHandler)
			if !ok {
				t.Fatalf("context watcher = %T, want *pgconn.CancelRequestContextWatcherHandler", watcher)
			}
			if watcher.CancelRequestDelay != 0 || watcher.DeadlineDelay != postgresCancelRequestDeadlineDelay {
				t.Fatalf("context watcher delays = (%s, %s), want (0s, %s)",
					watcher.CancelRequestDelay, watcher.DeadlineDelay, postgresCancelRequestDeadlineDelay)
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

func TestNormalizeConfigConsumerModesAndBatchDefaults(t *testing.T) {
	t.Parallel()

	config := CorrectnessConfig(nil)
	config.ConsumerMode = ConsumerModeBatch
	config.ConsumerBatchMaxMessages = 1
	if err := normalizeConfig(&config); err != nil {
		t.Fatalf("normalizeConfig(batch) error = %v", err)
	}
	if config.ConsumerBatchMaxMessages != 1 ||
		config.ConsumerBatchMaxBytes != 4<<20 || config.ConsumerBatchMaxWait != 25*time.Millisecond {
		t.Fatalf("normalized batch config = %#v", config)
	}

	config = CorrectnessConfig(nil)
	config.ConsumerMode = ""
	config.ConsumerBatchMaxMessages = 0
	config.ConsumerBatchMaxBytes = 0
	config.ConsumerBatchMaxWait = 0
	if err := normalizeConfig(&config); err != nil {
		t.Fatalf("normalizeConfig(zero batch limits) error = %v", err)
	}
	if config.ConsumerMode != ConsumerModeSingle || config.ConsumerBatchMaxMessages != 100 ||
		config.ConsumerBatchMaxBytes != 4<<20 || config.ConsumerBatchMaxWait != 25*time.Millisecond {
		t.Fatalf("normalized zero config = %#v", config)
	}
}

func TestNormalizeConfigRejectsInvalidConsumerMode(t *testing.T) {
	t.Parallel()
	config := CorrectnessConfig(nil)
	config.ConsumerMode = "automatic"
	if err := normalizeConfig(&config); err == nil {
		t.Fatal("normalizeConfig() unexpectedly accepted consumer mode automatic")
	}
}

func TestPoolAcquireObserverTracksConcurrentHighWaterMark(t *testing.T) {
	t.Parallel()

	observer := &poolAcquireObserver{}
	observer.TraceAcquireEnd(context.Background(), nil, pgxpool.TraceAcquireEndData{Conn: &pgx.Conn{}})
	observer.TraceAcquireEnd(context.Background(), nil, pgxpool.TraceAcquireEndData{Conn: &pgx.Conn{}})
	if got := observer.maximum.Load(); got != 2 {
		t.Fatalf("maximum acquisitions = %d, want 2", got)
	}
	observer.TraceAcquireEnd(context.Background(), nil, pgxpool.TraceAcquireEndData{Err: context.Canceled})
	if got := observer.current.Load(); got != 2 {
		t.Fatalf("current acquisitions = %d, want 2", got)
	}
}
