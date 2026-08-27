package demo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLPoolStats is the stable subset of database/sql pool state used by reports.
type SQLPoolStats struct {
	MaxOpenConnections int   `json:"maxOpenConnections"`
	OpenConnections    int   `json:"openConnections"`
	InUse              int   `json:"inUse"`
	Idle               int   `json:"idle"`
	WaitCount          int64 `json:"waitCount"`
	WaitDurationNanos  int64 `json:"waitDurationNanos"`
}

// PGXPoolStats is the stable subset of the Outbox pgx pool state used by reports.
type PGXPoolStats struct {
	MaxConnections         int32 `json:"maxConnections"`
	TotalConnections       int32 `json:"totalConnections"`
	AcquiredConnections    int32 `json:"acquiredConnections"`
	IdleConnections        int32 `json:"idleConnections"`
	AcquireCount           int64 `json:"acquireCount"`
	EmptyAcquireCount      int64 `json:"emptyAcquireCount"`
	AcquireDurationNanos   int64 `json:"acquireDurationNanos"`
	MaxAcquiredConnections int32 `json:"maxAcquiredConnections"`
}

// OutboxStats describes the live relay queue.
type OutboxStats struct {
	Total            int64   `json:"total"`
	Available        int64   `json:"available"`
	Processing       int64   `json:"processing"`
	OldestAgeSeconds float64 `json:"oldestAgeSeconds"`
}

// OperationStats reports one observed consumer boundary without message labels.
type OperationStats struct {
	Count     int64   `json:"count"`
	Errors    int64   `json:"errors"`
	P50Millis float64 `json:"p50Millis"`
	P95Millis float64 `json:"p95Millis"`
	P99Millis float64 `json:"p99Millis"`
}

// ConsumerObservationStats separates Inbox transaction time from broker ACK time.
type ConsumerObservationStats struct {
	InboxHandle OperationStats `json:"inboxHandle"`
	BrokerAck   OperationStats `json:"brokerAck"`
	Duplicates  int64          `json:"duplicates"`
}

// AppStats is returned by the capacity-only diagnostic endpoint.
type AppStats struct {
	ObservedAt      time.Time                `json:"observedAt"`
	Ready           bool                     `json:"ready"`
	ReadinessError  string                   `json:"readinessError,omitempty"`
	Outbox          OutboxStats              `json:"outbox"`
	BusinessDB      SQLPoolStats             `json:"businessDb"`
	ProducerDB      PGXPoolStats             `json:"producerDb"`
	RelayDB         PGXPoolStats             `json:"relayDb"`
	Publications    PublicationRecorderStats `json:"publicationRecorder"`
	InboxDuplicates int64                    `json:"inboxDuplicates"`
	Consumer        ConsumerObservationStats `json:"consumer"`
}

// Stats reads a point-in-time application and pool snapshot.
func (a *Application) Stats(ctx context.Context, labels BenchmarkLabels) (AppStats, error) {
	if a == nil || a.db == nil || a.outbox == nil || a.publications == nil {
		return AppStats{}, errors.New("demo application is not initialized")
	}
	queue, err := a.outbox.Service().GetQueueStats(ctx)
	if err != nil {
		return AppStats{}, fmt.Errorf("read Outbox queue stats: %w", err)
	}
	var oldest sql.NullFloat64
	if err := a.db.QueryRowContext(ctx, `SELECT EXTRACT(EPOCH FROM (clock_timestamp() - MIN(created_at)))
		FROM jobs`).Scan(&oldest); err != nil {
		return AppStats{}, fmt.Errorf("read oldest Outbox job age: %w", err)
	}
	readyErr := a.Readiness(ctx)
	dbStats := a.db.Stats()
	producerPool := a.outbox.ProducerClient().DB().Pool()
	relayPool := a.outbox.RelayClient().DB().Pool()
	result := AppStats{
		ObservedAt: time.Now().UTC(),
		Ready:      readyErr == nil,
		Outbox: OutboxStats{
			Total: queue.Total, Available: queue.Available, Processing: queue.Processing,
		},
		BusinessDB: SQLPoolStats{
			MaxOpenConnections: dbStats.MaxOpenConnections,
			OpenConnections:    dbStats.OpenConnections,
			InUse:              dbStats.InUse,
			Idle:               dbStats.Idle,
			WaitCount:          dbStats.WaitCount,
			WaitDurationNanos:  dbStats.WaitDuration.Nanoseconds(),
		},
		ProducerDB:      poolStats(producerPool, &a.producerMaxAcquired),
		RelayDB:         poolStats(relayPool, &a.relayMaxAcquired),
		Publications:    a.publications.Stats(),
		InboxDuplicates: a.duplicates.Load(),
		Consumer:        a.observations.stats(labels),
	}
	if oldest.Valid && oldest.Float64 > 0 {
		result.Outbox.OldestAgeSeconds = oldest.Float64
	}
	if readyErr != nil {
		result.ReadinessError = readyErr.Error()
	}
	return result, nil
}

func poolStats(pool *pgxpool.Pool, maxAcquired *atomic.Int32) PGXPoolStats {
	stats := pool.Stat()
	observeMaxAcquired(pool, maxAcquired)
	// A completed acquisition proves a high-water mark of at least one even
	// when point-in-time sampling happens between short queries.
	if stats.AcquireCount() > 0 && maxAcquired.Load() == 0 {
		maxAcquired.CompareAndSwap(0, 1)
	}
	acquired := stats.AcquiredConns()
	return PGXPoolStats{
		MaxConnections:         stats.MaxConns(),
		TotalConnections:       stats.TotalConns(),
		AcquiredConnections:    acquired,
		IdleConnections:        stats.IdleConns(),
		AcquireCount:           stats.AcquireCount(),
		EmptyAcquireCount:      stats.EmptyAcquireCount(),
		AcquireDurationNanos:   stats.AcquireDuration().Nanoseconds(),
		MaxAcquiredConnections: maxAcquired.Load(),
	}
}

func observeMaxAcquired(pool *pgxpool.Pool, maxAcquired *atomic.Int32) {
	acquired := pool.Stat().AcquiredConns()
	for {
		previous := maxAcquired.Load()
		if acquired <= previous || maxAcquired.CompareAndSwap(previous, acquired) {
			return
		}
	}
}
