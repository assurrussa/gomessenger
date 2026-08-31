package demo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pgsql "github.com/assurrussa/outbox/backends/pgsql"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsfailedrepo"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsrepo"
	pgsqlstorage "github.com/assurrussa/outbox/backends/pgsql/storage"
	"github.com/assurrussa/outbox/backends/pgsql/storage/pgsqlclient"
	pgsqltx "github.com/assurrussa/outbox/backends/pgsql/storage/transaction"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/outbox/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	producerApplicationName            = "gomessenger-outbox-producer"
	relayApplicationName               = "gomessenger-outbox-relay"
	postgresCancelRequestDeadlineDelay = 5 * time.Second
)

// splitOutboxRuntime deliberately gives producer staging and relay execution
// different pools. Repositories remain bound to the relay client, but honor a
// pgx transaction carried in context by the producer transactor.
type splitOutboxRuntime struct {
	producerClient       pgsql.Client
	relayClient          pgsql.Client
	producerTransactor   *pgsqltx.Manager
	service              *coreoutbox.Service
	producerAcquisitions *poolAcquireObserver
	relayAcquisitions    *poolAcquireObserver
	capabilitiesMu       sync.RWMutex
	capabilities         map[coreoutbox.JobCapability]struct{}
	observations         *outboxObservationRecorder

	closeRelayOnce    sync.Once
	closeProducerOnce sync.Once
	closeRelayErr     error
	closeProducerErr  error
}

func openSplitOutboxRuntime(ctx context.Context, config Config) (*splitOutboxRuntime, error) {
	producerClient, producerAcquisitions, err := openObservedPoolClient(
		ctx,
		config.PostgresDSN,
		producerApplicationName,
		// normalizeConfig caps the host setting at 1,024 before this constructor runs.
		//nolint:gosec // The validated value cannot overflow int32.
		int32(config.OutboxProducerMaxConns),
	)
	if err != nil {
		return nil, fmt.Errorf("open Outbox producer pool: %w", err)
	}
	closeProducer := func() { _ = producerClient.Close() }

	relayClient, relayAcquisitions, err := openObservedPoolClient(
		ctx,
		config.PostgresDSN,
		relayApplicationName,
		// normalizeConfig caps the host setting at 1,024 before this constructor runs.
		//nolint:gosec // The validated value cannot overflow int32.
		int32(config.OutboxRelayMaxConns),
	)
	if err != nil {
		closeProducer()
		return nil, fmt.Errorf("open Outbox relay pool: %w", err)
	}
	closeRelay := func() { _ = relayClient.Close() }

	jobs, err := jobsrepo.New(jobsrepo.NewOptions(relayClient))
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox jobs repository: %w", err)
	}
	failed, err := jobsfailedrepo.New(jobsfailedrepo.NewOptions(relayClient))
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox failed jobs repository: %w", err)
	}
	observations := newOutboxObservationRecorder()
	observedJobs := &observedOutboxJobsRepository{Repo: jobs, observations: observations}
	relayTransactor := pgsqltx.New(relayClient.DB())
	service, err := coreoutbox.New(
		coreoutbox.WithWorkers(config.OutboxWorkers),
		coreoutbox.WithReservationBatchSize(config.OutboxReservationBatchSize),
		coreoutbox.WithIdleTime(100*time.Millisecond),
		coreoutbox.WithReserveFor(5*time.Second),
		coreoutbox.WithJobsRepo(observedJobs),
		coreoutbox.WithJobsFailedRepo(failed),
		coreoutbox.WithTransactor(relayTransactor),
		coreoutbox.WithLogger(logger.Discard()),
	)
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox service: %w", err)
	}

	return &splitOutboxRuntime{
		producerClient:       producerClient,
		relayClient:          relayClient,
		producerTransactor:   pgsqltx.New(producerClient.DB()),
		service:              service,
		producerAcquisitions: producerAcquisitions,
		relayAcquisitions:    relayAcquisitions,
		capabilities:         make(map[coreoutbox.JobCapability]struct{}),
		observations:         observations,
	}, nil
}

func (r *splitOutboxRuntime) Run(ctx context.Context) error { return r.service.Run(ctx) }

func (r *splitOutboxRuntime) Readiness(ctx context.Context) error {
	if r == nil || r.producerClient == nil || r.relayClient == nil || r.service == nil {
		return errors.New("split Outbox runtime is not initialized")
	}
	if err := r.producerClient.DB().Ping(ctx); err != nil {
		return fmt.Errorf("producer pool: %w", err)
	}
	if err := r.relayClient.DB().Ping(ctx); err != nil {
		return fmt.Errorf("relay pool: %w", err)
	}
	return r.service.Readiness(ctx)
}

func (r *splitOutboxRuntime) BeginDrain() {
	if r != nil && r.service != nil {
		r.service.BeginDrain()
	}
}

func (r *splitOutboxRuntime) Service() *coreoutbox.Service { return r.service }

func (r *splitOutboxRuntime) registerJob(job coreoutbox.Job) error {
	if err := r.service.RegisterJob(job); err != nil {
		return err
	}
	capability := coreoutbox.JobCapability{
		Name:          job.Name(),
		SchemaVersion: coreoutbox.DefaultSchemaVersion,
	}
	if versioned, ok := job.(coreoutbox.VersionedJob); ok {
		capability.SchemaVersion = versioned.SchemaVersion()
	}
	r.capabilitiesMu.Lock()
	r.capabilities[capability] = struct{}{}
	r.capabilitiesMu.Unlock()
	return nil
}

func (r *splitOutboxRuntime) registerBatchJob(job coreoutbox.BatchJob, config coreoutbox.BatchConfig) error {
	if err := r.service.RegisterBatchJob(job, config); err != nil {
		return err
	}
	capability := coreoutbox.JobCapability{Name: job.Name(), SchemaVersion: coreoutbox.DefaultSchemaVersion}
	if versioned, ok := job.(coreoutbox.VersionedBatchJob); ok {
		capability.SchemaVersion = versioned.SchemaVersion()
	}
	r.capabilitiesMu.Lock()
	r.capabilities[capability] = struct{}{}
	r.capabilitiesMu.Unlock()
	return nil
}

func (r *splitOutboxRuntime) supportsCapability(name string, schemaVersion coreoutbox.SchemaVersion) bool {
	if r == nil {
		return false
	}
	r.capabilitiesMu.RLock()
	_, supported := r.capabilities[coreoutbox.JobCapability{Name: name, SchemaVersion: schemaVersion}]
	r.capabilitiesMu.RUnlock()
	return supported
}

func (r *splitOutboxRuntime) ProducerClient() pgsql.Client { return r.producerClient }

func (r *splitOutboxRuntime) RelayClient() pgsql.Client { return r.relayClient }

func (r *splitOutboxRuntime) ProducerTransactor() *pgsqltx.Manager { return r.producerTransactor }

func (r *splitOutboxRuntime) ObservationStats(labels BenchmarkLabels) OutboxExecutionStats {
	if r == nil {
		return OutboxExecutionStats{}
	}
	return r.observations.stats(labels)
}

func (r *splitOutboxRuntime) CloseRelay() error {
	if r == nil || r.relayClient == nil {
		return nil
	}
	r.closeRelayOnce.Do(func() { r.closeRelayErr = r.relayClient.Close() })
	return r.closeRelayErr
}

func (r *splitOutboxRuntime) CloseProducer() error {
	if r == nil || r.producerClient == nil {
		return nil
	}
	r.closeProducerOnce.Do(func() { r.closeProducerErr = r.producerClient.Close() })
	return r.closeProducerErr
}

type observedPoolClient struct {
	db pgsqlstorage.DBEngine
}

func (c *observedPoolClient) DB() pgsqlstorage.DBEngine { return c.db }

func (c *observedPoolClient) Close() error {
	c.db.Close()
	return nil
}

type poolAcquireObserver struct {
	current          atomic.Int32
	maximum          atomic.Int32
	unusableReleases atomic.Int64
}

var (
	_ pgx.QueryTracer       = (*poolAcquireObserver)(nil)
	_ pgxpool.AcquireTracer = (*poolAcquireObserver)(nil)
	_ pgxpool.ReleaseTracer = (*poolAcquireObserver)(nil)
)

func (*poolAcquireObserver) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	return ctx
}

func (*poolAcquireObserver) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (*poolAcquireObserver) TraceAcquireStart(
	ctx context.Context,
	_ *pgxpool.Pool,
	_ pgxpool.TraceAcquireStartData,
) context.Context {
	return ctx
}

func (o *poolAcquireObserver) TraceAcquireEnd(
	_ context.Context,
	_ *pgxpool.Pool,
	data pgxpool.TraceAcquireEndData,
) {
	if data.Err != nil || data.Conn == nil {
		return
	}
	current := o.current.Add(1)
	for {
		previous := o.maximum.Load()
		if current <= previous || o.maximum.CompareAndSwap(previous, current) {
			return
		}
	}
}

func (o *poolAcquireObserver) TraceRelease(
	_ *pgxpool.Pool,
	data pgxpool.TraceReleaseData,
) {
	o.current.Add(-1)
	connection := data.Conn
	if connection.IsClosed() || connection.PgConn().IsBusy() || connection.PgConn().TxStatus() != 'I' {
		o.unusableReleases.Add(1)
	}
}

func openObservedPoolClient(
	ctx context.Context,
	dsn string,
	applicationName string,
	maxConnections int32,
) (pgsql.Client, *poolAcquireObserver, error) {
	observer := &poolAcquireObserver{}
	config, err := observedPoolConfig(dsn, applicationName, maxConnections, observer)
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, fmt.Errorf("open PostgreSQL connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping PostgreSQL connection pool: %w", err)
	}
	return &observedPoolClient{
		db: pgsqlclient.NewDBEngine(pool, "prod", logger.Discard()),
	}, observer, nil
}

func observedPoolConfig(
	dsn string,
	applicationName string,
	maxConnections int32,
	observer *poolAcquireObserver,
) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection string: %w", err)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	config.MinConns = 1
	config.MaxConns = maxConnections
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = time.Hour
	// Supplemental batch claims are intentionally deadline-bounded. pgx's
	// default watcher closes the connection when such a deadline wins; ask
	// PostgreSQL to cancel the statement and retain the synchronized connection.
	config.ConnConfig.BuildContextWatcherHandler = func(connection *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{
			Conn:          connection,
			DeadlineDelay: postgresCancelRequestDeadlineDelay,
		}
	}
	if config.ConnConfig.Tracer == nil {
		config.ConnConfig.Tracer = observer
	} else {
		config.ConnConfig.Tracer = multitracer.New(config.ConnConfig.Tracer, observer)
	}
	config.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
		return connection.Ping(ctx)
	}
	return config, nil
}
