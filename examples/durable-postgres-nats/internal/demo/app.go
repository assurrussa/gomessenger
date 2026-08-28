package demo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	outboxmigrator "github.com/assurrussa/outbox/backends/pgsql/migrator"
	outboxstorage "github.com/assurrussa/outbox/backends/pgsql/storage"
	outboxlogger "github.com/assurrussa/outbox/outbox/logger"
	_ "github.com/jackc/pgx/v5/stdlib" // Register the host-owned business pool driver.
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The credentials belong only to the isolated local Compose example.
//
//nolint:gosec // This is an isolated local example credential.
const defaultPostgresDSN = "postgres://gomessenger:gomessenger@127.0.0.1:5432/gomessenger?sslmode=disable"

// Config controls one shared demo application runtime.
type Config struct {
	PostgresDSN                string
	NATSURL                    string
	ConnectionName             string
	OutboxWorkers              int
	OutboxReservationBatchSize int
	OutboxProducerMaxConns     int
	OutboxRelayMaxConns        int
	ConsumerConcurrency        int
	DBMaxOpenConns             int
	FileStorage                bool
	Logger                     *slog.Logger
}

// CorrectnessConfig returns the intentionally small development topology used
// by the deterministic correctness demonstration.
func CorrectnessConfig(logger *slog.Logger) Config {
	return Config{
		PostgresDSN: EnvOr("POSTGRES_DSN", defaultPostgresDSN),
		NATSURL:     EnvOr("NATS_URL", natsio.DefaultURL), ConnectionName: "gomessenger-durable-demo",
		OutboxWorkers: 1, OutboxReservationBatchSize: 1,
		OutboxProducerMaxConns: 1, OutboxRelayMaxConns: 1,
		ConsumerConcurrency: 1, DBMaxOpenConns: 5, Logger: logger,
	}
}

// CapacityConfig returns the file-backed baseline used by the capacity service.
func CapacityConfig(logger *slog.Logger) Config {
	return Config{
		PostgresDSN: EnvOr("POSTGRES_DSN", defaultPostgresDSN),
		NATSURL:     EnvOr("NATS_URL", natsio.DefaultURL), ConnectionName: "gomessenger-capacity-api",
		OutboxWorkers: 4, OutboxReservationBatchSize: 1,
		OutboxProducerMaxConns: 9, OutboxRelayMaxConns: 1,
		ConsumerConcurrency: 4, DBMaxOpenConns: 32,
		FileStorage: true, Logger: logger,
	}
}

// Application owns the database, Outbox relay, NATS connection, and durable consumer.
type Application struct {
	log          *slog.Logger
	db           *sql.DB
	connection   *natsio.Conn
	outbox       *splitOutboxRuntime
	consumer     *natsadapter.Consumer
	bus          *messenger.Messenger
	event        messenger.Event[OrderCreated]
	attempts     *attemptTracker
	duplicates   *atomic.Int64
	observations *benchmarkObservationRecorder
	publications *publicationRecorder

	publicationRunner *runner
	outboxRunner      *runner
	consumerRunner    *runner
	runtimeStopped    <-chan struct{}
	runtimeCause      func() error
	cancelRuntime     context.CancelCauseFunc
	draining          atomic.Bool
	drainOnce         sync.Once
	closeOnce         sync.Once
	closeErr          error
}

// Open starts the complete producer, relay, broker, Inbox, and consumer path.
func Open(ctx context.Context, config Config) (application *Application, openErr error) {
	if err := normalizeConfig(&config); err != nil {
		return nil, err
	}
	runtimeCtx, cancelRuntime := context.WithCancelCause(context.WithoutCancel(ctx))
	application = &Application{
		log: config.Logger, runtimeStopped: runtimeCtx.Done(), cancelRuntime: cancelRuntime,
		runtimeCause: func() error { return context.Cause(runtimeCtx) },
	}
	defer func() {
		if openErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		openErr = errors.Join(openErr, application.Close(cleanupCtx))
	}()

	db, err := waitForPostgres(ctx, config.PostgresDSN)
	if err != nil {
		return nil, err
	}
	application.db = db
	db.SetMaxOpenConns(config.DBMaxOpenConns)
	db.SetMaxIdleConns(config.DBMaxOpenConns)
	config.Logger.Info("postgres ready")

	if err := migrate(ctx, db); err != nil {
		return nil, err
	}
	config.Logger.Info("outbox, inbox, and demo business schema ready")

	connection, err := waitForNATS(ctx, config.NATSURL, config.ConnectionName)
	if err != nil {
		return nil, err
	}
	application.connection = connection
	config.Logger.Info("nats ready", "url", config.NATSURL)

	if _, err := natsadapter.ApplyTopology(ctx, connection, demoTopology(config.FileStorage)); err != nil {
		return nil, fmt.Errorf("apply demo topology: %w", err)
	}

	outboxRuntime, consumer, bus, event, attempts, duplicates, observations, publications, err := buildMessaging(
		ctx, config, db, connection,
	)
	if err != nil {
		return nil, err
	}
	application.outbox = outboxRuntime
	application.consumer = consumer
	application.bus = bus
	application.event = event
	application.attempts = attempts
	application.duplicates = duplicates
	application.observations = observations
	application.publications = publications

	// Open's context bounds startup only. Once ready, the host owns the runtime
	// until Close, even if it releases or cancels that startup context.
	application.publicationRunner = startRunner(runtimeCtx, publications.Run)
	application.superviseRunner("publication recorder", application.publicationRunner)
	if err := waitReady(
		ctx, "publication recorder", publications.Readiness, application.publicationRunner,
		application.runtimeDone(), application.runtimeFailure,
	); err != nil {
		return nil, err
	}
	application.outboxRunner = startRunner(runtimeCtx, outboxRuntime.Run)
	application.superviseRunner("outbox", application.outboxRunner)
	if err := waitReady(
		ctx, "outbox", outboxRuntime.Readiness, application.outboxRunner,
		application.runtimeDone(), application.runtimeFailure,
	); err != nil {
		return nil, err
	}
	application.consumerRunner = startRunner(runtimeCtx, consumer.Run)
	application.superviseRunner("consumer", application.consumerRunner)
	if err := waitReady(
		ctx, "consumer", consumer.Readiness, application.consumerRunner,
		application.runtimeDone(), application.runtimeFailure,
	); err != nil {
		return nil, err
	}
	return application, nil
}

// StageOrder atomically inserts the business order and stages its canonical event.
func (a *Application) StageOrder(
	ctx context.Context,
	payload OrderCreated,
	labels BenchmarkLabels,
	offeredAt time.Time,
) (messenger.Receipt, error) {
	if a == nil || a.outbox == nil || a.bus == nil {
		return messenger.Receipt{}, errors.New("demo application is not initialized")
	}
	if a.draining.Load() {
		return messenger.Receipt{}, errors.New("demo application is draining")
	}
	var headers map[string]string
	if labels != (BenchmarkLabels{}) {
		if err := labels.Validate(); err != nil {
			return messenger.Receipt{}, fmt.Errorf("validate benchmark labels: %w", err)
		}
		headers = map[string]string{
			BenchmarkRunHeader:   labels.RunID,
			BenchmarkStageHeader: labels.StageID,
		}
	}
	if offeredAt.IsZero() {
		offeredAt = time.Now().UTC()
	}
	items, err := json.Marshal(payload.Items)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("encode order items: %w", err)
	}

	var receipt messenger.Receipt
	registeredObservation := false
	err = a.outbox.ProducerTransactor().RunInTx(ctx, func(txCtx context.Context) error {
		tx := outboxstorage.GetTx(txCtx)
		if tx == nil {
			return errors.New("missing Outbox business transaction")
		}
		if _, err := tx.Exec(txCtx, `INSERT INTO demo.orders (
			id, customer_id, currency, items, amount, note, scenario, run_id, stage_id, offered_at
		) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10)`,
			payload.OrderID, payload.CustomerID, payload.Currency, string(items), payload.Amount,
			payload.Note, payload.Scenario, labels.RunID, labels.StageID, offeredAt,
		); err != nil {
			return fmt.Errorf("insert business order: %w", err)
		}
		var publishErr error
		receipt, publishErr = a.bus.PublishMessage(txCtx, a.event, messenger.Outgoing[OrderCreated]{
			Payload:  payload,
			Metadata: messenger.OutgoingMetadata{Key: payload.OrderID, Headers: headers},
		})
		if publishErr != nil {
			return publishErr
		}
		if labels != (BenchmarkLabels{}) {
			a.observations.register(receipt.MessageID.String(), labels)
			registeredObservation = true
		}
		tag, err := tx.Exec(txCtx, `UPDATE demo.orders
			SET message_id = $2, accepted_at = clock_timestamp() WHERE id = $1`,
			payload.OrderID, receipt.MessageID.String())
		if err != nil {
			return fmt.Errorf("attach message identity to business order: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errors.New("business order disappeared before commit")
		}
		return nil
	})
	if err != nil {
		if registeredObservation {
			a.observations.unregister(receipt.MessageID.String())
		}
		return messenger.Receipt{}, fmt.Errorf("commit business order and Outbox event: %w", err)
	}
	if receipt.State != messenger.ReceiptStaged || receipt.MessageID.IsZero() {
		return messenger.Receipt{}, fmt.Errorf("unexpected Outbox receipt: %#v", receipt)
	}
	if labels == (BenchmarkLabels{}) {
		a.log.Info("business order and event staged atomically",
			"order_id", payload.OrderID, "message_id", receipt.MessageID.String())
	}
	return receipt, nil
}

// Readiness checks every resource required to accept and complete an order.
func (a *Application) Readiness(ctx context.Context) error {
	if a == nil || a.db == nil || a.connection == nil || a.outbox == nil || a.consumer == nil ||
		a.publications == nil {
		return errors.New("demo application is not initialized")
	}
	if a.draining.Load() {
		return errors.New("demo application is draining")
	}
	if err := a.runtimeFailure(); err != nil {
		return fmt.Errorf("demo required runtime failed: %w", err)
	}
	if err := a.db.PingContext(ctx); err != nil {
		return fmt.Errorf("business PostgreSQL readiness: %w", err)
	}
	if err := a.publications.Readiness(ctx); err != nil {
		return fmt.Errorf("publication recorder readiness: %w", err)
	}
	if !a.connection.IsConnected() {
		return errors.New("NATS connection is not ready")
	}
	if err := a.outbox.Readiness(ctx); err != nil {
		return fmt.Errorf("outbox readiness: %w", err)
	}
	if err := a.consumer.Readiness(ctx); err != nil {
		return fmt.Errorf("consumer readiness: %w", err)
	}
	return nil
}

// BeginDrain closes HTTP/business admission and starts runtime drain.
func (a *Application) BeginDrain() {
	if a == nil {
		return
	}
	a.drainOnce.Do(func() {
		a.draining.Store(true)
		if a.consumer != nil {
			a.consumer.BeginDrain()
		}
		if a.outbox != nil {
			a.outbox.BeginDrain()
		}
	})
}

// Close stops all runtime loops and releases their owned resources.
func (a *Application) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.BeginDrain()
		if a.consumer != nil {
			joinError(&a.closeErr, "shutdown consumer", a.consumer.Shutdown(ctx))
		}
		if a.consumerRunner != nil {
			joinError(&a.closeErr, "stop consumer runtime", a.consumerRunner.stop(ctx))
		}
		if a.outboxRunner != nil {
			joinError(&a.closeErr, "stop outbox runtime", a.outboxRunner.stop(ctx))
		}
		if a.publicationRunner != nil {
			joinError(&a.closeErr, "stop publication recorder", a.publicationRunner.stop(ctx))
		}
		if a.publications != nil {
			joinError(&a.closeErr, "final publication recorder flush", a.publications.Flush(ctx))
		}
		if a.outbox != nil {
			joinError(&a.closeErr, "close Outbox relay pool", a.outbox.CloseRelay())
			joinError(&a.closeErr, "close host-owned Outbox producer pool", a.outbox.CloseProducer())
		}
		if a.connection != nil {
			a.connection.Close()
		}
		if a.db != nil {
			joinError(&a.closeErr, "close PostgreSQL", a.db.Close())
		}
		if a.cancelRuntime != nil {
			a.cancelRuntime(context.Canceled)
		}
	})
	return a.closeErr
}

func (a *Application) superviseRunner(name string, runtimeRunner *runner) {
	go func() {
		<-runtimeRunner.done
		if a.draining.Load() {
			return
		}
		runErr := runtimeRunner.result()
		if runErr == nil {
			runErr = errors.New("runtime stopped without an error")
		}
		a.draining.Store(true)
		a.cancelRuntime(fmt.Errorf("%s runtime stopped unexpectedly: %w", name, runErr))
	}()
}

func (a *Application) runtimeDone() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.runtimeStopped
}

func (a *Application) runtimeFailure() error {
	if a == nil || a.runtimeCause == nil {
		return nil
	}
	return a.runtimeCause()
}

// DB exposes the host-owned business pool to the example's correctness checks.
func (a *Application) DB() *sql.DB { return a.db }

// NATS exposes the host-owned connection to the example's correctness checks.
func (a *Application) NATS() *natsio.Conn { return a.connection }

// OutboxTotal returns the exact number of queued relay jobs.
func (a *Application) OutboxTotal(ctx context.Context) (int64, error) {
	stats, err := a.outbox.Service().GetQueueStats(ctx)
	if err != nil {
		return 0, fmt.Errorf("read Outbox queue stats: %w", err)
	}
	return stats.Total, nil
}

// FlushPublications persists broker confirmations already accepted by the
// in-memory recorder. It is a capacity-only measurement boundary; relay
// delivery does not depend on its result.
func (a *Application) FlushPublications(ctx context.Context) error {
	if a == nil || a.publications == nil {
		return errors.New("publication recorder is not initialized")
	}
	return a.publications.Flush(ctx)
}

func normalizeConfig(config *Config) error {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.PostgresDSN == "" || config.NATSURL == "" || config.ConnectionName == "" {
		return errors.New("PostgreSQL DSN, NATS URL, and connection name are required")
	}
	if config.OutboxWorkers < 1 || config.OutboxWorkers > 128 {
		return errors.New("outbox workers must be in 1..128")
	}
	if config.OutboxReservationBatchSize < 1 || config.OutboxReservationBatchSize > 1_000 {
		return errors.New("outbox reservation batch size must be in 1..1000")
	}
	if config.OutboxProducerMaxConns < 1 || config.OutboxProducerMaxConns > 1_024 {
		return errors.New("outbox producer max connections must be in 1..1024")
	}
	if config.OutboxRelayMaxConns < 1 || config.OutboxRelayMaxConns > 1_024 {
		return errors.New("outbox relay max connections must be in 1..1024")
	}
	if config.ConsumerConcurrency < 1 || config.ConsumerConcurrency > 128 {
		return errors.New("consumer concurrency must be in 1..128")
	}
	if config.DBMaxOpenConns < config.ConsumerConcurrency+2 || config.DBMaxOpenConns > 1_024 {
		return errors.New("business DB max open connections must cover consumer concurrency plus two")
	}
	return nil
}

func demoTopology(fileStorage bool) natsadapter.Topology {
	stream := natsadapter.DevStream(Stream, Namespace+".command.>", Namespace+".event.>")
	dlq := natsadapter.DevDLQStream(DLQStream, DLQSubject)
	if fileStorage {
		stream.Storage = jetstream.FileStorage
		dlq.Storage = jetstream.FileStorage
	}
	return natsadapter.Topology{
		SpecVersion: natsadapter.TopologySpecVersion,
		Streams:     []natsadapter.StreamSpec{stream, dlq},
	}
}

func buildMessaging(
	ctx context.Context,
	config Config,
	db *sql.DB,
	connection *natsio.Conn,
) (
	*splitOutboxRuntime,
	*natsadapter.Consumer,
	*messenger.Messenger,
	messenger.Event[OrderCreated],
	*attemptTracker,
	*atomic.Int64,
	*benchmarkObservationRecorder,
	*publicationRecorder,
	error,
) {
	outboxRuntime, err := openSplitOutboxRuntime(ctx, config)
	if err != nil {
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("open PostgreSQL outbox runtime: %w", err)
	}
	closeOutbox := func() {
		_ = outboxRuntime.CloseRelay()
		_ = outboxRuntime.CloseProducer()
	}
	publications, err := newPublicationRecorder(db)
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil, err
	}

	brokerRoute, err := natsadapter.NewRoute(connection, natsadapter.RouteConfig{
		Name: "nats.demo.events", Namespace: Namespace, WireMode: natsadapter.WireNative,
	})
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("create NATS route: %w", err)
	}
	publisher, err := newMeasurementPublisher(brokerRoute, publications.Record)
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil, err
	}
	relay, err := outboxadapter.NewRelayJob(publisher, outboxadapter.RelayJobConfig{
		ExecutionTimeout: 5 * time.Second,
		MaxAttempts:      3,
	})
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("create Outbox relay: %w", err)
	}
	if err := outboxRuntime.registerJob(relay); err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("register Outbox relay: %w", err)
	}
	producer, err := outboxadapter.NewProducer(outboxRuntime.Service(), outboxadapter.ProducerConfig{
		Name: "outbox.demo.events",
	})
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("create Outbox producer: %w", err)
	}
	measuredProducer, err := newMeasurementRoute(producer)
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil, err
	}

	event := messenger.MustEvent("orders.created", 1, messenger.JSON[OrderCreated]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:durable-demo-producer"))
	builder.RouteEvent(event, measuredProducer)
	bus, _, err := builder.Build()
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("build producer messenger: %w", err)
	}

	durableInbox, err := inboxpgsql.New(db,
		inboxpgsql.WithSchema(Namespace), inboxpgsql.WithTablePrefix("gm_"))
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("create PostgreSQL inbox: %w", err)
	}
	observed := &observingInbox{delegate: durableInbox}
	store, err := inbox.New(observed)
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("observe PostgreSQL inbox: %w", err)
	}
	attempts := newAttemptTracker()
	application := &handlerApplication{log: config.Logger, attempts: attempts}
	observations := newBenchmarkObservationRecorder()
	consumer, err := natsadapter.NewEventConsumer(
		connection,
		store,
		event,
		application.handleOrder,
		natsadapter.HandlerConfig{
			Stream: Stream, Namespace: Namespace, ConsumerID: ConsumerID,
			WireMode: natsadapter.WireNative, Concurrency: config.ConsumerConcurrency,
			Timeout: 5 * time.Second, FinalizationTimeout: 2 * time.Second, MaxAttempts: 3,
			BaseRetry: 250 * time.Millisecond, MaxRetry: 500 * time.Millisecond,
			AckWait: 2 * time.Second, DLQSubject: DLQSubject, Replicas: 1,
			MemoryStorage: !config.FileStorage, Logger: messenger.AdaptSlog(config.Logger),
			Observers: []messenger.Observer{observations},
		},
	)
	if err != nil {
		closeOutbox()
		return nil, nil, nil, messenger.Event[OrderCreated]{}, nil, nil, nil, nil,
			fmt.Errorf("create durable consumer: %w", err)
	}
	return outboxRuntime, consumer, bus, event, attempts, &observed.duplicates, observations, publications, nil
}

type handlerApplication struct {
	log      *slog.Logger
	attempts *attemptTracker
}

func (a *handlerApplication) handleOrder(ctx context.Context, message messenger.Message[OrderCreated]) error {
	payload := message.Payload
	attempt := 1
	if payload.Scenario != ScenarioSuccess {
		attempt = a.attempts.next(payload.OrderID)
	}
	tx, ok := inbox.SQLTxFromContext(ctx)
	if !ok {
		return errors.New("missing Inbox transaction")
	}
	labels, measured, err := benchmarkLabels(message.Metadata.Headers)
	if err != nil {
		return messenger.Permanent(err)
	}
	if !measured {
		labels = BenchmarkLabels{}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO demo.order_projection (
		order_id, message_id, amount, handler_attempt, run_id, stage_id, handled_at
	) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())`,
		payload.OrderID, message.Metadata.ID.String(), payload.Amount, attempt, labels.RunID, labels.StageID,
	); err != nil {
		return fmt.Errorf("write order projection: %w", err)
	}

	switch payload.Scenario {
	case ScenarioSuccess:
		return nil
	case ScenarioRetry:
		if attempt == 1 {
			a.log.Info("handler requests retry after writing; Inbox will roll the write back",
				"order_id", payload.OrderID, "attempt", attempt)
			return messenger.RetryAfter(errors.New("inventory temporarily unavailable"), 300*time.Millisecond)
		}
	case ScenarioDLQ:
		if attempt == 1 {
			a.log.Info("handler marks failure permanent after writing; Inbox will roll the write back",
				"order_id", payload.OrderID, "attempt", attempt)
			return messenger.Permanent(errors.New("unsupported order for demo"))
		}
	default:
		return messenger.Permanent(fmt.Errorf("unknown demo scenario %q", payload.Scenario))
	}

	a.log.Info("handler business write will commit with Inbox marker",
		"order_id", payload.OrderID, "attempt", attempt)
	return nil
}

type attemptTracker struct {
	mu     sync.Mutex
	values map[string]int
}

func newAttemptTracker() *attemptTracker {
	return &attemptTracker{values: make(map[string]int)}
}

func (t *attemptTracker) next(orderID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.values[orderID]++
	return t.values[orderID]
}

func (t *attemptTracker) get(orderID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.values[orderID]
}

type observingInbox struct {
	delegate   *inbox.Store
	duplicates atomic.Int64
}

func (b *observingInbox) Process(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	result, err := b.delegate.Process(ctx, key, fingerprint, handler)
	if err == nil && result.Duplicate {
		b.duplicates.Add(1)
	}
	return result, err
}

func (b *observingInbox) ProcessAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	maxAttempts uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	result, err := b.delegate.ProcessAttempt(ctx, key, fingerprint, maxAttempts, handler)
	if err == nil && result.Duplicate {
		b.duplicates.Add(1)
	}
	return result, err
}

func (b *observingInbox) ForgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	return b.delegate.ForgetAttempt(ctx, key, fingerprint)
}

func (b *observingInbox) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	return b.delegate.Prune(ctx, before, limit)
}

func migrate(ctx context.Context, db *sql.DB) error {
	if err := outboxmigrator.RunEmbedded(
		ctx, db, outboxlogger.Discard(), outboxmigrator.WithCommand("up"),
	); err != nil {
		return fmt.Errorf("migrate Outbox: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS demo`); err != nil {
		return fmt.Errorf("create demo schema: %w", err)
	}
	if err := inboxpgsql.Migrate(ctx, db,
		inboxpgsql.WithSchema(Namespace), inboxpgsql.WithTablePrefix("gm_")); err != nil {
		return fmt.Errorf("migrate Inbox: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo.orders (
		id TEXT PRIMARY KEY,
		customer_id TEXT NOT NULL DEFAULT '',
		currency TEXT NOT NULL DEFAULT 'USD',
		items JSONB NOT NULL DEFAULT '[]'::jsonb,
		amount BIGINT NOT NULL,
		note TEXT NOT NULL DEFAULT '',
		scenario TEXT NOT NULL,
		run_id TEXT NOT NULL DEFAULT '',
		stage_id TEXT NOT NULL DEFAULT '',
		message_id UUID NULL,
		offered_at TIMESTAMPTZ NOT NULL,
		accepted_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create demo orders: %w", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE demo.orders
		ADD COLUMN IF NOT EXISTS customer_id TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'USD',
		ADD COLUMN IF NOT EXISTS items JSONB NOT NULL DEFAULT '[]'::jsonb,
		ADD COLUMN IF NOT EXISTS note TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS run_id TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS stage_id TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS message_id UUID NULL,
		ADD COLUMN IF NOT EXISTS offered_at TIMESTAMPTZ NULL,
		ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ NULL`); err != nil {
		return fmt.Errorf("extend demo orders: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS demo_orders_message_id_uq
		ON demo.orders (message_id) WHERE message_id IS NOT NULL`); err != nil {
		return fmt.Errorf("index demo order message identities: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS demo_orders_run_stage_idx
		ON demo.orders (run_id, stage_id)`); err != nil {
		return fmt.Errorf("index demo order capacity labels: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo.order_projection (
		order_id TEXT PRIMARY KEY,
		message_id UUID NULL,
		amount BIGINT NOT NULL,
		handler_attempt INTEGER NOT NULL,
		run_id TEXT NOT NULL DEFAULT '',
		stage_id TEXT NOT NULL DEFAULT '',
		handled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create demo projection: %w", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE demo.order_projection
		ADD COLUMN IF NOT EXISTS message_id UUID NULL,
		ADD COLUMN IF NOT EXISTS run_id TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS stage_id TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("extend demo projection: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS demo_projection_message_id_uq
		ON demo.order_projection (message_id) WHERE message_id IS NOT NULL`); err != nil {
		return fmt.Errorf("index demo projection message identities: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS demo_projection_run_stage_idx
		ON demo.order_projection (run_id, stage_id)`); err != nil {
		return fmt.Errorf("index demo projection capacity labels: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo.envelope_measurements (
		message_id UUID PRIMARY KEY,
		run_id TEXT NOT NULL,
		stage_id TEXT NOT NULL,
		envelope_bytes BIGINT NOT NULL CHECK (envelope_bytes > 0),
		envelope_sha256 CHAR(64) NOT NULL,
		staged_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
		published_at TIMESTAMPTZ NULL
	)`); err != nil {
		return fmt.Errorf("create envelope measurements: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS demo_measurements_run_stage_idx
		ON demo.envelope_measurements (run_id, stage_id)`); err != nil {
		return fmt.Errorf("index envelope measurement capacity labels: %w", err)
	}
	return nil
}

func waitForPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return db, nil
		}
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, fmt.Errorf("wait for PostgreSQL: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func waitForNATS(ctx context.Context, url, connectionName string) (*natsio.Conn, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		connection, err := natsio.Connect(url,
			natsio.Name(connectionName), natsio.Timeout(time.Second),
		)
		if err == nil {
			return connection, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for NATS: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}
