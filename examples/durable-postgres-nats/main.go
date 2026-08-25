package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	outboxmigrator "github.com/assurrussa/outbox/backends/pgsql/migrator"
	outboxruntime "github.com/assurrussa/outbox/backends/pgsql/runtime"
	outboxstorage "github.com/assurrussa/outbox/backends/pgsql/storage"
	outboxlogger "github.com/assurrussa/outbox/outbox/logger"
	_ "github.com/jackc/pgx/v5/stdlib"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	demoNamespace  = "demo"
	demoStream     = "GOMESSENGER_DEMO"
	demoDLQStream  = "GOMESSENGER_DEMO_DLQ"
	demoDLQSubject = "demo.dlq"
	demoConsumerID = "demo-order-projection-v1"

	scenarioRetry = "retry"
	scenarioDLQ   = "dlq"
)

type orderCreated struct {
	OrderID  string `json:"orderId"`
	Amount   int64  `json:"amount"`
	Scenario string `json:"scenario"`
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

type runner struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

func startRunner(parent context.Context, run func(context.Context) error) *runner {
	ctx, cancel := context.WithCancel(parent)
	r := &runner{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		err := run(ctx)
		r.mu.Lock()
		r.err = err
		r.mu.Unlock()
	}()
	return r
}

func (r *runner) result() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *runner) stop(parent context.Context, timeout time.Duration) error {
	r.cancel()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	select {
	case <-r.done:
		err := r.result()
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type demoApp struct {
	log        *slog.Logger
	outbox     *outboxruntime.Runtime
	bus        *messenger.Messenger
	event      messenger.Event[orderCreated]
	attempts   *attemptTracker
	duplicates *atomic.Int64
}

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := run(ctx, log); err != nil {
		log.Error("durable demo failed", "error", err)
		return 1
	}
	log.Info("durable demo passed")
	return 0
}

func run(ctx context.Context, log *slog.Logger) (runErr error) {
	dsn := envOr("POSTGRES_DSN", "postgres://gomessenger:gomessenger@127.0.0.1:5432/gomessenger?sslmode=disable")
	natsURL := envOr("NATS_URL", natsio.DefaultURL)

	db, err := waitForPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { joinRunError(&runErr, "close PostgreSQL", db.Close()) }()
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	log.Info("postgres ready")

	if err := migrate(ctx, db); err != nil {
		return err
	}
	log.Info("outbox, inbox, and demo business schema ready")

	connection, err := waitForNATS(ctx, natsURL)
	if err != nil {
		return err
	}
	defer connection.Close()
	log.Info("nats ready", "url", natsURL)

	if _, err := natsadapter.ApplyTopology(ctx, connection, natsadapter.Topology{
		SpecVersion: natsadapter.TopologySpecVersion,
		Streams: []natsadapter.StreamSpec{
			natsadapter.DevStream(demoStream, demoNamespace+".command.>", demoNamespace+".event.>"),
			natsadapter.DevDLQStream(demoDLQStream, demoDLQSubject),
		},
	}); err != nil {
		return fmt.Errorf("apply demo topology: %w", err)
	}

	sourceSubscription, err := connection.SubscribeSync(demoNamespace + ".event.>")
	if err != nil {
		return fmt.Errorf("subscribe to demo source: %w", err)
	}
	dlqSubscription, err := connection.SubscribeSync(demoDLQSubject)
	if err != nil {
		return fmt.Errorf("subscribe to demo DLQ: %w", err)
	}
	if err := connection.FlushTimeout(2 * time.Second); err != nil {
		return fmt.Errorf("flush demo subscriptions: %w", err)
	}

	app, consumer, err := buildApp(ctx, log, db, dsn, connection)
	if err != nil {
		return err
	}
	defer func() { joinRunError(&runErr, "close outbox runtime", app.outbox.Close()) }()

	outboxRunner := startRunner(ctx, app.outbox.Run)
	defer func() {
		app.outbox.BeginDrain()
		joinRunError(&runErr, "stop outbox runtime", outboxRunner.stop(ctx, 5*time.Second))
	}()
	if err := waitReady(ctx, "outbox", app.outbox.Readiness, outboxRunner); err != nil {
		return err
	}

	consumerRunner := startRunner(ctx, consumer.Run)
	defer func() {
		consumer.BeginDrain()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		shutdownErr := consumer.Shutdown(shutdownCtx)
		cancel()
		joinRunError(&runErr, "shutdown consumer", shutdownErr)
		joinRunError(&runErr, "stop consumer runtime", consumerRunner.stop(ctx, 5*time.Second))
	}()
	if err := waitReady(ctx, "consumer", consumer.Readiness, consumerRunner); err != nil {
		return err
	}

	js, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("create JetStream context: %w", err)
	}
	return runScenarios(ctx, log, db, app, js, sourceSubscription, dlqSubscription)
}

func runScenarios(
	ctx context.Context,
	log *slog.Logger,
	db *sql.DB,
	app *demoApp,
	js jetstream.JetStream,
	sourceSubscription *natsio.Subscription,
	dlqSubscription *natsio.Subscription,
) error {
	runID, err := randomID()
	if err != nil {
		return err
	}
	retryOrderID := runID + "-retry"
	dlqOrderID := runID + "-dlq"

	retryReceipt, err := app.stageOrder(ctx, orderCreated{
		OrderID: retryOrderID, Amount: 4200, Scenario: scenarioRetry,
	})
	if err != nil {
		return err
	}
	retryWire, err := waitForSource(ctx, sourceSubscription, retryReceipt.MessageID)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, "retried projection", func() (bool, error) {
		exists, existsErr := projectionExists(ctx, db, retryOrderID)
		return exists && app.attempts.get(retryOrderID) == 2, existsErr
	}); err != nil {
		return err
	}
	log.Info("intentional retry rolled back the first write and committed the second",
		"order_id", retryOrderID, "handler_attempts", app.attempts.get(retryOrderID))

	if err := publishDistinctDuplicate(ctx, js, retryWire, retryReceipt.MessageID); err != nil {
		return err
	}
	if err := waitFor(ctx, "inbox duplicate", func() (bool, error) {
		return app.duplicates.Load() >= 1, nil
	}); err != nil {
		return err
	}
	if attempts := app.attempts.get(retryOrderID); attempts != 2 {
		return fmt.Errorf("duplicate invoked retry handler: attempts=%d", attempts)
	}
	log.Info("inbox suppressed a distinct broker delivery", "order_id", retryOrderID)

	dlqReceipt, err := app.stageOrder(ctx, orderCreated{
		OrderID: dlqOrderID, Amount: 9900, Scenario: scenarioDLQ,
	})
	if err != nil {
		return err
	}
	if _, err := waitForSource(ctx, sourceSubscription, dlqReceipt.MessageID); err != nil {
		return err
	}
	record, err := waitForDLQ(ctx, dlqSubscription, dlqReceipt.MessageID)
	if err != nil {
		return err
	}
	if exists, err := projectionExists(ctx, db, dlqOrderID); err != nil {
		return err
	} else if exists {
		return errors.New("permanent handler write was not rolled back before DLQ hand-off")
	}
	log.Info("permanent failure rolled back business state and reached DLQ", "order_id", dlqOrderID)

	replay, err := natsadapter.ReplayDLQ(ctx, js, record)
	if err != nil {
		return fmt.Errorf("replay DLQ record: %w", err)
	}
	if replay.Duplicate {
		return errors.New("first DLQ replay was unexpectedly broker-deduplicated")
	}
	if err := waitFor(ctx, "replayed projection", func() (bool, error) {
		exists, existsErr := projectionExists(ctx, db, dlqOrderID)
		return exists && app.attempts.get(dlqOrderID) == 2, existsErr
	}); err != nil {
		return err
	}
	secondReplay, err := natsadapter.ReplayDLQ(ctx, js, record)
	if err != nil {
		return fmt.Errorf("repeat DLQ replay: %w", err)
	}
	if !secondReplay.Duplicate || secondReplay.Plan.ReplayID != replay.Plan.ReplayID {
		return errors.New("repeated DLQ replay did not use deterministic broker deduplication")
	}
	log.Info("DLQ replay committed once and repeated replay was broker-deduplicated",
		"order_id", dlqOrderID, "replay_id", replay.Plan.ReplayID)

	if err := waitFor(ctx, "empty outbox", func() (bool, error) {
		stats, statsErr := app.outbox.Service().GetQueueStats(ctx)
		return statsErr == nil && stats.Total == 0, statsErr
	}); err != nil {
		return err
	}

	log.Info("durable scenarios passed; draining runtimes",
		"business_orders", 2,
		"committed_projections", 2,
		"inbox_duplicates", app.duplicates.Load(),
		"retry_attempts", app.attempts.get(retryOrderID),
		"dlq_replay_attempts", app.attempts.get(dlqOrderID))
	return nil
}

func joinRunError(target *error, operation string, err error) {
	if err == nil {
		return
	}
	*target = errors.Join(*target, fmt.Errorf("%s: %w", operation, err))
}

func buildApp(
	ctx context.Context,
	log *slog.Logger,
	db *sql.DB,
	dsn string,
	connection *natsio.Conn,
) (*demoApp, *natsadapter.Consumer, error) {
	outboxRuntime, err := outboxruntime.Open(ctx, outboxruntime.Config{
		DSN: dsn, Workers: 1, IdleTime: 200 * time.Millisecond, ReserveFor: 5 * time.Second,
		Logger: outboxlogger.Discard(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open PostgreSQL outbox runtime: %w", err)
	}

	brokerRoute, err := natsadapter.NewRoute(connection, natsadapter.RouteConfig{
		Name: "nats.demo.events", Namespace: demoNamespace, WireMode: natsadapter.WireNative,
	})
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("create NATS route: %w", err)
	}
	relay, err := outboxadapter.NewRelayJob(brokerRoute, outboxadapter.RelayJobConfig{
		ExecutionTimeout: 5 * time.Second,
		MaxAttempts:      3,
	})
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("create Outbox relay: %w", err)
	}
	if err := outboxRuntime.Service().RegisterJob(relay); err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("register Outbox relay: %w", err)
	}
	producer, err := outboxadapter.NewProducer(outboxRuntime.Service(), outboxadapter.ProducerConfig{
		Name: "outbox.demo.events",
	})
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("create Outbox producer: %w", err)
	}

	event := messenger.MustEvent("orders.created", 1, messenger.JSON[orderCreated]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:durable-demo-producer"))
	builder.RouteEvent(event, producer)
	bus, _, err := builder.Build()
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("build producer messenger: %w", err)
	}

	inboxOptions := []inboxpgsql.Option{
		inboxpgsql.WithSchema(demoNamespace),
		inboxpgsql.WithTablePrefix("gm_"),
	}
	durableInbox, err := inboxpgsql.New(db, inboxOptions...)
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("create PostgreSQL inbox: %w", err)
	}
	observed := &observingInbox{delegate: durableInbox}
	store, err := inbox.New(observed)
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("observe PostgreSQL inbox: %w", err)
	}

	app := &demoApp{
		log: log, outbox: outboxRuntime, bus: bus, event: event,
		attempts: newAttemptTracker(), duplicates: &observed.duplicates,
	}
	consumer, err := natsadapter.NewEventConsumer(
		connection,
		store,
		event,
		app.handleOrder,
		natsadapter.HandlerConfig{
			Stream: demoStream, Namespace: demoNamespace, ConsumerID: demoConsumerID,
			WireMode: natsadapter.WireNative, Concurrency: 1, Timeout: 5 * time.Second,
			FinalizationTimeout: 2 * time.Second, MaxAttempts: 3,
			BaseRetry: 250 * time.Millisecond, MaxRetry: 500 * time.Millisecond,
			AckWait: 2 * time.Second, DLQSubject: demoDLQSubject,
			Replicas: 1, MemoryStorage: true, Logger: messenger.AdaptSlog(log),
		},
	)
	if err != nil {
		_ = outboxRuntime.Close()
		return nil, nil, fmt.Errorf("create durable consumer: %w", err)
	}
	return app, consumer, nil
}

func (a *demoApp) stageOrder(ctx context.Context, payload orderCreated) (messenger.Receipt, error) {
	var receipt messenger.Receipt
	err := a.outbox.Transactor().RunInTx(ctx, func(txCtx context.Context) error {
		tx := outboxstorage.GetTx(txCtx)
		if tx == nil {
			return errors.New("missing Outbox business transaction")
		}
		if _, err := tx.Exec(txCtx, `INSERT INTO demo.orders(id, amount, scenario) VALUES ($1, $2, $3)`,
			payload.OrderID, payload.Amount, payload.Scenario); err != nil {
			return fmt.Errorf("insert business order: %w", err)
		}
		var publishErr error
		receipt, publishErr = a.bus.Publish(txCtx, a.event, payload)
		return publishErr
	})
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("commit business order and Outbox event: %w", err)
	}
	if receipt.State != messenger.ReceiptStaged || receipt.MessageID.IsZero() {
		return messenger.Receipt{}, fmt.Errorf("unexpected Outbox receipt: %#v", receipt)
	}
	a.log.Info("business order and event staged atomically",
		"order_id", payload.OrderID, "message_id", receipt.MessageID.String())
	return receipt, nil
}

func (a *demoApp) handleOrder(ctx context.Context, message messenger.Message[orderCreated]) error {
	payload := message.Payload
	attempt := a.attempts.next(payload.OrderID)
	tx, ok := inbox.SQLTxFromContext(ctx)
	if !ok {
		return errors.New("missing Inbox transaction")
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO demo.order_projection(order_id, amount, handler_attempt) VALUES ($1, $2, $3)`,
		payload.OrderID, payload.Amount, attempt); err != nil {
		return fmt.Errorf("write order projection: %w", err)
	}

	switch payload.Scenario {
	case scenarioRetry:
		if attempt == 1 {
			a.log.Info("handler requests retry after writing; Inbox will roll the write back",
				"order_id", payload.OrderID, "attempt", attempt)
			return messenger.RetryAfter(errors.New("inventory temporarily unavailable"), 300*time.Millisecond)
		}
	case scenarioDLQ:
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

func migrate(ctx context.Context, db *sql.DB) error {
	if err := outboxmigrator.RunEmbedded(
		ctx,
		db,
		outboxlogger.Discard(),
		outboxmigrator.WithCommand("up"),
	); err != nil {
		return fmt.Errorf("migrate Outbox: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS demo`); err != nil {
		return fmt.Errorf("create demo schema: %w", err)
	}
	inboxOptions := []inboxpgsql.Option{
		inboxpgsql.WithSchema(demoNamespace),
		inboxpgsql.WithTablePrefix("gm_"),
	}
	if err := inboxpgsql.Migrate(ctx, db, inboxOptions...); err != nil {
		return fmt.Errorf("migrate Inbox: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo.orders (
		id TEXT PRIMARY KEY,
		amount BIGINT NOT NULL,
		scenario TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create demo orders: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS demo.order_projection (
		order_id TEXT PRIMARY KEY,
		amount BIGINT NOT NULL,
		handler_attempt INTEGER NOT NULL,
		handled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create demo projection: %w", err)
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

func waitForNATS(ctx context.Context, url string) (*natsio.Conn, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		connection, err := natsio.Connect(url,
			natsio.Name("gomessenger-durable-demo"),
			natsio.Timeout(time.Second),
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

func waitReady(
	ctx context.Context,
	name string,
	readiness func(context.Context) error,
	r *runner,
) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := readiness(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s readiness: %w", name, ctx.Err())
		case <-r.done:
			runErr := r.result()
			if runErr == nil {
				return fmt.Errorf("%s stopped before readiness", name)
			}
			return fmt.Errorf("%s stopped before readiness: %w", name, runErr)
		case <-ticker.C:
		}
	}
}

func waitFor(ctx context.Context, description string, condition func() (bool, error)) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if err != nil {
			return fmt.Errorf("wait for %s: %w", description, err)
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s: %w", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForSource(
	ctx context.Context,
	subscription *natsio.Subscription,
	messageID messenger.MessageID,
) (*natsio.Msg, error) {
	for {
		message, err := subscription.NextMsg(250 * time.Millisecond)
		if errors.Is(err, natsio.ErrTimeout) {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("wait for source message %s: %w", messageID, ctx.Err())
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read source message: %w", err)
		}
		envelope, err := messenger.UnmarshalEnvelope(message.Data)
		if err == nil && envelope.ID == messageID {
			return message, nil
		}
	}
}

func waitForDLQ(
	ctx context.Context,
	subscription *natsio.Subscription,
	messageID messenger.MessageID,
) (natsadapter.DLQRecord, error) {
	for {
		message, err := subscription.NextMsg(250 * time.Millisecond)
		if errors.Is(err, natsio.ErrTimeout) {
			if ctx.Err() != nil {
				return natsadapter.DLQRecord{}, fmt.Errorf("wait for DLQ message %s: %w", messageID, ctx.Err())
			}
			continue
		}
		if err != nil {
			return natsadapter.DLQRecord{}, fmt.Errorf("read DLQ message: %w", err)
		}
		record, err := natsadapter.DecodeDLQRecord(message.Data)
		if err != nil {
			return natsadapter.DLQRecord{}, fmt.Errorf("decode DLQ message: %w", err)
		}
		envelope, err := messenger.UnmarshalEnvelope(record.Envelope)
		if err == nil && envelope.ID == messageID {
			return record, nil
		}
	}
}

func publishDistinctDuplicate(
	ctx context.Context,
	js jetstream.JetStream,
	original *natsio.Msg,
	messageID messenger.MessageID,
) error {
	header := cloneHeader(original.Header)
	header.Del(natsio.MsgIdHdr)
	_, err := js.PublishMsg(ctx, &natsio.Msg{
		Subject: original.Subject,
		Header:  header,
		Data:    bytes.Clone(original.Data),
	}, jetstream.WithMsgID("demo-distinct-delivery-"+messageID.String()))
	if err != nil {
		return fmt.Errorf("publish distinct duplicate: %w", err)
	}
	return nil
}

func cloneHeader(source natsio.Header) natsio.Header {
	result := make(natsio.Header, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func projectionExists(ctx context.Context, db *sql.DB, orderID string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM demo.order_projection WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		return false, fmt.Errorf("query order projection: %w", err)
	}
	return count == 1, nil
}

func randomID() (string, error) {
	var value [8]byte
	if _, err := crand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate demo run ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
