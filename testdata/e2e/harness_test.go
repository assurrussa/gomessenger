package e2e_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	outboxmigrator "github.com/assurrussa/outbox/backends/sqlite/migrator"
	outboxruntime "github.com/assurrussa/outbox/backends/sqlite/runtime"
	outboxtx "github.com/assurrussa/outbox/backends/sqlite/storage/transaction"
	outboxlogger "github.com/assurrussa/outbox/outbox/logger"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "modernc.org/sqlite"
)

const (
	testNamespace = "e2e"
	testStream    = "GOMESSENGER_E2E"
	testDLQ       = "e2e.dlq"
)

type processPayload struct {
	JobID int64 `json:"jobId"`
}

type traceContextKey struct{}

type e2eTracePropagator struct{}

func (e2eTracePropagator) Inject(_ context.Context, carrier map[string]string) {
	carrier["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	carrier["tracestate"] = "vendor=value"
}

func (e2eTracePropagator) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return context.WithValue(ctx, traceContextKey{}, carrier["traceparent"]+"|"+carrier["tracestate"])
}

type testHarness struct {
	command    messenger.Command[processPayload]
	server     *server.Server
	relayConn  *natsio.Conn
	outbox     *outboxruntime.Runtime
	consumerDB *sql.DB
	inbox      *inbox.Store
	duplicates *atomic.Int32
	bus        *messenger.Messenger
}

type observingInboxBackend struct {
	delegate   *inbox.Store
	duplicates atomic.Int32
}

func (b *observingInboxBackend) Process(
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

func (b *observingInboxBackend) ProcessAttempt(
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

func (b *observingInboxBackend) ForgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	return b.delegate.ForgetAttempt(ctx, key, fingerprint)
}

func (b *observingInboxBackend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	maxAttempts uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	result, err := b.delegate.ProcessBatchAttempt(ctx, items, maxAttempts, handler)
	if err == nil {
		for _, item := range result.Items {
			if item.Duplicate {
				b.duplicates.Add(1)
			}
		}
	}
	return result, err
}

func (b *observingInboxBackend) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	return b.delegate.Prune(ctx, before, limit)
}

type serviceRunner struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	instance, err := server.NewServer(&server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		MaxPayload: natsadapter.DefaultMaxDLQMessageBytes,
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	instance.Start()
	if !instance.ReadyForConnections(10 * time.Second) {
		instance.Shutdown()
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() {
		instance.Shutdown()
		instance.WaitForShutdown()
	})

	relayConnection := connectNATS(t, instance.ClientURL())
	if _, err := natsadapter.ApplyTopology(t.Context(), relayConnection, natsadapter.Topology{
		SpecVersion: "1.0",
		Streams: []natsadapter.StreamSpec{
			natsadapter.DevStream(testStream, testNamespace+".command.>", testNamespace+".event.>"),
			natsadapter.DevDLQStream(testStream+"_DLQ", testDLQ),
		},
	}); err != nil {
		t.Fatalf("apply NATS topology: %v", err)
	}

	producerPath := filepath.Join(t.TempDir(), "producer.db")
	outboxRuntime, err := outboxruntime.Open(t.Context(), outboxruntime.Config{
		DSN:        producerPath,
		Workers:    1,
		IdleTime:   100 * time.Millisecond,
		ReserveFor: 2 * time.Second,
		Logger:     outboxlogger.Discard(),
	})
	if err != nil {
		t.Fatalf("open outbox runtime: %v", err)
	}
	t.Cleanup(func() {
		if err := outboxRuntime.Close(); err != nil {
			t.Errorf("close outbox runtime: %v", err)
		}
	})
	if err := outboxmigrator.RunEmbedded(
		t.Context(),
		outboxRuntime.Client().DB(),
		outboxlogger.Discard(),
		outboxmigrator.WithCommand("up"),
	); err != nil {
		t.Fatalf("migrate outbox database: %v", err)
	}
	if _, err := outboxRuntime.Client().DB().ExecContext(t.Context(), `CREATE TABLE producer_jobs (
		id INTEGER PRIMARY KEY,
		status TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create producer table: %v", err)
	}

	consumerDatabase, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatalf("open consumer database: %v", err)
	}
	consumerDatabase.SetMaxOpenConns(1)
	consumerDatabase.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := consumerDatabase.Close(); err != nil {
			t.Errorf("close consumer database: %v", err)
		}
	})
	if _, err := consumerDatabase.ExecContext(t.Context(), "PRAGMA busy_timeout=5000"); err != nil {
		t.Fatalf("configure consumer database: %v", err)
	}
	if err := inboxsqlite.Migrate(t.Context(), consumerDatabase); err != nil {
		t.Fatalf("migrate inbox database: %v", err)
	}
	if _, err := consumerDatabase.ExecContext(t.Context(), `CREATE TABLE business_counters (
		name TEXT PRIMARY KEY,
		value INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create consumer table: %v", err)
	}
	if _, err := consumerDatabase.ExecContext(
		t.Context(),
		`INSERT INTO business_counters(name, value) VALUES ('processed', 0)`,
	); err != nil {
		t.Fatalf("seed consumer counter: %v", err)
	}
	durableInbox, err := inboxsqlite.New(consumerDatabase)
	if err != nil {
		t.Fatalf("create inbox store: %v", err)
	}
	observedInbox := &observingInboxBackend{delegate: durableInbox}
	inboxStore, err := inbox.New(observedInbox)
	if err != nil {
		t.Fatalf("create observing inbox store: %v", err)
	}

	brokerRoute, err := natsadapter.NewRoute(relayConnection, natsadapter.RouteConfig{
		Name: "nats.e2e", Namespace: testNamespace, WireMode: natsadapter.WireNative,
	})
	if err != nil {
		t.Fatalf("create NATS route: %v", err)
	}
	relayJob, err := outboxadapter.NewRelayJob(brokerRoute, outboxadapter.RelayJobConfig{
		ExecutionTimeout: 2 * time.Second,
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("create relay job: %v", err)
	}
	if err := outboxRuntime.Service().RegisterJob(relayJob); err != nil {
		t.Fatalf("register relay job: %v", err)
	}
	producerRoute, err := outboxadapter.NewProducer(outboxRuntime.Service(), outboxadapter.ProducerConfig{
		Name: "outbox.e2e",
	})
	if err != nil {
		t.Fatalf("create outbox producer: %v", err)
	}
	command := messenger.MustCommand("jobs.process", 1, messenger.JSON[processPayload]())
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:e2e-producer"),
		messenger.WithContextPropagator(e2eTracePropagator{}),
	)
	builder.RouteCommand(command, producerRoute)
	bus, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build messenger: %v", err)
	}

	return &testHarness{
		command: command, server: instance, relayConn: relayConnection,
		outbox: outboxRuntime, consumerDB: consumerDatabase, inbox: inboxStore,
		duplicates: &observedInbox.duplicates, bus: bus,
	}
}

func connectNATS(t *testing.T, url string) *natsio.Conn {
	t.Helper()
	connection, err := natsio.Connect(url, natsio.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	return connection
}

func (h *testHarness) stage(
	t *testing.T,
	jobID int64,
	rollback error,
) (messenger.Receipt, error) {
	t.Helper()
	var receipt messenger.Receipt
	err := h.outbox.Transactor().RunInTx(t.Context(), func(ctx context.Context) error {
		tx := outboxtx.GetTx(ctx)
		if tx == nil {
			return errors.New("producer transaction missing from context")
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO producer_jobs(id, status) VALUES (?, ?)`,
			jobID,
			"created",
		); err != nil {
			return fmt.Errorf("insert producer job: %w", err)
		}
		var err error
		receipt, err = h.bus.Send(ctx, h.command, processPayload{JobID: jobID})
		if err != nil {
			return fmt.Errorf("stage command: %w", err)
		}
		return rollback
	})
	return receipt, err
}

func (h *testHarness) newConsumer(
	t *testing.T,
	connection *natsio.Conn,
	consumerID string,
	handler messenger.Handler[processPayload],
	configure func(*natsadapter.HandlerConfig),
) *natsadapter.Consumer {
	t.Helper()
	config := natsadapter.HandlerConfig{
		Stream: testStream, Namespace: testNamespace, ConsumerID: consumerID,
		WireMode: natsadapter.WireNative, Concurrency: 1, Timeout: 2 * time.Second,
		MaxAttempts: 3, BaseRetry: 25 * time.Millisecond, MaxRetry: 250 * time.Millisecond,
		AckWait: 300 * time.Millisecond, DLQSubject: testDLQ, Replicas: 1, MemoryStorage: true,
		Propagator: e2eTracePropagator{},
	}
	if configure != nil {
		configure(&config)
	}
	consumer, err := natsadapter.NewCommandConsumer(connection, h.inbox, h.command, handler, config)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return consumer
}

func (h *testHarness) newBatchConsumer(
	t *testing.T,
	connection *natsio.Conn,
	consumerID string,
	handler messenger.BatchHandler[processPayload],
	batchConfig messenger.BatchConfig,
) *natsadapter.Consumer {
	t.Helper()
	config := natsadapter.HandlerConfig{
		Stream: testStream, Namespace: testNamespace, ConsumerID: consumerID,
		WireMode: natsadapter.WireNative, Concurrency: 1, Timeout: 2 * time.Second,
		MaxAttempts: 3, BaseRetry: 25 * time.Millisecond, MaxRetry: 250 * time.Millisecond,
		AckWait: 300 * time.Millisecond, DLQSubject: testDLQ, Replicas: 1, MemoryStorage: true,
		Propagator: e2eTracePropagator{},
	}
	consumer, err := natsadapter.NewBatchCommandConsumer(
		connection,
		h.inbox,
		h.command,
		handler,
		config,
		batchConfig,
	)
	if err != nil {
		t.Fatalf("create batch consumer: %v", err)
	}
	return consumer
}

func (h *testHarness) startOutbox(t *testing.T) *serviceRunner {
	t.Helper()
	runner := startService(t, h.outbox.Run, false)
	eventually(t, func() (bool, error) {
		err := h.outbox.Readiness(t.Context())
		return err == nil, err
	})
	return runner
}

func startConsumer(
	t *testing.T,
	consumer *natsadapter.Consumer,
	allowRunError bool,
) *serviceRunner {
	t.Helper()
	runner := startService(t, consumer.Run, allowRunError)
	eventually(t, func() (bool, error) {
		err := consumer.Readiness(t.Context())
		return err == nil, err
	})
	return runner
}

func startService(
	t *testing.T,
	run func(context.Context) error,
	allowRunError bool,
) *serviceRunner {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	runner := &serviceRunner{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(runner.done)
		runner.err = run(ctx)
	}()
	t.Cleanup(func() {
		runner.cancel()
		select {
		case <-runner.done:
			if runner.err != nil && !allowRunError {
				t.Errorf("service run: %v", runner.err)
			}
		case <-time.After(5 * time.Second):
			t.Error("service did not stop during cleanup")
		}
	})
	return runner
}

func stopService(t *testing.T, runner *serviceRunner) error {
	t.Helper()
	runner.cancel()
	select {
	case <-runner.done:
		return runner.err
	case <-time.After(5 * time.Second):
		t.Fatal("service did not stop")
		return nil
	}
}

func waitService(t *testing.T, runner *serviceRunner) error {
	t.Helper()
	select {
	case <-runner.done:
		return runner.err
	case <-time.After(5 * time.Second):
		t.Fatal("service did not return")
		return nil
	}
}

func (h *testHarness) incrementBusiness(ctx context.Context) error {
	tx, ok := inbox.SQLTxFromContext(ctx)
	if !ok {
		return errors.New("inbox transaction missing from context")
	}
	result, err := tx.ExecContext(ctx, `UPDATE business_counters SET value = value + 1 WHERE name = 'processed'`)
	if err != nil {
		return fmt.Errorf("increment business counter: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("business counter affected rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("business counter affected %d rows", rows)
	}
	return nil
}

func (h *testHarness) businessCount(t *testing.T) int64 {
	t.Helper()
	var count int64
	if err := h.consumerDB.QueryRowContext(
		t.Context(),
		`SELECT value FROM business_counters WHERE name = 'processed'`,
	).Scan(&count); err != nil {
		t.Fatalf("read business counter: %v", err)
	}
	return count
}

func (h *testHarness) producerRows(t *testing.T) int64 {
	t.Helper()
	var count int64
	if err := h.outbox.Client().DB().QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM producer_jobs`,
	).Scan(&count); err != nil {
		t.Fatalf("count producer rows: %v", err)
	}
	return count
}

func (h *testHarness) waitOutboxEmpty(t *testing.T) {
	t.Helper()
	eventually(t, func() (bool, error) {
		stats, err := h.outbox.Service().GetQueueStats(t.Context())
		return err == nil && stats.Total == 0, err
	})
}

func (h *testHarness) outboxTotal(t *testing.T) int64 {
	t.Helper()
	stats, err := h.outbox.Service().GetQueueStats(t.Context())
	if err != nil {
		t.Fatalf("read outbox stats: %v", err)
	}
	return stats.Total
}

func (h *testHarness) waitConsumerEmpty(t *testing.T, consumerID string) {
	t.Helper()
	js, err := jetstream.New(h.relayConn)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	eventually(t, func() (bool, error) {
		consumer, err := js.Consumer(t.Context(), testStream, consumerID)
		if err != nil {
			return false, err
		}
		info, err := consumer.Info(t.Context())
		if err != nil {
			return false, err
		}
		return info.NumAckPending == 0 && info.NumPending == 0, nil
	})
}

func (h *testHarness) streamMessages(t *testing.T) uint64 {
	t.Helper()
	js, err := jetstream.New(h.relayConn)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	stream, err := js.Stream(t.Context(), testStream)
	if err != nil {
		t.Fatalf("open test stream: %v", err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatalf("read test stream: %v", err)
	}
	return info.State.Msgs
}

func eventually(
	t *testing.T,
	condition func() (bool, error),
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		ok, err := condition()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached: %v", lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
