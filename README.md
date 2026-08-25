# GoMessenger

GoMessenger is a typed CQRS toolkit for Go 1.27+: commands, local request/reply queries, and events share one explicit
descriptor, handler, middleware, lifecycle, and observability facade. One-way commands and events can additionally use
transactional outbox staging, NATS JetStream or Kafka delivery, and durable inbox deduplication.

The root module is intentionally small: standard library plus
[`gobus`](https://github.com/assurrussa/gobus). Broker, SQL, Prometheus, and OpenTelemetry dependencies live in optional
nested modules.

## When to use it

Use GoMessenger when an application wants one typed command/query/event facade, or when a one-way message may cross a
process boundary or must survive a restart. Local `Query[Q,R]` delegates to GoBus result dispatch but adds descriptors,
metadata lineage, middleware, observations, DI binding, and bounded async isolation.

Distributed queries are not implemented. A local query never uses `Delivery`, a native envelope, `Receipt`, Outbox,
Inbox, NATS, Kafka, retry, or DLQ. Remote request/reply requires the separate contract in
[ADR-0003](docs/decisions/0003-distributed-queries.md). GoMessenger is not a replacement for a database transaction,
workflow engine, or streaming analytics platform.

The durable contract is **at-least-once**:

- an outbox route reports success only after the envelope is staged in the caller's database transaction;
- a direct NATS route reports success only after a JetStream `PubAck`;
- a direct Kafka route reports success only after its producer transaction commits;
- a durable consumer acknowledges only after its inbox transaction and handler commit;
- stable message identity and an inbox prevent repeating the same committed consumer transaction;
- external side effects still need their own idempotency key or transactional boundary. The library does not claim
  exactly-once effects.

## Requirements and modules

GoMessenger requires Go 1.27 because the builder and messenger expose generic methods.

| Module                              | Responsibility                                                   |
|-------------------------------------|------------------------------------------------------------------|
| `github.com/assurrussa/gomessenger` | CQRS descriptors, local queries, envelopes, local routes, runtime, manifest |
| `.../adapters/outbox`               | transactional staging and a broker relay job                     |
| `.../adapters/nats`                 | JetStream producers, consumers, topology, CloudEvents            |
| `.../adapters/kafka`                | transactional Kafka producers, consumers, topology, retry/DLQ   |
| `.../adapters/inbox`                | atomic PostgreSQL and SQLite consumer deduplication              |
| `.../observability`                 | Prometheus, OpenTelemetry spans, W3C Trace Context                |
| `.../tools/gomessengerctl`          | manifest/topology validation, plan/apply, DLQ inspect/replay      |

The initial GoMessenger release is being prepared as `v0.1.0`. Outbox root and SQLite backend `v0.11.0` are already
published and are the pinned durable-producer dependencies. During repository development `go.work` selects local
GoMessenger modules; published consumer modules must use path-qualified tags and no local `replace` directives.

## How to use it

Start with one explicit command, query, or event descriptor, select its route, build the messenger, and inject a narrow
`Sender`, `Querier`, or `Publisher` into business code. Durable consumers are separate managed services attached to the
returned `Runtime`.

```text
business code -> Messenger.Query -> local result handler -> typed result
             `-> Send/Publish -> local handler
                              `-> Outbox -> relay -> JetStream/Kafka -> consumer -> Inbox transaction -> handler -> ACK/offset
```

Use the route that matches the required success boundary:

- `NewLocalSyncRoute` returns after the in-process handler completes;
- for a query, `NewLocalSyncRoute` returns the handler result and `NewLocalAsyncRoute` queues bounded work but
  `Messenger.Query` still waits for exactly one result;
- `natsadapter.NewRoute` returns after JetStream `PubAck`;
- `kafkaadapter.NewRoute` returns after a Kafka producer transaction commits;
- `outboxadapter.NewProducer` returns after staging inside the caller's transaction. The receipt is provisional until
  that transaction commits.

For a durable flow, provision topology, apply Outbox and Inbox migrations, register the relay before producer traffic,
start the managed runtimes, and expose readiness before accepting traffic. See the
[practical usage guide](docs/usage.md) for the complete producer, consumer, failure, and shutdown sequence.

## Minimal local example

```go
type ResizeMedia struct {
	JobID int64 `json:"jobId"`
}

resize := messenger.MustCommand("media.resize", 1, messenger.JSON[ResizeMedia]())
builder := messenger.NewBuilder(messenger.WithSource("urn:service:media-resizer"))
builder.HandleCommandFunc(resize, "media-worker", func(_ context.Context, payload ResizeMedia) error {
	fmt.Println("resize", payload.JobID)
	return nil
})
builder.RouteCommand(resize, messenger.NewLocalSyncRoute())

bus, _, err := builder.Build()
if err != nil {
	return err
}
resizeSender := messenger.BindSender(bus, resize)
_, err = resizeSender.Send(ctx, ResizeMedia{JobID: 42})
```

The equivalent runnable API is compiled as `ExampleMessenger_Send`; `testdata/consumer` separately compiles the
complete public facade from an external Go module.

## Minimal local query example

```go
type FindArticle struct{ ID int64 }
type ArticleView struct {
	ID    int64
	Title string
}

findArticle := messenger.MustQuery[FindArticle, ArticleView](
	"article.find", 1, messenger.JSON[FindArticle](),
)
builder := messenger.NewBuilder(messenger.WithSource("urn:service:catalog"))
builder.HandleQueryFunc(findArticle, "article-reader", func(_ context.Context, query FindArticle) (ArticleView, error) {
	return ArticleView{ID: query.ID, Title: "CQRS in Go"}, nil
})
builder.RouteQuery(findArticle, messenger.NewLocalSyncRoute())

bus, _, err := builder.Build()
if err != nil {
	return err
}
reader := messenger.BindQuerier(bus, findArticle)
article, err := reader.Query(ctx, FindArticle{ID: 42})
```

`Query[Q,R]` uses the codec and schema only for request identity; `R` is an in-process type identity and is not written
to the manifest. Every registered query must have exactly one handler and one built-in local route. The async route
uses the caller context for admission, execution, and waiting, so cancellation returns `ctx.Err()` and accepted result
delivery cannot block runtime drain. The runnable version is compiled as `ExampleMessenger_Query`.

Descriptors use explicit stable wire names, schema versions, content types, data encodings, and optional schema URIs.
Go type names and package paths never become the wire contract implicitly. Native envelopes carry `dataEncoding` as
`json`, `text`, or `binary`, so custom codecs do not infer their representation from a media-type prefix.

## Transactional outbox producer

Use `adapters/outbox` when a business write and message publication must commit or roll back together. Register the
broker relay before the producer can stage its job:

```go
natsRoute, err := natsadapter.NewRoute(natsConnection, natsadapter.RouteConfig{
	Name:      "nats.integration-events",
	Namespace: "prod",
	WireMode:  natsadapter.WireNative,
})
if err != nil {
	return err
}
relay, err := outboxadapter.NewRelayJob(natsRoute, outboxadapter.RelayJobConfig{})
if err != nil {
	return err
}
if err := outboxRuntime.Service().RegisterJob(relay); err != nil {
	return err
}

route, err := outboxadapter.NewProducer(
	outboxRuntime.Service(),
	outboxadapter.ProducerConfig{Name: "outbox.integration-events"},
)
if err != nil {
	return err
}
builder := messenger.NewBuilder(
	messenger.WithSource("urn:service:media-resizer"),
)
builder.RouteEvent(mediaResized, route)

bus, _, err := builder.Build()
if err != nil {
	return err
}

err = outboxRuntime.Transactor().RunInTx(ctx, func(txCtx context.Context) error {
	if err := mediaRepository.Save(txCtx, media); err != nil {
		return err
	}
	_, err := bus.Publish(txCtx, mediaResized, MediaResized{JobID: media.ID})
	return err
})
```

The repository write must use the transaction carried by `txCtx`. `ReceiptStaged` reports staging in that active
transaction; a callback rollback still removes the row. The producer stores the canonical envelope under its
`MessageID`; the relay later publishes those exact bytes and waits for `PubAck`. Repeating identical content resolves the
same outbox tombstone, while reusing the identity with different content fails closed. Immediate messages use their
immutable message time as `availableAt`, preventing retry-time fingerprint drift.

`messenger.RetryAfter` from the broker route becomes a persisted `outbox.RetryAt`; permanent envelope failures move
directly to the outbox DLQ.

## Durable JetStream consumer and inbox

A consumer owns one stable `ConsumerID`, explicit concurrency and retry bounds, and one SQL inbox:

```go
if err := inboxpgsql.Migrate(ctx, database); err != nil {
	return err
}
store, err := inboxpgsql.New(database)
if err != nil {
	return err
}
consumer, err := natsadapter.NewEventConsumer(
	natsConnection,
	store,
	mediaResized,
	func(ctx context.Context, message messenger.Message[MediaResized]) error {
		tx, ok := inbox.SQLTxFromContext(ctx)
		if !ok {
			return errors.New("missing inbox transaction")
		}
		return projection.ApplyTx(ctx, tx, message.Metadata.ID.String(), message.Payload)
	},
	natsadapter.HandlerConfig{
		Stream: "MESSAGES", Namespace: "prod", ConsumerID: "media-projection", WireMode: natsadapter.WireNative,
		Concurrency: 8, Timeout: 30 * time.Second, FinalizationTimeout: 5 * time.Second,
		AckWait: 30 * time.Second, MaxAttempts: 10,
		DLQSubject: "prod.dlq",
	},
)
if err != nil {
	return err
}
consumerBuilder := messenger.NewBuilder(
	messenger.WithSource("urn:service:media-projection"),
)
consumerBuilder.Use("consumer.media-projection", consumer)

_, runtime, err := consumerBuilder.Build()
if err != nil {
	return err
}
```

Run the embedded additive inbox migrations explicitly with `inboxpgsql.Migrate` or `inboxsqlite.Migrate` before
constructing consumers. They include the durable handler-attempt count and permanent-outcome state required by NATS
consumers. Hosts own database connections, migration ordering, NATS connections, credentials, and process supervision.

Each active consumer worker may hold one SQL transaction for the full handler invocation. When several consumers share
one `sql.DB`, configure `SetMaxOpenConns` to at least the sum of their `HandlerConfig.Concurrency` values, plus explicit
headroom for application and maintenance queries. A smaller pool turns configured consumer concurrency into database
connection contention and can exhaust handler deadlines before application code runs.

`Timeout` bounds application handler execution. The Inbox transaction receives an additional
`FinalizationTimeout` (5 seconds by default) to commit or roll back after that deadline. Increase it when a remote or
otherwise slow database needs more finalization time; it does not extend the handler deadline.

Inbox table names are currently fixed as `gomessenger_inbox`, `gomessenger_inbox_attempts`, and
`gomessenger_inbox_attempt_generations`; there is no table-prefix or schema option. PostgreSQL hosts that use a
non-default schema must apply migrations and configure the same stable connection-level `search_path` for every pooled
connection. SQLite uses the fixed names in the selected database file.

Provision source and DLQ capacity separately. `DevStream` keeps native source messages at the 1 MiB envelope bound;
`DevDLQStream` reserves `DefaultMaxDLQMessageBytes` for the expanded JSON DLQ record. The NATS server/account
`max_payload` must be at least the same value. `Consumer.Run` and `Readiness` reject a missing or undersized DLQ route
before reporting ready.

## Middleware, logging, and tracing

Global middleware wraps local query/command/event handlers and can be reused by durable NATS consumers. Registration order is execution
order: the first middleware is outermost. A middleware may replace the context or short-circuit, but `next` may be
called at most once. Typed one-way decorators use `messenger.ChainHandler`; typed query decorators use
`messenger.ChainQueryHandler` and may return a cached or synthetic `R`. Global middleware cannot synthesize a typed
result: successful completion without one returns `ErrQueryResultMissing`.

```go
logger := messenger.AdaptSlog(slog.Default())
builder := messenger.NewBuilder(
	messenger.WithSource("urn:service:media-resizer"),
	messenger.WithLogger(logger),
	messenger.WithObserver(messenger.NewLoggingObserver(logger)),
	messenger.WithContextPropagator(observability.NewTraceContextPropagator()),
)
builder.UseMiddleware(func(
	ctx context.Context,
	metadata messenger.Metadata,
	handlerID string,
	next messenger.HandlerFunc,
) error {
	return next(ctx)
})
```

Logging is disabled by default. `AdaptSlog(nil)` is a safe no-op adapter; direct `WithLogger(nil)` is a configuration
error. Observer registrations are additive. A panic in one observer is logged and isolated from the remaining
observers. Kafka `TransportConfig.Logger` reports adapter-owned startup/readiness, producer and consumer transaction,
abort/fencing, and topology failures or applied changes. Core logging contains infrastructure state only and never logs
record keys, payloads, message bodies, or arbitrary headers. `WithClientLogger` is a separate explicit opt-in to
franz-go's own client logs.

The observability propagator carries only W3C `traceparent` and `tracestate`. It works through native envelopes,
CloudEvents structured/binary modes, and transactional Outbox storage. Baggage is intentionally not supported yet.

## Runtime lifecycle

Run the managed services under the host supervisor. On a signal, close admission first and then wait for accepted work
within a deadline:

```go
signalCtx, stopSignals := signal.NotifyContext(
	context.Background(),
	os.Interrupt,
	syscall.SIGTERM,
)
defer stopSignals()

runErr := make(chan error, 1)
go func() {
	runErr <- runtime.Run(context.Background())
}()

select {
case err := <-runErr:
	return err
case <-signalCtx.Done():
}

runtime.BeginDrain()
shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
if err := runtime.Shutdown(shutdownCtx); err != nil {
	return err
}
return <-runErr
```

`Runtime.Readiness` checks the runtime and every attached service. The selected Outbox backend runtime has its own
host-supervised lifecycle; GoMessenger does not open connections or supervise it implicitly. Never call `Shutdown` from
a handler executing on the same runtime.

## Failure semantics

- Return `messenger.Permanent(err)` for a non-retryable handler failure.
- Return `messenger.RetryAfter(err, delay)` for an explicit durable delay.
- Other errors use bounded full-jitter exponential retry in the NATS and Kafka adapters.
- `MaxAttempts` bounds handler invocations, not broker deliveries. A `NotBefore` deferral does not invoke the handler or
  consume an attempt.
- `AckWait` is a broker redelivery deadline, not a handler timeout. It must be at least 100 ms and may be shorter than
  `Timeout`; active handlers refresh acknowledgement progress every `AckWait / 3`.
- The NATS adapter publishes and confirms a DLQ record before broker-confirming acknowledgement of the original message.
  Failed terminal hand-off operations retry on the current delivery without invoking the application handler beyond
  `MaxAttempts`. A permanent outcome is persisted independently of the attempt count, so an interrupted hand-off cannot
  invoke that handler again after restart.
- `NotBefore` becomes a retry delay until due; `ExpiresAt` becomes a permanent expired outcome.

## Kafka

The independent Kafka module provides native-envelope transactional publish, Outbox relay, read-committed consumers,
consumer-specific retry topics, atomic retry/DLQ offset hand-off, protected replay, static worker identity, and
non-destructive topic planning. It intentionally does not reuse the NATS engine or support CloudEvents/query transport.
Retry may be overtaken by later source records, so ordering is guaranteed only before the first failure. See the
[Kafka adapter guide](docs/kafka.md) and [ADR-0004](docs/decisions/0004-kafka-adapter.md).

## CloudEvents

The NATS adapter supports the native envelope for commands and events, plus CloudEvents 1.0 structured and binary modes
for events. Native metadata is mapped explicitly; delivery-attempt state never enters the canonical envelope or
fingerprint. An omitted CloudEvents `time` is derived deterministically from a UUIDv7 event ID; events without `time`
and with a non-UUIDv7 ID are rejected so retries cannot change the canonical fingerprint. The required `dataencoding`
extension carries `json`, `text`, or `binary`; missing, invalid, or descriptor-conflicting values are rejected before the
payload codec runs.

## Topology and CLI

Topology management is declarative and non-destructive. It may create missing resources or apply compatible additive
changes. Retention/storage/replica changes, subject removal, reduced limits, or consumer delivery-contract drift are
conflicts; the tool never deletes and recreates resources automatically. Compatible updates preserve broker settings
outside the manifest's managed subset.

```sh
gomessengerctl manifest validate --file manifest.json
gomessengerctl topology validate --file topology.json
gomessengerctl topology plan --file topology.json --server nats://localhost:4222
gomessengerctl topology apply --file topology.json --server nats://localhost:4222
gomessengerctl dlq inspect --file record.json
gomessengerctl dlq replay --file record.json
gomessengerctl dlq replay --file record.json --confirm --server nats://localhost:4222
gomessengerctl kafka topology validate --file kafka-topology.json
gomessengerctl kafka topology plan --file kafka-topology.json --brokers localhost:9092 --instance-id ops-a
gomessengerctl kafka topology apply --file kafka-topology.json --brokers localhost:9092 --instance-id ops-a
gomessengerctl kafka dlq inspect --file kafka-record.json
gomessengerctl kafka dlq replay --file kafka-record.json
gomessengerctl kafka dlq replay --file kafka-record.json --confirm --brokers localhost:9092 --instance-id ops-a
```

`topology plan` exits with code `3` when the printed plan contains a conflict.
`dlq inspect` prints a safe summary and replayability status without handler error text, wire bytes, or header values.
`dlq replay` is offline by default and prints a payload-free deterministic JSON plan. `--confirm` republishes the
original subject, wire bytes, and bounded headers with a deterministic, DLQ-record-specific `Nats-Msg-Id`, then waits
for JetStream `PubAck`. The target consumer starts one fresh bounded attempt generation even if post-ACK Inbox cleanup
was interrupted; broker redeliveries of that replay retain the same generation and do not reset `MaxAttempts`. Replay
does not support subject substitution, record deletion, or payload/header output. Its internal replay headers are
reserved transport metadata and must not be injected by ordinary publishers; enforce that boundary with NATS publish
permissions when producers can access subjects directly.

## Development and verification

```sh
make prepare
make check
make test-e2e
make test-integration
make test-kafka
GOMESSENGER_POSTGRES_DSN='postgres://...' make test-postgres
make bench-all
```

`make check` covers every module, static lint, race and checkptr builds, a 90% root coverage gate, an isolated
clean-consumer module, and the Docker-free durable pipeline E2E. `make test-e2e` reruns only that full
Outbox-to-JetStream-to-Inbox path. `make test-kafka` is the separate local Docker gate against official Kafka 4.1.2
and 4.3.1 images; it is intentionally not a hosted-CI service. Release requirements are prepared and checked explicitly:

```sh
make check
make release-ready VERSION=v0.1.0 OUTBOX_VERSION=v0.11.0
make release-readiness VERSION=v0.1.0 OUTBOX_VERSION=v0.11.0
```

Run the full source gate before `release-ready` removes development replacements. `release-readiness` then checks the
exact tag declarations without trying to download GoMessenger tags that do not exist yet.

A published release is verified separately, after all dependency-ordered module tags exist:

```sh
make test-consumer-release VERSION=v0.1.0
```

GitHub Actions runs the same read-only gate and a PostgreSQL integration job. A separate workflow compares base and
head benchmarks with pinned `benchstat`, uploads raw samples, and reports when the base predates the Go module instead
of failing the first comparison. It does not enforce an unstable cross-machine performance threshold.

See the [practical usage guide](docs/usage.md), [Kafka guide](docs/kafka.md), [contracts](docs/contracts.md),
[architecture](docs/architecture.md), [durable pipeline E2E](docs/e2e.md),
[adoption and migration guide](MIGRATION.md), [release order](docs/release.md), and the
[first real-project pilot decision](docs/decisions/0002-real-project-pilot.md). Distributed request/reply remains a
separate, unimplemented boundary in [ADR-0003](docs/decisions/0003-distributed-queries.md).
