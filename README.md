# GoMessenger

[![CI](https://github.com/assurrussa/gomessenger/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/assurrussa/gomessenger/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/assurrussa/gomessenger)](https://github.com/assurrussa/gomessenger/releases/latest)
[![Go Reference](https://pkg.go.dev/badge/github.com/assurrussa/gomessenger.svg)](https://pkg.go.dev/github.com/assurrussa/gomessenger@v0.2.1)
![Go 1.27](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Typed durable messaging for Go.**

Build commands, local queries, and events with explicit transaction, delivery, retry, and broker semantics.

Transactional Outbox/Inbox, NATS JetStream, Kafka, bounded retries, DLQ/replay, tracing, and managed lifecycle.

> **Release status:** `v0.2.1` is the current release line. Source validation, dependency-ordered tag publication, and
> the clean published-consumer gate are separate release evidence. The real-service pilot remains pending, so
> controlled repository gates are not a production-readiness claim. The current checkout may accumulate follow-up
> changes beyond that release line.

## What it is

GoMessenger is a typed facade for Go 1.27+ services that need both process-local messaging and durable one-way delivery.
Commands, local request/reply queries, and events share explicit descriptors, middleware, lifecycle, and observations;
one-way commands and events can additionally use transactional Outbox staging, NATS JetStream or Kafka delivery, and a
durable SQL Inbox.

It keeps transport truth visible. An Outbox receipt means staged in the caller's transaction, a NATS receipt means
JetStream returned `PubAck`, a Kafka receipt means the producer transaction committed, and a consumer ACK/offset follows
the committed Inbox transaction. The root module remains standard library plus
[`gobus`](https://github.com/assurrussa/gobus); broker, SQL, Prometheus, and OpenTelemetry dependencies live in optional
nested modules.

## Why it exists

Use GoMessenger when a service needs at least one of these boundaries:

- a business write and outgoing command/event must commit or roll back together;
- broker redelivery must not repeat an already committed SQL handler effect;
- retry, terminal failure, DLQ, and replay must be bounded and explicit;
- local commands, local queries, and durable events should use stable typed descriptors without pretending that NATS
  and Kafka have identical semantics.

If all work is process-local, GoBus or direct function calls may be enough. If the problem is a durable multi-step
workflow with timers and compensation, use a workflow engine. See the [use-case comparison](docs/comparison.md) before
choosing an abstraction.

## Install

GoMessenger requires Go 1.27+. For local commands, queries, and events:

```sh
go get github.com/assurrussa/gomessenger@v0.2.1
```

For durable NATS JetStream delivery with Inbox and transactional Outbox integration:

```sh
go get github.com/assurrussa/gomessenger@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/inbox@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/nats@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/outbox@v0.2.1
```

For durable Kafka delivery with Inbox and transactional Outbox integration:

```sh
go get github.com/assurrussa/gomessenger@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/inbox@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/kafka@v0.2.1 \
  github.com/assurrussa/gomessenger/adapters/outbox@v0.2.1
```

Optional telemetry and CLI modules use the same release version:

```sh
go get github.com/assurrussa/gomessenger/observability@v0.2.1
go install github.com/assurrussa/gomessenger/tools/gomessengerctl@v0.2.1
```

These commands target the exact path-qualified `v0.2.1` tags. The rest of this README tracks the current checkout and
may describe unreleased APIs that are not present in that release line. Use the versioned
[Go Reference](https://pkg.go.dev/github.com/assurrussa/gomessenger@v0.2.1) for the exact release API, or use the checkout
workflow below when evaluating unreleased changes.

Keep every GoMessenger module in one consumer on the same version. The Outbox adapter requires Outbox `v0.11.0`; the
host selects and installs its matching database backend separately. To evaluate the current checkout instead:

```sh
git clone https://github.com/assurrussa/gomessenger.git
cd gomessenger
GOWORK=off go test ./...
```

## 30-line local quickstart

The smallest local command is:

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

For the durable path, run the [PostgreSQL + NATS demo](examples/durable-postgres-nats):

```sh
make demo-durable-postgres-nats
```

It performs a business write and Outbox stage in one PostgreSQL transaction, relays through JetStream, retries an
intentional handler failure, suppresses a distinct duplicate delivery in the Inbox, moves a permanent failure to the
DLQ, and confirms replay. The example is a checkout-level demonstration with local GoMessenger replacements; it is not
evidence of published-module resolution or production readiness.

The example also contains an opt-in open-loop NATS capacity experiment over
`HTTP -> business transaction + Outbox -> JetStream -> Inbox -> business projection`:

```sh
make capacity-nats
```

It reports unique committed business effects and their exact canonical envelope bytes inside the load window, then
reconciles every accepted order after a separate bounded drain. See the
[example capacity contract](examples/durable-postgres-nats#capacity-experiment). Results describe only the recorded
checkout, host, and local Docker topology; they are not production benchmark claims.

## Guarantees

The durable contract is **at-least-once**:

- an Outbox route reports success only after the envelope is staged in the caller's database transaction;
- a direct NATS route reports success only after a JetStream `PubAck`;
- a direct Kafka route reports success only after its producer transaction commits;
- a durable consumer acknowledges only after its Inbox transaction and handler commit;
- stable message identity and an Inbox suppress a second execution of the same committed consumer transaction;
- concurrency, envelopes, headers, handler attempts, retry delays, and shutdown waits are bounded by explicit
  configuration or documented limits.

## Non-goals

- No exactly-once external effects: HTTP calls, email, object storage, and writes to another database still need their
  own idempotency or durable hand-off.
- No distributed queries: `Query[Q,R]` is process-local. Remote request/reply remains the separate, unimplemented
  contract in [ADR-0003](docs/decisions/0003-distributed-queries.md).
- No workflow, saga, event-sourcing, service-discovery, or streaming-analytics engine.
- No automatic ownership of host database connections, broker credentials, migrations, topology policy, supervision,
  or deployment.
- No claim of universal broker abstraction or production-proven maturity; the real-service pilot in
  [ADR-0002](docs/decisions/0002-real-project-pilot.md) is still pending.

## Choose a route

Start with an explicit descriptor, choose the success boundary, build the messenger, and inject a narrow `Sender`,
`Querier`, or `Publisher` into business code. Durable consumers are separate managed services.

| Route | A successful call means | Important boundary |
|---|---|---|
| `NewLocalSyncRoute` | the in-process handler completed | no restart durability |
| `NewLocalAsyncRoute` | bounded runtime work completed for queries or was accepted for one-way work | no restart durability |
| `natsadapter.NewRoute` | JetStream returned `PubAck` | not atomic with a separate business database write |
| `kafkaadapter.NewRoute` | the producer transaction committed | not atomic with a separate business database write |
| `outboxadapter.NewProducer` | the envelope was staged in the active host transaction | provisional until that transaction commits |

```text
business code -> Messenger.Query -> local result handler -> typed result
             `-> Send/Publish -> local handler
                              `-> Outbox -> relay -> JetStream/Kafka -> consumer -> Inbox transaction -> handler -> ACK/offset
```

For a durable flow, provision topology, apply Outbox and Inbox migrations, register the relay before producer traffic,
start the managed runtimes, and expose readiness before accepting traffic. The [practical usage guide](docs/usage.md)
shows the full composition and shutdown order.

## Modules and release status

GoMessenger requires Go 1.27 because the builder and messenger expose generic methods.

| Module | Responsibility |
|---|---|
| `github.com/assurrussa/gomessenger` | descriptors, local queries, envelopes, local routes, runtime, manifest |
| `.../adapters/outbox` | transactional staging and broker relay job |
| `.../adapters/nats` | JetStream producers, consumers, topology, CloudEvents |
| `.../adapters/kafka` | transactional Kafka producers, consumers, topology, retry/DLQ |
| `.../adapters/inbox` | atomic PostgreSQL and SQLite consumer deduplication |
| `.../observability` | Prometheus, OpenTelemetry spans, W3C Trace Context |
| `.../tools/gomessengerctl` | manifest/topology validation, plan/apply, DLQ inspect/replay |

The module set uses synchronized path-qualified `v0.2.1` tags. Release completion requires every tag above plus the
clean post-publication consumer probe; neither is inferred from source-only checks. Outbox root and its
PostgreSQL/SQLite backend tags at `v0.11.0` are the pinned durable-producer dependencies. During repository development
`go.work` selects local GoMessenger modules; published consumers use matching path-qualified tags and no local
`replace` directives. See the [release process](docs/release.md) for dependency order and verification.

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

## Local method-call benchmarks

Representative GitHub-hosted benchmark results for the synchronous local public path (Linux/amd64, Intel Xeon Platinum
8573C, Go 1.27, ten samples):

| Public method | Scenario | Median time/op | Approx. calls/s | B/op | allocs/op |
|---------------|----------|---------------:|----------------:|-----:|----------:|
| `Messenger.Send` | one local command handler | 1.066 µs | ~938k | 1472 | 9 |
| `Messenger.Query` | one local query handler and typed result | 937.6 ns | ~1.07M | 1296 | 10 |
| `Messenger.Publish` | one local event subscriber | 1.086 µs | ~921k | 1472 | 9 |

The calls/s column is the reciprocal of the median single-thread time/op, not a concurrency or durable-throughput
claim. These benchmarks exercise `NewLocalSyncRoute` with no-op application handlers. They do not include NATS, Kafka,
Outbox, Inbox, SQL, network latency, retries, or telemetry exporters. Reproduce the sample with:

```sh
GOWORK=off go test -run '^$' -bench '^BenchmarkMessengerLocal' -benchmem -count=10 .
```

GitHub Actions retains the raw samples and `benchstat` comparison. Command and query results in this snapshot did not
change significantly from the base commit.

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
constructing consumers. They include the durable handler-attempt count and permanent-outcome state required by durable
consumers. Hosts own database connections, migration ordering, NATS connections, credentials, and process supervision.

Each active consumer worker holds one SQL transaction for the full handler invocation. This is the atomic boundary for
business writes and the Inbox completion marker; do not move the handler outside it. The adapter adds no connection
semaphore: `database/sql` is the host-managed backpressure boundary. When several consumers share one `sql.DB`, configure
`SetMaxOpenConns` to at least the sum of their `HandlerConfig.Concurrency` values, plus explicit headroom for application
and maintenance queries. A smaller pool becomes the bottleneck before Go dispatch and can exhaust handler deadlines
before application code runs.

`Timeout` bounds application handler execution. The Inbox transaction receives an additional
`FinalizationTimeout` (5 seconds by default) to commit or roll back after that deadline. Increase it when a remote or
otherwise slow database needs more finalization time; it does not extend the handler deadline.

Both SQL backends default to the `gomessenger_` prefix and therefore preserve `gomessenger_inbox`,
`gomessenger_inbox_attempts`, and `gomessenger_inbox_attempt_generations`. Pass the same options to migration and runtime
construction when a host needs another namespace:

```go
postgresInbox := []inboxpgsql.Option{
	inboxpgsql.WithSchema("messaging"), // the host creates the schema and grants access
	inboxpgsql.WithTablePrefix("site_"),
}
if err := inboxpgsql.Migrate(ctx, database, postgresInbox...); err != nil {
	return err
}
store, err := inboxpgsql.New(database, postgresInbox...)
```

`WithTablePrefix` is also available for SQLite; SQLite has no schema option. PostgreSQL qualifies every relation instead
of relying on `search_path`, and its migrator never creates the configured schema. Changing the prefix or schema selects
a separate Inbox: migrations do not rename, copy, or otherwise transfer existing deduplication history.

Provision source and DLQ capacity separately. `DevStream` keeps native source messages at the 1 MiB envelope bound;
`DevDLQStream` reserves `DefaultMaxDLQMessageBytes` for the expanded JSON DLQ record. The NATS server/account
`max_payload` must be at least the same value. `Consumer.Run` rejects a missing or undersized DLQ route before starting;
the low-frequency `Consumer.DeepHealth` probe detects later topology drift without making ordinary readiness expensive.

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
abort/fencing, topology failures or applied changes, and retry partition deferrals. Core logging contains
infrastructure state only and never logs record keys, payloads, message bodies, or arbitrary headers.
`WithClientLogger` is a separate explicit opt-in to franz-go's own client logs.

Recovered handler panic values and stacks are dropped by default; ordinary errors satisfy the transport-neutral
`HandlerPanicError` interface and can be classified with `errors.As` across independently versioned adapters.
Configure `WithPanicReporter` (or the corresponding durable consumer field) only for a trusted diagnostic sink.
Consumer observations and DLQ errors use the conservative `DefaultFailureSanitizer` unless the host explicitly supplies
another sanitizer.

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

`Runtime.Readiness` is the lightweight admission/connectivity probe for the runtime and every attached service.
`Runtime.DeepHealth` performs explicit topology diagnostics; `Runtime.Liveness` does not require readiness. The selected
Outbox backend runtime has its own host-supervised lifecycle; GoMessenger does not open connections or supervise it
implicitly. Never call `Shutdown` from a handler executing on the same runtime.

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

## Level 2 roadmap

The next maturity level is an operational messaging platform, not a workflow framework. Work is ordered evidence-first:
pilot and performance baseline, schema compatibility, broker capability declarations, partition-key and ordering
semantics, batching only when profiling justifies it, then load, soak, and chaos validation. Saga engines, workflow
orchestration, and generic distributed request/reply remain out of scope.

See the [Level 2 roadmap](docs/roadmap.md) for workstreams and exit criteria.

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
Outbox-to-JetStream-to-Inbox path. `make test-kafka` is the local Docker entry point against official Kafka 4.1.2
and 4.3.1 images; hosted CI runs each version in an independent matrix job. Release requirements are prepared and
checked explicitly:

```sh
make check
make release-ready VERSION=v0.2.1 OUTBOX_VERSION=v0.11.0
make release-readiness VERSION=v0.2.1 OUTBOX_VERSION=v0.11.0
```

Run the full source gate before `release-ready` removes development replacements. `release-readiness` then checks the
exact tag declarations without trying to download GoMessenger tags that do not exist yet.

A published release is verified separately, after all dependency-ordered module tags exist:

```sh
make test-consumer-release VERSION=v0.2.1
```

GitHub Actions runs static, race, checkptr, PostgreSQL, and Kafka integration shards; the aggregate `Full gate`
requires all of them. A separate workflow compares base and head benchmarks with pinned `benchstat`, uploads raw samples,
and reports when the base predates the Go module instead of failing the first comparison. It does not enforce an unstable
cross-machine performance threshold.

See the [practical usage guide](docs/usage.md), [Kafka guide](docs/kafka.md), [contracts](docs/contracts.md),
[architecture](docs/architecture.md), [durable pipeline E2E](docs/e2e.md), [use-case comparison](docs/comparison.md),
[Level 2 roadmap](docs/roadmap.md), [adoption and migration guide](MIGRATION.md), [release order](docs/release.md), and the
[first real-project pilot decision](docs/decisions/0002-real-project-pilot.md). Distributed request/reply remains a
separate, unimplemented boundary in [ADR-0003](docs/decisions/0003-distributed-queries.md).
