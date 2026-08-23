# GoMessenger

GoMessenger is a typed command and event toolkit for Go 1.27+. It keeps one explicit descriptor and handler API across
local dispatch, transactional outbox staging, NATS JetStream delivery, durable inbox deduplication, and operational
telemetry.

The root module is intentionally small: standard library plus
[`gobus`](https://github.com/assurrussa/gobus). Broker, SQL, Prometheus, and OpenTelemetry dependencies live in optional
nested modules.

## When to use it

Use GoMessenger when a message may cross a process boundary or must survive a process restart. Continue using GoBus
directly for small in-process command, query, and best-effort event dispatch. GoMessenger is not a replacement for a
database transaction, workflow engine, or streaming analytics platform.

Queries are intentionally outside the current GoMessenger public contract. In-process queries use GoBus
`RegisterResult`/`DispatchResult`; a future cross-process query would require a separate request/reply contract rather
than reusing durable command receipts, Outbox, Inbox, retry, or DLQ semantics. See the
[query boundary decision](docs/decisions/0001-query-boundary.md).

The durable contract is **at-least-once**:

- an outbox route reports success only after the envelope is staged in the caller's database transaction;
- a direct NATS route reports success only after a JetStream `PubAck`;
- a durable consumer acknowledges only after its inbox transaction and handler commit;
- stable message identity and an inbox prevent repeating the same committed consumer transaction;
- external side effects still need their own idempotency key or transactional boundary. The library does not claim
  exactly-once effects.

## Requirements and modules

GoMessenger requires Go 1.27 because the builder and messenger expose generic methods.

| Module                              | Responsibility                                                   |
|-------------------------------------|------------------------------------------------------------------|
| `github.com/assurrussa/gomessenger` | descriptors, envelopes, lineage, local routes, runtime, manifest |
| `.../adapters/outbox`               | transactional staging and a broker relay job                     |
| `.../adapters/nats`                 | JetStream producers, consumers, topology, CloudEvents            |
| `.../adapters/inbox`                | atomic PostgreSQL and SQLite consumer deduplication              |
| `.../observability`                 | Prometheus, OpenTelemetry spans, W3C Trace Context                |
| `.../tools/gomessengerctl`          | manifest/topology validation, plan/apply, DLQ inspect/replay      |

The initial GoMessenger release is being prepared as `v0.1.0`. Outbox root and SQLite backend `v0.11.0` are already
published and are the pinned durable-producer dependencies. During repository development `go.work` selects local
GoMessenger modules; published consumer modules must use path-qualified tags and no local `replace` directives.

## Typed local example

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
_, err = bus.Send(ctx, resize, ResizeMedia{JobID: 42})
```

The equivalent runnable API is compiled as `ExampleMessenger_Send`; `testdata/consumer` separately compiles the
complete public facade from an external Go module.

Descriptors use explicit stable wire names, schema versions, content types, data encodings, and optional schema URIs.
Go type names and package paths never become the wire contract implicitly. Native envelopes carry `dataEncoding` as
`json`, `text`, or `binary`, so custom codecs do not infer their representation from a media-type prefix.

## Transactional outbox producer

Use `adapters/outbox` when a business write and message publication must commit or roll back together:

```go
route, err := outboxadapter.NewProducer(outboxService, outboxadapter.ProducerConfig{
	Name: "outbox.integration-events",
})
if err != nil {
	return err
}
builder.RouteEvent(mediaResized, route)

err = transactions.RunInTx(ctx, func(txCtx context.Context) error {
	if err := mediaRepository.Save(txCtx, media); err != nil {
		return err
	}
	_, err := bus.Publish(txCtx, mediaResized, MediaResized{JobID: media.ID})
	return err
})
```

The producer stores the canonical envelope under its `MessageID`. Repeating identical content resolves the same outbox
tombstone; reusing the identity with different content fails closed. Immediate messages use their immutable message time
as `availableAt`, preventing retry-time fingerprint drift.

Register the relay job on the shared outbox runtime:

```go
natsRoute, err := natsadapter.NewRoute(natsConnection, natsadapter.RouteConfig{
	Name: "nats.integration-events", Namespace: "prod", WireMode: natsadapter.WireNative,
})
if err != nil {
	return err
}
relay, err := outboxadapter.NewRelayJob(natsRoute, outboxadapter.RelayJobConfig{})
if err != nil {
	return err
}
outboxService.MustRegisterJob(relay)
```

`messenger.RetryAfter` from the broker route becomes a persisted `outbox.RetryAt`; permanent envelope failures move
directly to the outbox DLQ.

## Durable JetStream consumer and inbox

A consumer owns one stable `ConsumerID`, explicit concurrency and retry bounds, and one SQL inbox:

```go
store, err := inboxpgsql.New(database)
if err != nil {
	return err
}
consumer, err := natsadapter.NewEventConsumer(
	natsConnection,
	store,
	mediaResized,
	func(ctx context.Context, message messenger.Message[MediaResized]) error {
		// SQL repositories can use inbox.SQLTxFromContext(ctx), so this write and
		// the inbox completion marker commit atomically.
		return projection.Apply(ctx, message.Metadata.ID.String(), message.Payload)
	},
	natsadapter.HandlerConfig{
		Stream: "MESSAGES", Namespace: "prod", ConsumerID: "media-projection",
		Concurrency: 8, Timeout: 30 * time.Second, MaxAttempts: 10,
	},
)
if err != nil {
	return err
}
builder.Use("consumer.media-projection", consumer)
```

Run the embedded additive inbox migrations explicitly with `inboxpgsql.Migrate` or `inboxsqlite.Migrate`. They include
the durable handler-attempt count and permanent-outcome state required by NATS consumers. Hosts own database
connections, migration ordering, NATS connections, credentials, and process supervision.

Each active consumer worker may hold one SQL transaction for the full handler invocation. When several consumers share
one `sql.DB`, configure `SetMaxOpenConns` to at least the sum of their `HandlerConfig.Concurrency` values, plus explicit
headroom for application and maintenance queries. A smaller pool turns configured consumer concurrency into database
connection contention and can exhaust handler deadlines before application code runs.

Inbox table names are currently fixed as `gomessenger_inbox`, `gomessenger_inbox_attempts`, and
`gomessenger_inbox_attempt_generations`; there is no table-prefix or schema option. PostgreSQL hosts that use a
non-default schema must apply migrations and configure the same stable connection-level `search_path` for every pooled
connection. SQLite uses the fixed names in the selected database file.

Provision source and DLQ capacity separately. `DevStream` keeps native source messages at the 1 MiB envelope bound;
`DevDLQStream` reserves `DefaultMaxDLQMessageBytes` for the expanded JSON DLQ record. The NATS server/account
`max_payload` must be at least the same value. `Consumer.Run` and `Readiness` reject a missing or undersized DLQ route
before reporting ready.

## Middleware, logging, and tracing

Global middleware wraps local handlers and can be reused by durable NATS consumers. Registration order is execution
order: the first middleware is outermost. A middleware may replace the context or short-circuit, but `next` may be
called at most once. Typed handler decorators use `messenger.ChainHandler`.

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
observers. Core logging contains infrastructure state only—observer/service failures, ACK/NAK, heartbeat, and DLQ
hand-off failures—and never logs payloads, message bodies, or arbitrary headers.

The observability propagator carries only W3C `traceparent` and `tracestate`. It works through native envelopes,
CloudEvents structured/binary modes, and transactional Outbox storage. Baggage is intentionally not supported yet.

## Failure and lifecycle semantics

- Return `messenger.Permanent(err)` for a non-retryable handler failure.
- Return `messenger.RetryAfter(err, delay)` for an explicit durable delay.
- Other errors use bounded full-jitter exponential retry in the NATS adapter.
- `AckWait` is a broker redelivery deadline, not a handler timeout. It must be at least 100 ms and may be shorter than
  `Timeout`; active handlers refresh acknowledgement progress every `AckWait / 3`.
- The NATS adapter publishes and confirms a DLQ record before broker-confirming acknowledgement of the original message.
  Failed terminal hand-off operations retry on the current delivery without invoking the application handler beyond
  `MaxAttempts`. A permanent outcome is persisted independently of the attempt count, so an interrupted hand-off cannot
  invoke that handler again after restart.
- `NotBefore` becomes a retry delay until due; `ExpiresAt` becomes a permanent expired outcome.
- `Runtime.BeginDrain` closes admission. `Runtime.Shutdown` waits for accepted work until its context deadline, then
  force-cancels the shared run context.
- Never call `Shutdown` from a handler executing on the same runtime; coordinate shutdown from the host lifecycle.

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
GOMESSENGER_POSTGRES_DSN='postgres://...' make test-postgres
make bench-all
```

`make check` covers every module, static lint, race and checkptr builds, a 90% root coverage gate, an isolated
clean-consumer module, and the Docker-free durable pipeline E2E. `make test-e2e` reruns only that full
Outbox-to-JetStream-to-Inbox path. Release requirements are prepared and checked explicitly:

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

See [contracts](docs/contracts.md), [architecture](docs/architecture.md), [durable pipeline E2E](docs/e2e.md),
[adoption and migration guide](MIGRATION.md), [release order](docs/release.md), and the
[first real-project pilot decision](docs/decisions/0002-real-project-pilot.md).
