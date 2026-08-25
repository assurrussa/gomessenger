# Adoption and migration guide

This document covers incremental service adoption and operational rollout. It is not a database migration catalog or a
GoMessenger version-to-version changelog; backend schema migrations remain owned by the selected Outbox and Inbox
modules.

GoMessenger is additive. A service can introduce one descriptor and route at a time while existing GoBus handlers,
outbox jobs, and webhook contracts continue unchanged.

Before adopting it, use the [use-case comparison](docs/comparison.md) to decide whether GoBus, direct broker APIs,
synchronous RPC, or a workflow engine is the smaller boundary. The
[PostgreSQL + NATS durable demo](examples/durable-postgres-nats) is the fastest checkout-level evaluation of the full
Outbox/Inbox path; it does not replace the published-module probe or real-service pilot.

## From GoBus only

Keep GoBus for low-level process-local work that does not need the shared facade. Introduce GoMessenger when code wants
one typed command/query/event API, or when a one-way message needs a stable name/schema, persistence, broker delivery,
cross-service consumption, or durable retry/DLQ behavior.

1. Define a descriptor with an explicit name and schema version.
2. Register the existing handler through `Builder.HandleCommand` or `Builder.Subscribe`.
3. Start with `NewLocalSyncRoute` and verify payload compatibility.
4. Move the route to outbox or NATS without changing the descriptor or handler type.
5. Add the runtime to the host's readiness, drain, and shutdown lifecycle before enabling asynchronous admission.

Do not derive a wire name from a Go type. Do not treat a local async receipt as a durable receipt.

## From GoBus result dispatch

Existing GoBus `RegisterResult`/`DispatchResult` code can remain unchanged. To adopt the GoMessenger CQRS facade:

1. declare `MustQuery[Q,R]` with an explicit request name, version, and request codec;
2. register the existing result handler with `HandleQuery` or `HandleQueryFunc`;
3. select `NewLocalSyncRoute`, or a running bounded `LocalAsyncRoute` for worker isolation;
4. inject `BindQuerier` instead of the full messenger.

The migration remains process-local. Result `R` has no codec and never enters the manifest or wire envelope. Do not
route the query through Outbox, NATS, Inbox, receipt, retry, or DLQ infrastructure. A future distributed read requires
the separate result-envelope and failure contract in `docs/decisions/0003-distributed-queries.md`.

## Producer migration to transactional outbox

The GoMessenger outbox producer needs outbox v0.11's unique versioned put capability. Upgrade the root outbox module and
the backend module together, run their existing migrations, and configure the capability repository on the service.

Inside the existing business transaction, publish through the GoMessenger outbox route. Keep the old job registered
until every previously staged row has completed or moved to its existing DLQ. A transport migration must not rename or
reinterpret historical outbox rows.

The new relay job has its own explicit job name and schema version. Deploy the relay registration before code can stage
that job, then enable producers. Publish immutable path-qualified tags for nested Go modules before testing a clean
consumer; never use local `replace` directives as release evidence.

## Consumer migration to JetStream and inbox

1. Apply the additive inbox migration explicitly.
2. Choose a stable consumer ID, stream, subject, concurrency, timeout, and max attempts.
3. Ensure handler database writes use `inbox.SQLTxFromContext` or are otherwise idempotent.
4. Plan topology and resolve conflicts before deployment.
5. Deploy the consumer dark, check readiness and telemetry, then enable producer traffic.
6. Drain and remove the old consumer only after lag and duplicate behavior are understood.

A new consumer ID creates a new inbox identity and normally reprocesses retained messages. Treat such a rename as a data
migration, not a cosmetic refactor.

## Existing gowebhooks users

Do not mechanically replace the existing `gowebhooks` fan-out pipeline. Its recipient set is resolved and snapshotted at
event creation time. Resolving subscriptions in a later generic broker consumer would change delivery semantics when a
subscription is edited between those moments.

A safe first adoption is the selected additive `content.article.published` audit event after the existing snapshot is
staged. It does not replace webhooks. Migrate the fan-out itself only if the new envelope carries the immutable recipient
snapshot and compatibility tests prove the same ordering, retry, and tombstone behavior.

## Existing media-resizer users

Keep current `media.processor` and `webhook.notify` outbox payloads and schema versions while upgrading outbox. Picodata
can use the additive fenced reschedule capability, but the GoMessenger producer must not be enabled there until that
backend also provides the unique versioned put contract.

Use a new GoMessenger job name for new envelopes. Do not reinterpret old rows as canonical envelopes.

## Existing download users

No source migration is required for the initial GoMessenger release. The clean consumer fixture verifies that the
existing download payload remains exactly `{"jobId":42}`. If the service later emits a typed integration event, give it a
new descriptor and leave `download-video` job compatibility intact.

## Rollback

Disable new producers first, let the relay and consumers drain, then roll back application code. Additive topology and
inbox tables may remain. Do not move published tags, delete streams with retained messages, reuse a message ID for new
content, or remove the old outbox job handler while old rows can still be claimed.
