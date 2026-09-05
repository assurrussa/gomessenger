# Architecture

GoMessenger separates the stable application contract from transport and storage capabilities.

```text
typed descriptor + payload
          |
          v
      Messenger
       /      \
local route   one-way transport route
   |           /          \
GoBus     outbox stage   JetStream/Kafka publish
  |           |                |
query R       v                v
        relay worker ----> durable consumer
                                 |
                                 v
                         SQL inbox transaction
                                 |
                                 v
                         typed handler
```

See the [practical usage guide](usage.md) for the concrete composition order, current producer and consumer examples,
failure classification, and host shutdown sequence behind this diagram.

## Root module

The root module owns the concepts that applications compile against: typed command/query/event descriptors, message
metadata, the canonical one-way envelope, route receipts, failure classification, local routes, runtime lifecycle, and
manifests. Its only non-standard dependency is GoBus, used as the local dispatch engine.

Transport-neutral `Logger`, `Observer`, `Middleware`, and `ContextPropagator` interfaces also live at the root. Their
default implementations are no-ops, so local-only consumers do not acquire logging or telemetry dependencies.

The root does not open network connections, migrate databases, install global telemetry providers, or infer production
topology. This keeps local-only consumers lightweight and lets hosts own infrastructure lifecycle.

## Query boundary

`Query[Q,R]` is a typed local facade. Its descriptor records request identity; `R` is an in-process type identity. The
builder requires one handler and one sealed `LocalQueryRoute`. Sync dispatch uses GoBus `RegisterResult` and
`DispatchResult`; async dispatch uses bounded `SubmitResult` and still waits for one result through `Messenger.Query`.

Queries receive generated metadata, lineage, trace propagation, middleware, observations, panic isolation, and async
lifecycle behavior, but they do not become `Delivery`. They have no result envelope, receipt, Outbox, Inbox, NATS/Kafka,
retry, DLQ, or replay path. Native envelope validation and transport adapters explicitly reject `KindQuery`.

A cross-process query additionally needs a result codec/envelope, absolute deadlines, best-effort cancellation, remote
error mapping, responder availability, response bounds, and consistency-aware retry. That future API is separate from
the local method. See [ADR-0001](decisions/0001-query-boundary.md) and the unimplemented distributed contract in
[ADR-0003](decisions/0003-distributed-queries.md).

## Route boundary

For a command or event, `Messenger` creates metadata, lazily encodes the payload, and hands a `Delivery` to exactly one
configured route. The route determines the meaning of successful admission and returns an explicit receipt. A query
instead hands an internal result call to its local route and returns `R` directly.

Local routes invoke handlers in the same process. The NATS route publishes the canonical envelope directly and waits
for `PubAck`; the Kafka route commits it in a producer transaction. The outbox route persists the same envelope and a
relay later gives those exact bytes to either envelope publisher. No relay decode and rebuild step is allowed because
that could change identity or timestamps.

## Producer transaction

The outbox adapter requires the unified version-aware repository and batch contracts in Outbox v0.15.0. It uses the
message ID as the unique key and fingerprints the immutable job definition. The host supplies the transaction through
its configured outbox repository context; GoMessenger does not begin or commit the business transaction.

This boundary provides the following invariant:

> A committed business mutation has its message staged, or neither mutation commits.

It does not imply that the broker has received the message at transaction commit time. The supervised outbox worker owns
that later delivery.

### High-throughput platforms and CDC Outbox

The standard outbox uses a transactional polling publisher model with batching (`adapters/outbox`). For typical service
workloads, this sustains several thousand messages per second. However, for high-throughput platforms requiring tens or
hundreds of thousands of messages per second, polling and status updates (`SELECT ... FOR UPDATE SKIP LOCKED`, claim,
and completion updates) can create database IOPS bottlenecks, lock contention, and table bloat.

Because GoMessenger isolates routing through the minimal `Route` and `BatchRoute` interfaces, high-throughput systems
can adopt alternative staging strategies without changing application contracts:

- **Append-only + CDC (Change Data Capture):** The application implements a custom `Route` that writes an immutable,
  unindexed row (or uses binary streaming via `pgx.CopyFrom`) in the business transaction. An external CDC engine
  (e.g., Debezium, a PostgreSQL WAL / `pgoutput` streamer, or Kafka Connect) captures WAL changes and forwards canonical
  envelopes to Kafka or JetStream with near-zero transactional overhead on the primary database.
- **Custom high-speed routes:** Teams can implement `Route` to write directly to fast distributed journals, memory-mapped
  buffers, Redis Streams, or Tarantool when relational database staging is not required.
- **Contract preservation:** Regardless of the underlying staging or transport mechanism, application code retains
  GoMessenger's typed descriptors, schema versions, context propagation, middleware, tracing, and metadata lineage.

## Consumer transaction

NATS and Kafka consumers pass canonical bytes and a stable key to the inbox. PostgreSQL and SQLite backends begin a
database transaction, expose it in the handler context, run the handler, and mark the key complete in that same
transaction.

This boundary provides the following invariant:

> A successful database effect and its inbox completion marker commit together, or both roll back.

The NATS ACK or Kafka offset transaction happens only after commit. A crash between those boundaries creates a
duplicate delivery that the inbox recognizes without running the handler again.

Bounded NATS handling records its attempt before invoking application code. A savepoint isolates handler business
writes: a failed invocation rolls those writes back while the outer transaction commits the attempt counter. Database
failures before invocation and `NotBefore` deferrals therefore consume no attempt. PostgreSQL's unique constraint
serializes a new identity; a row lock serializes later attempts for an already persisted incomplete identity.

The original bounded cycle retains the legacy attempt row. Each explicit replay generation has its own row keyed by the
logical inbox identity and a generation-derived fingerprint, while the canonical fingerprint in the identity row remains
unchanged. Interleaved replay generations therefore cannot reset one another. Forgetting one generation removes only
that row and removes an incomplete logical identity only after no attempt rows remain.

## Runtime and backpressure

Concurrency and buffers are bounded and adapter configuration is explicit. One-way async delivery and async query
lifetimes are intentionally different:

- the caller context controls synchronous invocation and waiting for bounded admission;
- once an async command/event is accepted, execution is owned by the runtime and detached from caller cancellation;
- an async query retains caller cancellation for admission, execution, and result waiting; its buffered result cannot
  block runtime drain after the caller stops waiting;
- shutdown closes admission, drains accepted work, and force-cancels only when the host's deadline expires.

A service that never entered `Run` closes synchronously during shutdown, including after a pre-run drain.

Managed service shutdown calls run in parallel so one slow service does not serialize the host deadline. Services are
sorted by stable ID before startup and joined shutdown errors preserve that order. An unexpected service return cancels
peers and emits an observation/log record; automatic restart remains the host supervisor's responsibility.

NATS consumers use pull delivery, bounded workers, explicit acknowledgement deadlines, progress heartbeats for active
handlers, bounded application attempts, and a confirmed DLQ hand-off. Broker delivery stays unlimited until that
hand-off succeeds, preventing a failed DLQ publish from stranding an unacknowledged final delivery.

Kafka uses a separate adapter implementation because its safety boundary is different: descriptor-derived partitioned
topics, stable group members, read-committed isolation, and transactions that atomically combine consumed offsets with
retry/DLQ production. Retry topics preserve exact due time but permit later source records to overtake failed work.
Consumer readiness checks the resources required to run safely; declarative planning separately checks the complete
managed topic policy. See [ADR-0004](decisions/0004-kafka-adapter.md).

## Observability

The optional observability module implements the root `Observer` contract and a W3C Trace Context propagator. It records
low-cardinality Prometheus labels and OpenTelemetry spans using providers supplied by the host. Message IDs, attempts,
retry delays, and other unbounded values are not metric labels. They remain available in structured observations and
spans. `traceparent` and `tracestate` survive native, CloudEvents, and Outbox paths; baggage is intentionally excluded.

The core logger is independent from observability and defaults to no-op. It records only infrastructure failures. A
logging observer is opt-in for successful/failed operation records, and observer panics are isolated from fan-out.
`OperationQuery` covers the complete request/reply call and `OperationHandle` covers its handler. Neither observation
contains request payloads, result values, or arbitrary headers.

## DLQ replay boundary

The durable consumer captures bounded original headers alongside subject, wire mode, and original wire bytes. Offline
replay planning validates the record and derives a deterministic digest without opening NATS. Confirmed replay uses the
original subject and bytes, a DLQ-record-specific JetStream message ID, and requires `PubAck`. Consumer-scoped reserved
headers select a fresh durable attempt generation for that hand-off; broker redelivery keeps the same generation. It
cannot mutate the subject, delete the record, or print payload and secret headers. NATS publish permissions must prevent
ordinary direct publishers from forging reserved replay metadata.

Kafka uses its own bounded DLQ v1 rather than translating the NATS record. Its offline plan validates original source,
record key, canonical bytes, and deterministic consumer replay topic. Confirmed replay commits to that protected topic
with a DLQ-specific attempt generation. Both adapters retain the original DLQ record and preserve completed Inbox
suppression.

## Compatibility boundaries

Existing GoBus users can continue unchanged. Existing outbox job names, schema versions, and payloads are not rewritten
by installing GoMessenger. In particular, a service whose webhook fan-out resolves recipients at event creation time
must keep that snapshot contract; replacing it with a consumer that resolves recipients after broker delivery would be a
behavioral regression rather than a transport migration.
