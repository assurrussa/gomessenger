# ADR-0006: Transactional producer and relay batching

## Status

Accepted on 2026-09-01.

## Context

ADR-0005 reduced consumer-side handler and Inbox transaction overhead, but the
durable path still staged and relayed one envelope at a time. Increasing the
old Outbox reservation batch only prefetched rows; it did not create one
producer transaction, one relay handler invocation, or one broker batch.

GoMessenger needs a distinct batch ingress and relay contract without changing
the behavior of existing routes, single-message Outbox jobs, or direct broker
producers. The batch path must preserve stable identity, transaction atomicity,
retry limits, broker confirmation, and clean failure boundaries.

## Decision

### Producer boundary

The root facade adds `BatchRoute`, `BatchSender`, `BatchPublisher`, their bound
facades, and typed `SendBatch`, `SendMessageBatch`, `PublishBatch`, and
`PublishMessageBatch` methods. A batch call validates and canonicalizes every
message, rejects duplicate message IDs, calls the route once, and returns
receipts in input order only after the whole stage operation succeeds.

`outboxadapter.NewBatchProducer` is the supported `BatchRoute`. It uses the
Outbox atomic unique-batch put capability inside the host business
transaction. Local, direct NATS, and direct Kafka routes intentionally do not
implement `BatchRoute`; a batch facade call through them returns
`ErrUnsupportedCapability`. This prevents a hidden per-message fallback from
being mistaken for atomic producer batching.

### Outbox execution boundary

Outbox exposes a separate `BatchJob` registration with count, payload-byte, and
fill-wait limits. The zero value is 100 jobs, 4 MiB, and 25 ms;
`MaxMessages=1` exercises the same collector and outcome path. The older
`Job`, `RegisterJob`, and `ReservationBatchSize` contracts keep their existing
single-handler semantics.

One batch contains one exact `(name, schemaVersion)` capability and retains
database order. A handler result is keyed by `JobID` and must cover every input
exactly once. Missing, duplicate, or unknown IDs and a non-empty result paired
with a top-level error fail the service closed.

Item marker precedence is `Permanent`, `DeferAt`, `RetryAt`, then ordinary
error. Success deletes the fenced row; retry consumes the claimed attempt;
defer compensates it; permanent or exhaustion moves the job atomically to the
DLQ. A transient top-level error, panic, timeout, retry, or defer reschedules
the whole batch without attempts and uses a capability-wide bounded backoff.
Top-level `Permanent` and invalid results do not ACK or DLQ anything and stop
the service. Durable state resolves ambiguous commit outcomes on redelivery.

The complete item outcome set is applied in one fenced backend transaction.
PostgreSQL uses set-based `unnest`; MySQL and SQLite use bounded mutations in
one transaction. Picodata supports the same singleton path and rejects
`MaxMessages > 1`. Existing schemas are sufficient.

### Broker relay boundary

`outboxadapter.NewBatchRelayJob` accepts a narrow `BatchEnvelopePublisher`.
Invalid envelopes become item outcomes; the valid subset is passed to the
broker once.

NATS prepares every item and uses bounded asynchronous JetStream publication
with one future and `PubAck` per envelope. Stable message IDs retain broker
deduplication across commit ambiguity or relay restart. Kafka publishes the
valid subset as multiple records in one Kafka transaction; any produce or
commit failure defers that complete subset without consuming message attempts.
The existing synchronous route constructors and relay job remain unchanged.

### Capacity evidence

Capacity report specification 2.1 records runtime-confirmed ingress, relay,
and consumer modes; actual batch sizes and invocations; handler, publication,
and finalization latency; item outcomes; normalized PostgreSQL calls,
transactions/message, WAL/message, and checkpoints; producer/relay connection
creation, replacements, cancelled acquires, unusable releases, and acquired
high-water marks; image digests; resource
limits; pprof data; and PostgreSQL execution plans.

The supported frontier is a checkout-local NATS/PostgreSQL 18 profile with a
shared two-CPU SUT set, 2 GiB total container memory, and swap disabled. A
frontier claim requires three fresh-volume sustainable runs, exact
reconciliation, p95 at most two seconds, bounded lag and drain, and no dropped
iterations, retry, redelivery, or DLQ. Kafka remains a correctness gate, not a
performance claim.

## Consequences

Applications can independently compare legacy single ingress/relay/consumer,
consumer-only batching, relay plus consumer batching, and full producer/relay/
consumer batching. Batch size one is an explicit same-path control; no adapter
silently loops a single API to imitate a batch.

The API provides fewer SQL and broker transaction boundaries, but does not
promise exactly-once external effects or global ordering. Hosts still own the
business transaction, resource sizing, supervision, and operational capacity.
ADR-0005 remains the consumer-only decision; historical report specifications
and PostgreSQL 17 profiles are not comparable to the version 2.1 frontier.
