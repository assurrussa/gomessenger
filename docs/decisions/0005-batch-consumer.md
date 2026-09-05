# ADR-0005: End-to-end partial-result consumer batching

## Status

Accepted on 2026-09-01.

## Context

The original durable consumer API decoded and handled one command or event per
Inbox transaction. Outbox reservation and broker producer batching did not
reduce handler or Inbox transaction overhead. A checkout-local PostgreSQL/NATS
prototype established the semantics and supplied useful historical screening
evidence, but it duplicated supported adapter code and covered only one narrow
topology.

GoMessenger now needs a consumer-only batch path that keeps the existing
single-message constructors stable, preserves individual message outcomes, and
works across the supported NATS, Kafka, PostgreSQL, and SQLite combinations.

## Decision

### Public surface

The root module defines `BatchItemKey`, `BatchItemResult`, `BatchResult`,
`BatchHandler[T]`, batch-only middleware, `ChainBatchHandler`, `BatchConfig`,
`ErrInvalidBatchResult`, `DeferAfter`, and `DeferDelay`.

NATS and Kafka expose `NewBatchCommandConsumer` and
`NewBatchEventConsumer`. The existing `Handler[T]`, single-message
constructors, producers, Outbox staging, and relay remain unchanged. A batch
constructor rejects single-message middleware; it never adapts a
single-message handler by looping.

`BatchConfig{}` is valid and resolves to 100 messages, 4 MiB of canonical
envelope bytes, and 25 ms from the first admitted delivery. Non-zero values
override the defaults. Negative values, nil middleware, invalid concurrency,
and bound arithmetic overflow fail construction. `MaxMessages=1` exercises the
same batch path as a singleton control.

### Identity and result contract

One handler invocation contains one descriptor. Input contains only unique,
active logical messages in broker order. The result may be in any order but
must contain exactly one item for each `(Source, MessageID)` passed to the
handler. A missing, duplicate, or unknown key is `ErrInvalidBatchResult` and
fails the consumer closed.

Deliveries with equal identity and canonical fingerprint coalesce. A completed
Inbox identity is ACKed without entering the handler. Reusing an identity with
a different fingerprint is an individual terminal DLQ outcome. Terminal and
exhausted attempts are also filtered before invocation and stay closed after terminal handoff or changes to the
attempt limit; see [ADR-0006](0006-delivery-guarantees.md).

One batch uses one Inbox/business SQL transaction. The handler first
classifies the entire batch and performs business SQL only for the successful
subset. GoMessenger cannot associate arbitrary SQL writes with a result key, so
v1 does not add automatic per-item savepoints. Every business write that needs
Inbox atomicity must use the transaction in the handler context.

### Item and batch outcomes

Failure marker precedence is `Permanent`, `DeferAfter`, `RetryAfter`, then an
ordinary error.

| Result | SQL and attempts | Broker outcome |
|---|---|---|
| Item success | business writes and completion commit; attempt increments | ACK or offset commit |
| Item ordinary error or `RetryAfter` | failed item has no writes; attempt increments | retry, then DLQ at the limit |
| Item `DeferAfter` | failed item has no writes; attempt is unchanged | exact delayed retry |
| Item `Permanent` | failed item has no writes; terminal attempt commits | confirmed DLQ |
| Top-level ordinary error, panic, timeout, `RetryAfter`, or `DeferAfter` | complete rollback; all attempts unchanged | unbounded batch retry with exact or exponential delay |
| Top-level `Permanent` or invalid exact-cover result | complete rollback; all attempts unchanged | no ACK; consumer fails closed |

A non-nil top-level error is valid only with an empty `BatchResult`. An
ambiguous SQL commit is resolved by durable Inbox state on redelivery; the
library does not attempt to decrement a counter that may already be committed.

`ProcessAttempt` uses the same outcome model in singleton form. In particular,
`DeferAfter` rolls back business writes and does not consume an attempt.

### Inbox capability

`BatchAttemptBackend` is an optional capability; existing `Backend` and
`AttemptBackend` interfaces are unchanged. Batch constructors require it and
fail closed when it is absent. PostgreSQL implements deterministic identity
locking and set-based `unnest` mutations. SQLite uses a serializable
transaction and bounded multi-row statements. Batching itself reuses the existing attempt tables; terminal
retention additionally requires the additive migration and optional `TerminalRetentionBackend` in ADR-0006.

### NATS

Each worker owns at most one filling or active batch. `Concurrency` is the
number of concurrent batch invocations. A batch flushes at the first message,
canonical-byte, or wait limit; an individually oversized valid envelope is a
singleton. One heartbeat covers the complete filling/active batch. Drain stops
admission and flushes a partial batch immediately.

After the SQL commit, ACKs use confirmed `DoubleAck` in a group bounded to 16
operations per worker. Retry and defer use `NakWithDelay`. DLQ publication uses
a deterministic message ID, waits for `PubAck`, and only then confirms the
source delivery. Both modes share DLQ preparation: normal records stay v1; unrepresentable or oversized replay
captures become bounded, non-replayable quarantine v2 in the same subject. Internal preparation failure stops the
consumer and removes readiness. A DLQ outage retains the affected worker slot; other workers
continue. A failed source ACK is safe because redelivery is suppressed by the
completed Inbox identity or retained terminal generation.

Commands use native envelopes. Events support native, structured CloudEvents,
and binary CloudEvents, matching the existing single-message consumer.

### Kafka

One handler batch is an ascending, contiguous range from one concrete
topic-partition. Rebalancing remains blocked from the first poll until the
Kafka batch transaction commits or aborts. Polled records from other partitions
update the earliest observed offset map without unbounded record retention and
fill stops if a subsequent poll yields no records for the selected partition.
Any unconsumed offset is rewound and cannot enter the offset commit.
Because rebalance blocking is held through the batch lifecycle (fill, decode,
inbox, handler, and transaction), the consumer's `RebalanceTimeout` is sized
dynamically to cover `MaxWait + Timeout + FinalizationTimeout + 2*OperationTimeout + 5s`,
preventing group eviction during legitimate batch processing.

After the Inbox commit, one Kafka transaction publishes all retry and DLQ
records and commits the selected range offset. On a top-level handler failure,
all valid items move to the durable retry tier with exact `not-before` and are
committed with the source offset in the Kafka transaction, while preflight
terminal DLQs are published within the same transaction. An aborted
transaction is replayed. A fenced or otherwise unusable transactional session
is recreated under the worker supervisor.

The batch-wide retry streak is separate from message attempts, uses
`BaseRetry..MaxRetry`, never causes DLQ, and resets after a valid result.
Readiness remains admission-based; recreation/backoff appears in deep health,
logs, and observations.

### Observability and lifecycle

Batch observations are bounded: admitted size, canonical bytes, fill and
handler duration, handler logical-message count, and ACK/retry/defer/DLQ
counts. Per-item observations preserve message ID, attempt, retry/DLQ outcome,
and each message's extracted trace context. The shared handler context contains
cancellation, deadline, SQL transaction, and batch instrumentation, but does
not inherit arbitrary metadata from the first message.

Shutdown owns every worker, heartbeat, and retry timer. Graceful drain stops
admission and waits for active work; forced cancellation rolls back unfinished
Inbox work and leaves broker deliveries replayable.

## Consequences

Applications gain true consumer-side batching without changing their producer
or Outbox path. Partial success is explicit and efficient, but applications
must follow the classify-then-write rule. Global ordering is not promised;
partial retry can be observed after later successful messages.

The supported implementation replaces and removes the old internal prototype
and its runnable capacity harness. The historical performance report is kept
as evidence about that earlier checkout only; its 2x, 1.3x, WAL, latency, and
RSS thresholds do not gate this API implementation or constitute release
evidence.
