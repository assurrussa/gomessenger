# GoMessenger Level 2 roadmap

Status: proposed, evidence-driven roadmap. This document describes future work; it does not extend the current public
API or compatibility guarantees.

## Product boundary

The current product is a typed durable messenger for Go:

- typed command, local query, and event descriptors;
- canonical envelopes, correlation, causation, middleware, logging, metrics, and tracing;
- local routes plus transactional Outbox and Inbox boundaries;
- NATS JetStream and Kafka adapters with bounded retry, DLQ, replay, topology, and lifecycle support.

Level 2 develops this into an operational messaging platform. It focuses on schema contracts, explicit broker
capabilities, key-based ordering, evidence-backed batching, and production validation. It does not turn GoMessenger into
a distributed application framework.

Saga engines, workflow orchestration, service discovery, and generic distributed request/reply remain outside Level 2.
Applications may build process managers on GoMessenger primitives, while remote synchronous queries should continue to
use a separately designed transport contract.

## Principles

1. **Evidence before API.** Pilot measurements and failure data must justify each new abstraction.
2. **Transport-neutral contract, transport-specific truth.** Core APIs express required semantics; adapters disclose
   what they can implement rather than pretending that Kafka and JetStream are identical.
3. **Fail closed.** Unsupported guarantees and incompatible schemas must fail during build, topology planning, startup,
   or CI instead of silently degrading at runtime.
4. **No hidden cardinality or unbounded state.** Metrics, retry state, batches, and schema metadata remain bounded.
5. **Compatibility over convenience.** Public API additions require an ADR, external-consumer compilation, and migration
   guidance.

## Workstream 0: pilot and performance baseline

Before adding Level 2 APIs, run one non-critical real service flow through Outbox, a broker, Inbox, handler, retry, DLQ,
and replay. The pilot should record:

- message volume, payload-size distribution, producer and consumer concurrency;
- end-to-end latency and broker publish latency percentiles;
- PostgreSQL transaction duration, pool contention, duplicate rate, and prune cost;
- retry delay, DLQ rate, replay outcomes, shutdown duration, and recovery after reconnect/restart;
- CPU, allocations, memory, broker lag, and Inbox growth during steady load and bursts.

The repository benchmark suite remains split into clearly named layers:

1. local public method calls (`Send`, `Query`, `Publish`);
2. direct broker publication;
3. Outbox stage and relay;
4. Inbox transaction and handler execution;
5. complete durable pipelines.

GitHub Actions stores raw samples and `benchstat` comparisons without a machine-dependent pass/fail threshold. Stable
service-level objectives are set only after pilot evidence exists.

Exit criteria:

- a documented pilot topology and rollback plan;
- repeatable baseline results with environment metadata;
- identified bottlenecks and an ordered Level 2 backlog based on measured impact.

## Workstream 1: schema identity and compatibility

The current descriptor already carries a stable name, schema version, content type, encoding, and optional schema URI.
Level 2 should make the schema contract independently verifiable:

- define a canonical schema identity and fingerprint representation in the manifest;
- record codec and compatibility policy explicitly;
- add a CLI/CI command that compares a proposed manifest with a released baseline;
- distinguish backward, forward, full, and intentionally breaking changes;
- require an explicit version transition for incompatible payload changes;
- keep external registry support behind optional adapters after the internal contract is stable.

The first implementation should validate repository-owned schemas without requiring a network service. Integrations with
external schema registries are a later adapter concern, not a root-module dependency.

Exit criteria:

- deterministic fingerprints across machines;
- golden compatibility fixtures for supported codecs;
- clean diagnostics that identify the descriptor and incompatible field/change;
- release CI that blocks undeclared breaking changes.

## Workstream 2: broker capability model

Every transport adapter should expose a machine-readable capability set. Candidate capabilities include:

- transactional publication;
- consumer offset/ack transaction support;
- broker deduplication;
- delayed delivery or retry scheduling;
- partition/key routing;
- per-key ordering;
- producer and consumer batching;
- replay identity and topology-management support.

A route or consumer that requests an unsupported guarantee must fail during configuration or startup. The capability
model must describe semantics, not vendor names, and must not make weaker behavior look equivalent.

Examples of distinctions that must remain visible:

- Kafka partitions and JetStream subjects/consumers provide different ordering and retry behavior;
- broker-level deduplication is not equivalent to transactional Inbox suppression;
- producer transactions do not imply exactly-once external side effects;
- delayed retry may permit later source messages to overtake the failed message.

Exit criteria:

- capability declarations for NATS, Kafka, local, and Outbox routes;
- configuration validation tests for supported and rejected combinations;
- capability information in topology/manifest output and documentation.

## Workstream 3: partition key and ordering semantics

Level 2 should define a transport-neutral partition-key contract for one-way messages. The API shape remains undecided;
possible forms include a typed key selector attached to a descriptor or route. The contract must answer:

- when the key is computed and whether it is part of the canonical fingerprint;
- how empty, oversized, or non-deterministic keys fail;
- what ordering is guaranteed before and after retry, DLQ, and replay;
- whether one slow key can block unrelated keys;
- how partition-count changes and rebalances affect processing;
- how Kafka and JetStream map the same semantic requirement without leaking transport types into the core module.

Ordering should normally mean parallelism across keys and serial processing within one key. A strict global ordering mode
is not planned.

Exit criteria:

- an accepted ADR with failure and migration semantics;
- multi-partition/rebalance tests that prove per-key order;
- documented retry-overtaking behavior;
- load results showing bounded memory and no cross-key head-of-line blocking.

## Workstream 4: end-to-end consumer batching

Implemented on 2026-09-01. Broker-native producer batching remains below GoMessenger; the supported API adds only a
consumer batch boundary. [ADR-0005](decisions/0005-batch-consumer.md) defines the accepted NATS/Kafka and
PostgreSQL/SQLite contract. The earlier prototype report remains historical evidence, not a release gate.

The accepted contract defines:

- maximum records, bytes, and wait time for a batch;
- whether a batch may span partitions or descriptors;
- Inbox identity and attempt accounting for every member;
- transaction behavior when only part of a batch fails;
- retry and DLQ behavior for failed members;
- ordering and cancellation semantics;
- memory and shutdown bounds.

The existing single-message API remains supported. Batch support is optional and capability-checked; backends without
the required partial-result transaction capability fail closed and never silently loop through singleton handlers.

Completed exit criteria:

- an ADR defining partial-failure and transaction semantics;
- correctness tests for mixed success, retry, permanent failure, duplicate delivery, and drain;
- NATS and Kafka command/event parity, PostgreSQL and SQLite parity, and clean external-consumer compilation;
- a supported batch-1 versus real-batch invocation/transaction control.

Any new throughput, latency, WAL, or RSS claim remains a separate evidence task with clean provenance and fixed
resources.

## Workstream 5: operational validation

Level 2 readiness requires repeatable tests beyond unit and happy-path E2E coverage:

- sustained load and burst tests for NATS, Kafka, Outbox, and PostgreSQL Inbox;
- soak tests long enough to expose connection, goroutine, timer, and storage leaks;
- broker restart, network interruption, reconnect, and authentication-rotation scenarios;
- Kafka rebalance, static-member replacement, transaction fencing, and partition movement;
- JetStream heartbeat loss, consumer recreation, stream limits, and PubAck uncertainty;
- PostgreSQL lock contention, pool starvation, failover, large Inbox history, and concurrent prune;
- forced shutdown at every transaction boundary;
- deterministic replay and duplicate suppression after partial failures.

Results must include the exact Go version, architecture, broker/database versions, topology, payload distribution,
concurrency, and test duration. Cross-machine microbenchmarks remain informational; durable-pipeline regressions should
use controlled environments and workload-specific budgets.

Exit criteria:

- documented load, soak, and chaos procedures;
- retained raw results and environment manifests;
- no unbounded resource growth in the accepted test window;
- recovery and data-integrity assertions for every injected failure.

## Recommended sequence

1. Complete the real-project pilot and baseline the current system.
2. Introduce schema identity/compatibility and the broker capability model.
3. Design partition-key semantics using pilot ordering requirements.
4. Validate the supported batch consumer against real-service workload and capacity evidence.
5. Promote individual guarantees to supported status only after load/soak/chaos evidence exists.

This ordering deliberately avoids speculative APIs. Pilot evidence may change the priority, but it must not weaken the
transactional, bounded, and fail-closed invariants of the current library.
