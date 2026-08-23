# ADR-0002: Require a real-project pilot for operational validation

- Status: accepted validation stage; pilot target pending
- Date: 2026-08-24

## Context

Unit, race, checkptr, embedded JetStream, SQLite E2E, PostgreSQL integration, and clean-consumer tests prove the library
contracts in controlled environments. They do not prove service integration cost, production-shaped throughput,
operational ownership, deployment behavior, or useful observability under a real workload.

## Decision

Before describing GoMessenger as operationally production-proven, adopt it in one real but non-critical service flow.
The first pilot must exercise a one-way command or event; it must not introduce distributed query semantics.

The target flow must have:

- Go 1.27 and an owned NATS JetStream plus PostgreSQL integration path;
- one business transaction that stages an Outbox message;
- one consumer transaction with an observable, safely repeatable database effect;
- a stable message descriptor and a rollback path that does not require changing existing business semantics;
- host-owned migrations, connection sizing, lifecycle supervision, metrics, logs, traces, and runbook.

## Required scenarios

The pilot must demonstrate in a production-like environment:

1. business commit and Outbox staging, plus rollback without publication;
2. relay publication with JetStream `PubAck`;
3. Inbox commit followed by lost-ACK redelivery without a duplicate business effect;
4. transient retry, permanent failure, DLQ inspection, and confirmed replay;
5. service restart, NATS reconnect, temporary PostgreSQL failure, and graceful drain;
6. measured handler latency, consumer lag, database-pool pressure, retry/DLQ rates, and a representative soak window.

## Exit criteria

The pilot is successful only when the host can operate the flow from documented dashboards and runbooks, no committed
database effect is duplicated under the failure matrix, and measured capacity leaves explicit headroom at the intended
service load. Passing repository E2E alone does not satisfy this decision.

## Open selection

The service and message flow are intentionally not selected here. Selection must prefer a reversible, low-blast-radius
business flow and must not replace a workflow whose current timing, recipient snapshot, ordering, or retry semantics
would change merely to adopt GoMessenger.
