# Durable pipeline E2E

The checkout-level E2E module proves the complete durable path through public
GoMessenger and Outbox APIs:

```text
producer business transaction
  -> canonical envelope staged in Outbox
  -> real Outbox worker
  -> JetStream PubAck
  -> durable pull consumer
  -> Inbox and business transaction
  -> DoubleAck, retry, or confirmed DLQ
```

Run it from the repository root:

```sh
make test-e2e
```

The harness is deterministic and Docker-free. It starts an embedded JetStream
server, uses one file-backed SQLite database for producer state and Outbox, and
another for Inbox and consumer business state. Relay and consumer connections
are separate so a consumer connection can be closed after the Inbox commit but
before `DoubleAck` without interrupting the producer path.

## Scenarios

- producer rollback removes both the business write and staged envelope;
- committed Outbox work is published with its original message identity;
- a connection loss between Inbox commit and broker ACK causes redelivery, but
  the completed Inbox identity prevents a second business write, while W3C
  trace headers survive the Outbox and redelivery path;
- `RetryAfter` rolls back the first Inbox transaction, waits, redelivers, and
  commits exactly once;
- a permanent handler error rolls back business state, publishes a validated
  DLQ record, and then terminates the original message;
- offline DLQ replay makes no broker connection and exposes no wire data;
- confirmed replay waits for JetStream `PubAck`, uses deterministic broker
  deduplication, starts a fresh bounded attempt generation after terminal
  hand-off even when post-ACK cleanup fails, and a completed identity still
  suppresses a distinct broker delivery;
- native and CloudEvents structured/binary messages preserve trace context and
  global middleware order through a real embedded JetStream consumer;
- bounded shutdown makes the consumer unready, leaves unfinished work
  unacknowledged, and lets a replacement consumer commit it.

The module lives under `testdata/e2e`, runs with `GOWORK=off`, and uses explicit
local replacements only for the GoMessenger modules under test. Its Outbox root
and SQLite backend resolve as published `v0.11.0` modules. It is not a published
module. `make test-e2e` proves the local checkout;
`make test-consumer-release VERSION=vX.Y.Z` remains the separate proof that all
public GoMessenger tags resolve without local replacements.

## Runnable adoption demo

`examples/durable-postgres-nats` presents the core path as a visible one-shot application rather than a test harness.
It uses PostgreSQL for both Outbox/business state and a namespaced Inbox, plus a compose-managed NATS server. Run it with:

```sh
make demo-durable-postgres-nats
```

The demo is compiled by `make check` and its live compose run covers business/Outbox atomicity, an intentional retry,
a distinct duplicate broker delivery, permanent DLQ hand-off, and deterministic confirmed replay. Unlike
`testdata/e2e`, it is Docker-based and optimized for evaluation logs rather than exhaustive failure assertions. Both
remain checkout proofs; neither replaces the published clean-consumer gate or real-service pilot.

## Local NATS capacity experiment

The runnable example also exposes a long-lived HTTP service and a Go-controlled k6 experiment over the full
`POST /orders -> business transaction + Outbox -> JetStream -> Inbox -> order_projection` path. Run its quick profile
with:

```sh
make capacity-nats
make capacity-nats-site
make capacity-inbox-postgres
```

`make capacity-nats-full` runs the longer schedule; `CAPACITY_RATES=500 make capacity-nats` isolates one rate; and
`CAPACITY_MIN_RATE=500 make capacity-nats` turns the result into a checkout-local performance gate. The controller uses
k6 `constant-arrival-rate`, performs a separate warm-up, samples PostgreSQL/application/JetStream state each second,
drains after every stage, and stops at the first unsustainable rate.

`make capacity-nats-site` reproduces PostgreSQL 17, Outbox workers `2`, reservation batch `1`, consumer `1`, and isolated Outbox producer/relay
`9 + 1` pgx budget, and separate Inbox/measurement pool `10` topology with a
small deterministic payload and two-minute `250,325,350,400,500 msg/s` stages. Set
`CAPACITY_PAYLOAD_PROFILE=mixed` for the existing 80/15/5 payload mix. `make capacity-inbox-postgres` removes HTTP,
Outbox, and NATS and measures the real PostgreSQL `ProcessAttempt` path for concurrency `1` and `4`, with three
repetitions of `20,000` measured operations after a `1,000`-operation warm-up.

`OUTBOX_RESERVATION_BATCH_SIZE` controls only how many immediately available
jobs one worker reserves per claim (`1..1000`, default `1`). Handlers, PubAck,
conditional delete, retry, and DLQ remain per-job and sequential inside each
worker. For a short screening run:

```sh
OUTBOX_RESERVATION_BATCH_SIZE=16 \
CAPACITY_RATES=500,650 \
CAPACITY_WARMUP_DURATION=10s \
CAPACITY_STAGE_DURATION=30s \
make capacity-nats-site
```

Reports include the exact batch in both `report.json` and `report.md`; compare
runs only with the same two workers, `9 + 1` Outbox pools, one consumer,
payload profile, durations, and SLO.

The primary metrics use a timestamped half-open load window and exclude drain:

```text
relay msg/s = published delta / load-window seconds
consumer msg/s = committed projection delta / load-window seconds
Outbox lag = staged delta - published delta
consumer lag = published delta - committed projection delta
consumer MiB/s = exact canonical envelope bytes for committed message IDs / load-window seconds / 1,048,576
```

After drain, accepted HTTP responses must reconcile exactly with business orders, transactional envelope measurements,
JetStream-confirmed publications, stream message delta, and unique projections. Integrity failures exit nonzero;
ordinary performance saturation reports the first unsustainable stage, while passing the whole schedule reports
`capacity >= maximum tested rate`. Capacity report spec `1.3` records the exact
Outbox module version used by the binary, separate relay/consumer rates and
lags, and no longer emits the former `effective*` fields. Reports and raw
samples live under ignored `tmp/capacity/<run-id>/`.

The isolated PostgreSQL stacks preload `pg_stat_statements` and enable query IDs, utility tracking, I/O timing, and WAL
I/O timing. Reports retain before/load-end/post-drain statement/database/WAL/I/O snapshots, sampled lock/I/O/WAL waits,
Inbox handle and broker ACK percentiles, and `resources.jsonl` container CPU/RAM/cumulative Block I/O. Probe SQL is
marked and excluded from Inbox statement classification.

This Docker experiment is explicit and local: it is not part of `make check` or hosted CI, supports NATS only, and does
not replace the real-service pilot or establish production capacity. Its exact commands, profiles, classification
rules, artifacts, and tuning variables are documented in the
[example README](../examples/durable-postgres-nats#capacity-experiment).

## Kafka compatibility pipeline

The same E2E module contains an opt-in Kafka pipeline. It is skipped when no broker is declared, so the source-only
`make check` gate remains Docker-free. Run the explicit local gate with:

```sh
make test-kafka
```

The harness starts official single-node Kafka 4.1.2 and 4.3.1 images one at a time with separate internal/external
listeners and one-partition transaction/group metadata topics. Service topics use two partitions with one consumer
worker: after observing the structured retry-deferral event, the test proves that a barrier from the other partition is
handled before the two-second retry deadline and that exactly one second handler attempt follows. The same scenario
also proves declarative topic convergence, transactional direct publish, retry hand-off, Inbox attempt accounting,
Outbox relay and duplicate suppression, transactional DLQ, protected replay, static worker identity, read-committed
visibility, and graceful runtime shutdown.

The same target runs locally and in independent hosted matrix jobs for both supported Kafka versions. It does not prove
multi-broker failover, capacity, production credentials, deployment, or a live operational smoke.
