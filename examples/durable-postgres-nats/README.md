# Durable PostgreSQL + NATS demo and capacity stack

This checkout-level example makes GoMessenger's transaction and delivery boundaries visible in one run:

```text
PostgreSQL business transaction
  -> Outbox row
  -> relay
  -> JetStream PubAck
  -> PostgreSQL Inbox transaction
       |-- business projection
       `-- completion marker
  -> ACK, retry, or confirmed DLQ
```

It proves the following controlled scenarios:

1. a business `orders` insert and canonical event are staged in one PostgreSQL transaction;
2. the first consumer attempt writes a projection and returns `RetryAfter`; that write rolls back, then the second
   attempt commits once;
3. a second broker delivery with the same envelope identity reaches the consumer but the Inbox suppresses the handler;
4. a permanent handler failure rolls back its projection and reaches the DLQ;
5. confirmed replay starts a fresh bounded attempt generation and commits the projection;
6. repeating the same replay uses deterministic JetStream deduplication.

## Run

Requirements: Docker with Compose v2. From the repository root:

```sh
make demo-durable-postgres-nats
```

The target is equivalent to:

```sh
docker compose -f examples/durable-postgres-nats/compose.yaml \
  up --build --abort-on-container-exit --exit-code-from demo
```

A successful run ends with `durable demo passed`. Remove the stopped containers afterward:

```sh
make demo-durable-postgres-nats-down
```

The compose stack uses a dedicated PostgreSQL 18 container and a single-node NATS development topology. The app creates
the `demo` schema, applies embedded Outbox and namespaced Inbox migrations, and creates two demo business tables. It does
not drop or truncate data. Do not point this example at a shared database without reviewing those additive migrations.

## Capacity experiment

The same example provides a reproducible checkout-local capacity experiment for the real business path:

```text
k6 constant-arrival-rate HTTP POST /orders
  -> orders row + exact envelope measurement + Outbox job in one PostgreSQL transaction
  -> JetStream PubAck
  -> PostgreSQL Inbox transaction
  -> unique order_projection business effect
```

Run the quick profile from the repository root:

```sh
make capacity-nats
```

Useful variants are:

```sh
make capacity-nats-full
make capacity-nats-site
make capacity-inbox-postgres
CAPACITY_RATES=500 make capacity-nats
CAPACITY_MIN_RATE=500 make capacity-nats
make capacity-nats-down
make capacity-inbox-postgres-down
```

The quick profile uses a 15-second warm-up and 30-second stages at `50,100,250,500 msg/s`, with a 30-second drain
limit. The full profile uses a 30-second warm-up and two-minute stages at `50,100,250,500,1000,2000 msg/s`, with a
60-second drain limit. Both start with four Outbox workers and consumer concurrency four; override them with
`OUTBOX_WORKERS` and `NATS_CONSUMER_CONCURRENCY`.

`make capacity-nats-site` is a separate PostgreSQL 17 profile with one Outbox worker, one NATS consumer, a ten-
connection business pool, a 30-second warm-up, and two-minute stages at `250,325,350,400,500 msg/s`; each stage has a
30-second drain limit. Its default payload is one small deterministic order. Set `CAPACITY_PAYLOAD_PROFILE=mixed` to
use the existing 80/15/5 mix. `POSTGRES_IMAGE` may select a PostgreSQL 17 or 18 image without changing the repository's
PostgreSQL 18 default for quick/full runs.

k6 generates a deterministic 80/15/5 mix of small, medium, and large orders. The producer route serializes the actual
canonical envelope inside the business transaction and records its exact byte length and SHA-256 before delegating to
the real Outbox producer. A delegate failure therefore rolls back the business row, measurement, and Outbox job
together. The relay marks the same size/hash only after the real JetStream `PubAck`.

### Measurement boundary

k6 attaches the request dispatch time. PostgreSQL records that `offered_at` value plus `accepted_at`, envelope
`staged_at`/`published_at`, and projection `handled_at`. The controller uses the half-open interval from the first
offered request through exactly the configured stage duration:

```text
effective msg/s = unique projections committed inside the load window / load-window seconds
effective MiB/s = exact canonical envelope bytes for those message IDs / load-window seconds / 1,048,576
```

k6 summary time and pipeline drain are outside both denominators. The report separately records offered iterations,
HTTP `202` responses, committed business orders, staged envelopes, JetStream-confirmed publications, unique committed
projections, p50/p95/p99 business latency, backlog slope and maxima, drain time, redeliveries, DLQ, and database pools.
Business latency runs from k6 request dispatch (`offered_at`) to the committed projection write (`handled_at`).
The NATS consumer's existing observations separately record the complete Inbox `OperationHandle` and subsequent
`OperationBrokerAck` p50/p95/p99. The message-to-stage index is populated inside the producer transaction before it can
commit and is removed if that transaction fails, so a fast consumer cannot outrun measurement registration.

The isolated capacity PostgreSQL enables `pg_stat_statements`, query IDs, utility tracking, I/O timing, and WAL I/O
timing. Every stage retains snapshots before load, at load end, and after drain, plus normalized statement, database,
WAL, I/O, and sampled relevant-wait deltas. Controller SQL carries a stable probe marker and is excluded from Inbox
statement classification.

A measured stage is sustainable only when it has no HTTP failures or dropped iterations; accepted and committed
throughput are each at least 99% of target; second-half business backlog grows by no more than 1% of target rate;
business p95 is at most two seconds; bounded drain completes; and no unexpected redelivery or DLQ appears. After drain,
the controller exactly reconciles HTTP-accepted orders, business rows, measurements, broker-confirmed envelopes,
JetStream message delta, and unique projections. Loss, an extra effect, or a mismatched envelope hash is an integrity
failure and exits nonzero. Reaching an ordinary unsustainable rate stops the schedule and reports the boundary; use
`CAPACITY_MIN_RATE` when that boundary must become a local performance gate.

Artifacts are written to the ignored `tmp/capacity/<run-id>/` directory: `report.md`, `report.json`, `samples.jsonl`,
`environment.json`, `resources.jsonl`, per-stage k6 summaries/logs, and the Compose log. Resource samples include
container CPU, memory, and cumulative Block I/O. The stack and its dedicated named PostgreSQL/NATS
volumes are removed automatically. Set `KEEP_CAPACITY_STACK=1` to retain them for diagnosis, then use
`make capacity-nats-down` for explicit cleanup.

### PostgreSQL-only Inbox benchmark

`make capacity-inbox-postgres` starts a separate PostgreSQL 17 stack and calls the real PostgreSQL
`ProcessAttempt` path directly. It excludes HTTP, producer, Outbox, NATS, and ACK costs while retaining one
transactional handler insert. Each concurrency `1` and `4` case performs `1,000` warm-up operations followed by
`20,000` measured fresh successful messages, repeated three times. The report includes throughput, p50/p95/p99,
exact business/Inbox reconciliation, normalized statement calls/time/WAL, database/WAL/I/O deltas, environment, and
container resources. For a short diagnostic run, override `INBOX_CAPACITY_WARMUP`, `INBOX_CAPACITY_OPERATIONS`,
`INBOX_CAPACITY_CONCURRENCIES`, and `INBOX_CAPACITY_REPETITIONS`.

## Scope

The example module uses local `replace` directives because it deliberately proves the current checkout, which may be
ahead of published modules. It is compiled by `make check`; it does not prove published-module resolution. The capacity
experiment is explicitly local and opt-in, so `make check` and hosted CI do not execute it. Single-node NATS, local
Docker resources, a synthetic business process, and checkout-local replacements do not establish production capacity,
failover, or operational readiness. The separate [release process](../../docs/release.md) and
[real-service pilot](../../docs/decisions/0002-real-project-pilot.md) remain required.
