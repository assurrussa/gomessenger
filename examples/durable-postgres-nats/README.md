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
k6 constant-arrival-rate HTTP POST /orders or /orders/batch
  -> orders rows + Outbox jobs in one PostgreSQL transaction
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
make capacity-frontier
make capacity-frontier-matrix
make capacity-outbox-batch-screen
make capacity-batch-proof
make capacity-inbox-postgres
CAPACITY_RATES=500 make capacity-nats
CAPACITY_MIN_RATE=500 make capacity-nats
make capacity-nats-down
make capacity-inbox-postgres-down
```

The quick profile uses a 15-second warm-up and 30-second stages at `50,100,250,500 msg/s`, with a 30-second drain
limit. The full profile uses a 30-second warm-up and two-minute stages at `50,100,250,500,1000,2000 msg/s`, with a
60-second drain limit. Both start with four Outbox workers and consumer concurrency four; override them with
`OUTBOX_WORKERS`, `OUTBOX_RESERVATION_BATCH_SIZE`, and `NATS_CONSUMER_CONCURRENCY`.
The reservation batch defaults to `1` and accepts `1..1000`; it changes claim
prefetch only, while each worker still handles, publishes, and acknowledges
jobs sequentially. Outbox staging and relay use separate pgx pools, controlled by
`OUTBOX_PRODUCER_MAX_CONNS` and `OUTBOX_RELAY_MAX_CONNS`.

`make capacity-nats-site` is retained as a historical screening profile. New
capacity claims use `make capacity-frontier` or `make
capacity-frontier-matrix`: PostgreSQL 18, a shared SUT cpuset `0-1`, API and
NATS at 512 MiB each, PostgreSQL at 1 GiB, and swap disabled. The matrix first
compares Outbox/consumer concurrency and connection-pool topologies `O1/C1
4+1/DB4`, `O2/C1 6+2/DB6`, and `O2/C2 6+2/DB8`, then fixes the winner. Exact
image digests and container OOM/restart state are retained with every run.

k6 generates either deterministic small orders or an 80/15/5 mix. Single
ingress calls `/orders`; producer-batch ingress groups at most 100 messages and
calls `/orders/batch`. The bulk endpoint pre-generates every message ID, inserts
all business rows with one `INSERT ... FROM unnest`, and uses one typed producer
batch in the same transaction. Any validation, idempotency, or staging failure
rolls back the complete request. The Outbox repositories are relay-pool owned,
but execute staging through the `pgx.Tx` carried in context. Producer and relay
connections use distinct PostgreSQL `application_name` values.

Canonical envelope size/hash and the real JetStream `PubAck` timestamp go to a
bounded asynchronous recorder backed by an UNLOGGED table. The recorder
deduplicates message IDs and persists bounded groups with `unnest`; it does not
add WAL to the business or relay transaction. Queue overflow, recorder failure,
or an unclean process restart invalidates the run but never changes delivery,
retry, or DLQ outcomes. Shutdown stops the relay before a bounded final flush.

### Measurement boundary

k6 attaches the request dispatch time. PostgreSQL records that `offered_at` value plus `accepted_at`, envelope
`staged_at`/`published_at`, and projection `handled_at`. The controller uses the half-open interval from the first
offered request through exactly the configured stage duration:

```text
relay msg/s = JetStream-confirmed publications inside the load window / load-window seconds
consumer msg/s = unique projections committed inside the load window / load-window seconds
Outbox lag = staged envelopes - JetStream-confirmed publications
consumer lag = JetStream-confirmed publications - unique committed projections
consumer MiB/s = exact canonical envelope bytes for committed message IDs / load-window seconds / 1,048,576
```

k6 summary time and pipeline drain are outside every throughput denominator. After drain and recorder flush, the controller
reconstructs the load-window counts from the stored timestamps, so asynchronous publication recording cannot move the
measurement boundary. Once-per-second progress samples use stage-local
in-process accepted/published/committed counters plus the live Outbox and
JetStream snapshots; they do not repeatedly rescan the growing business
tables. Load-end and post-drain counts still come from durable SQL. The report separately records offered iterations,
HTTP `202` responses, committed business orders, staged envelopes, JetStream-confirmed publications, unique committed
projections, relay/consumer throughput, Outbox/consumer lag and growth, p50/p95/p99 business latency, end-to-end backlog
slope and maxima, drain time, redeliveries, DLQ, actual Outbox handler/publish/finalization calls, sizes, latency and
outcomes, the exact checkout identities, and separate
producer/relay pool sizes, acquire counts/durations, empty-acquire counts, and acquired-connection high-water marks.
Business latency runs from k6 request dispatch (`offered_at`) to the committed projection write (`handled_at`).
The NATS consumer's existing observations separately record the complete Inbox `OperationHandle` and subsequent
`OperationBrokerAck` p50/p95/p99. The message-to-stage index is populated inside the producer transaction before it can
commit and is removed if that transaction fails, so a fast consumer cannot outrun measurement registration.

The isolated capacity PostgreSQL enables `pg_stat_statements`, query IDs, utility tracking, I/O timing, and WAL I/O
timing. Every stage retains snapshots before load, at load end, and after drain,
plus normalized SQL calls, transactions/message, WAL/message, completed
checkpoints, statement/database/WAL/I/O deltas, and sampled relevant waits.
Capacity-only artifacts include CPU/heap/goroutine profiles and PostgreSQL
`EXPLAIN (ANALYZE, BUFFERS, WAL)` for claim/finalization. Controller SQL carries
a stable probe marker, is excluded from application statement cost, and runs
with PostgreSQL parallel gather disabled so verifier queries cannot consume the
two SUT workers or Docker shared memory.

A measured stage is sustainable only when it has no HTTP failures or dropped iterations; accepted, relay, and consumer
throughput are each at least 99% of target; second-half Outbox, consumer, and end-to-end backlog each grow by no more
than 1% of target rate; business p95 is at most two seconds; bounded drain
completes; and no unexpected Outbox error/retry/defer/DLQ, broker redelivery, or
consumer DLQ appears. After drain,
the controller exactly reconciles HTTP-accepted orders, business rows, measurements, broker-confirmed envelopes,
JetStream message delta, and unique projections. Loss, an extra effect, or a mismatched envelope hash is an integrity
failure and exits nonzero. Reaching an ordinary unsustainable rate stops the schedule and reports the boundary; use
`CAPACITY_MIN_RATE` when that boundary must become a local performance gate.

### Full pipeline A/B and frontier

The capacity service preserves the established consumer as the default:

```sh
make capacity-nats-site-single
```

The new consumer is selected explicitly. `MaxMessages=1` is the control for
the same batch collector, handler, and shared-transaction path; `100` enables a
real batch:

```sh
make capacity-nats-site-batch-1
make capacity-nats-site-batch-100
```

`CONSUMER_BATCH_MAX_BYTES` defaults to `4194304` canonical bytes and
`CONSUMER_BATCH_MAX_WAIT` defaults to `25ms`. `NATS_CONSUMER_CONCURRENCY`
means concurrent message handlers in `single` mode and concurrent batch
invocations in `batch` mode. A defensible A/B keeps every other setting,
including `OUTBOX_RESERVATION_BATCH_SIZE`, rate schedule, payload, database
pools, resources, and repetitions identical.

True ingress and relay paths are independent of reservation prefetch:

```sh
OUTBOX_INGRESS_MODE=single OUTBOX_RELAY_MODE=single CONSUMER_MODE=single
OUTBOX_INGRESS_MODE=single OUTBOX_RELAY_MODE=single CONSUMER_MODE=batch
OUTBOX_INGRESS_MODE=single OUTBOX_RELAY_MODE=batch  CONSUMER_MODE=batch
OUTBOX_INGRESS_MODE=batch  OUTBOX_RELAY_MODE=batch  CONSUMER_MODE=batch
```

The frontier controller names these `legacy`, `consumer-batch`, `relay-batch`,
and `full-batch`. It starts at 250 msg/s with a 100 msg/s fallback, climbs
geometrically, bisects to a 50 msg/s step, and confirms the best candidate in
three fresh-volume runs. General frontier and matrix runs have a two-minute
precondition, wait for one
completed checkpoint within five minutes, measures for two minutes, and drains
within 60 seconds. `CAPACITY_FRONTIER_RUN_CONTROL=1` adds a matched singleton control;
it does not build a separate frontier.

Examples:

```sh
CAPACITY_FRONTIER_VARIANT=full-batch CAPACITY_PAYLOAD_PROFILE=small make capacity-frontier
CAPACITY_RUN_TUNED=0 make capacity-frontier-matrix
make capacity-frontier-matrix
```

For a fast Outbox-only development comparison, run:

```sh
make capacity-outbox-batch-screen
```

It holds the consumer in batch mode and compares singleton Outbox ingress and
relay (`consumer-batch`) with batch ingress and relay (`full-batch`). The
default is one `mixed` cell per variant at 1,500 msg/s, with 30 seconds of
warm-up and 60 seconds of measured load. One variant normally takes about two
minutes and the pair about four to five minutes including targeted preflight
and Docker turnover. Artifacts are marked
`development-screen` and `SCREEN_ONLY`; dirty checkout provenance is retained.
Both roles must reconcile with zero connection churn, and the candidate must
be sustainable with an average Outbox batch of at least 10. The control may be
unsustainable and remains only a baseline. No frontier, repeated confirmation,
or `>=1.3x` claim is produced.

For a merge-decision claim about true Outbox batching, use the fixed
PostgreSQL 18/NATS `o2-c2` proof instead of the topology-selection matrix:

```sh
make capacity-batch-proof
make capacity-batch-proof-verdict \
  PROOF_DIR=tmp/capacity/frontiers/<proof-id>
```

The proof requires clean GoMessenger and sibling Outbox checkouts. It compares
`consumer-batch` with `relay-batch` and `legacy` with `full-batch` for `small`
and `mixed` payloads, requires a 1.3x frontier ratio, and checks the matched
common-rate latency, PostgreSQL cost, actual batch sizes, resource use, and WAL
waits. The launcher first runs Outbox `make check-all` and GoMessenger
`make check-workspace` plus `make test-batch-integration`. Raw reports remain under
ignored `tmp/capacity/`; after PASS, the compact Markdown verdict is copied to
`docs/performance/<proof-id>.md`. The manifest and verdict explicitly use the
`checkout-workspace` evidence scope and retain both clean checkout commits. A
PASS is not evidence for a published release or production readiness;
published compatibility still requires the separate `GOWORK=off` release
readiness and clean release-consumer probes.

Stock and tuned PostgreSQL 18 results are distinct. Tuned candidates cover
checkpoint cadence, memory/WAL buffers, and their combination. Durability
settings `fsync`, `synchronous_commit`, and `full_page_writes` stay enabled. A
new claim index is admissible only after three matched runs show at least 20%
lower median claim time without more than 5% higher WAL/message.

Artifacts are written to the ignored `tmp/capacity/<run-id>/` directory: `report.md`, `report.json`, `samples.jsonl`,
`environment.json`, `resources.jsonl`, pprof files, `postgres-explain.txt`, container state, per-stage k6 summaries/logs,
and the Compose log. Resource samples include
container CPU, memory, and cumulative Block I/O. The stack and its dedicated named PostgreSQL/NATS
volumes are removed automatically. Set `KEEP_CAPACITY_STACK=1` to retain them for diagnosis, then use
`make capacity-nats-down` for explicit cleanup.

Capacity report spec `2.1` records the reservation batch as
`environment.outboxReservationBatchSize`, the binary-resolved dependency as
`environment.outboxVersion`, runtime-confirmed ingress/relay selection as
`environment.outboxIngressMode` and `environment.outboxRelayMode`, and consumer selection as
`environment.consumerMode`, `consumerBatchMaxMessages`,
`consumerBatchMaxBytes`, and `consumerBatchMaxWaitMillis`. Startup fails when
the runner and application consumer configurations differ. The report includes
these fields in the Markdown environment. Every measured stage also records
actual Outbox handler, broker publish, SQL finalization, and consumer-handler
invocations, messages per invocation, maximum observed batch size, duration,
and item outcomes. It also records producer and relay pool connection creation,
replacements, cancelled acquires, unusable releases, and acquired-connection
high-water marks; any churn makes the stage unsustainable.
The capacity-only `/benchmark/stats` response uses one Outbox queue snapshot:
`outbox.observedAt`, nullable global `oldestAvailableAt`, and `byCapability`
groups expose exact `(name, schemaVersion)` backlog. A group's `supported`
flag means that this process registered that exact handler; it is not a
cluster-wide capability registry.

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
