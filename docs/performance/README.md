# Performance evidence

This directory keeps versioned, reviewable performance snapshots for GoMessenger.
It complements the raw capacity artifacts under `tmp/capacity/`; it does not
replace them or turn a checkout-local experiment into a production benchmark.

## Evidence levels

- `make bench-all` measures allocation-aware, synchronous local dispatch. It
  does not exercise PostgreSQL, Outbox, a broker, Inbox, or business effects.
- `make capacity-frontier` measures one normalized checkout-local PostgreSQL
  18/NATS frontier; `make capacity-frontier-matrix` compares supported
  topologies, pipeline variants, payloads, and database profiles.
- `make capacity-outbox-batch-screen` is a short development signal for the
  old and full-batch Outbox paths. It does not find a frontier or prove a
  capacity advantage.
- A real-service pilot remains the production-readiness gate described by
  [ADR-0002](../decisions/0002-real-project-pilot.md).

Only compare runs with the same report specification, payload, topology,
software versions, stage duration, and host/container resources. Treat a
configuration as a measured capacity floor only when all three clean repetitions
are sustainable and pass post-drain reconciliation.

Current report spec `2.1` distinguishes runtime-confirmed ingress, relay, and
consumer modes. It records actual batch handler/publish/finalization calls,
sizes, latency and outcomes; normalized SQL calls, transactions/message,
WAL/message and checkpoints; exact image/checkout identity and resource limits;
producer/relay pool expansion, replacements, cancelled acquires, unusable
releases and acquired-connection high-water marks; pprof; and PostgreSQL
claim/finalization plans. Historical spec `1.3`, `1.4`, and `2.0`
evidence predates true producer/relay batching and must not be compared by
absolute throughput or used for ADR-0006 capacity claims.

## Baselines

| Evidence | Status | Defensible result |
|---|---|---|
| [True Outbox batch small-payload frontier](outbox-true-batch-small-frontier-20260903.md) | Primary true-batch matched frontier, checkout-workspace | At clean GoMessenger `dc33c96` and Outbox `e579abd`, the `full-batch` path confirmed 2,550 msg/s in 3/3 runs versus 450 msg/s for the singleton-Outbox `consumer-batch` control, a 5.67x frontier improvement |
| [v0.2.2 clean batch matrix](v0.2.2-clean-batch-matrix.md) | Primary release-candidate, checkout-local | At exact clean commit `6836d1b3`, batch `100` completed the 1500 msg/s cell in 3/3 runs; batch `16` completed it in 1/3 |
| [v0.2.2 batch-16 pool A/B](v0.2.2-batch16-pool-ab.md) | Historical tuning baseline, checkout-local | Identified `8 producer + 2 relay` as the better pool split, but its batch-16 1500 msg/s result was not reproduced by the later clean interleaved matrix |

The historical [batch consumer experiment](batch-consumer-experiment.md) keeps
its checkout-local matrix, direct-broker smoke, and then-deferred verdict
separate from both the supported API and release capacity evidence. Its old
prototype and reproduction commands were removed after ADR-0005 was accepted.

The machine-readable snapshots are
[`outbox-true-batch-small-frontier-20260903.json`](outbox-true-batch-small-frontier-20260903.json),
[`v0.2.2-clean-batch-matrix.json`](v0.2.2-clean-batch-matrix.json), and
[`v0.2.2-batch16-pool-ab.json`](v0.2.2-batch16-pool-ab.json). Each includes
every run's core metrics and SHA-256 of its source `report.json`.

## Reproduction contract

Start from a clean checkout and record the exact commit or immutable release
tag. The normalized one-cell controller searches a maximum sustainable rate by
geometric ladder and 50 msg/s bisection, then requires three fresh-volume
confirmations:

```sh
CAPACITY_FRONTIER_VARIANT=full-batch \
CAPACITY_FRONTIER_TOPOLOGY=o2-c2 \
CAPACITY_PAYLOAD_PROFILE=small \
CAPACITY_POSTGRES_PROFILE=stock \
make capacity-frontier
```

The full matrix first chooses among `o1-c1`, `o2-c1`, and `o2-c2`; then builds
separate `small` and `mixed` frontiers for `legacy`, `consumer-batch`,
`relay-batch`, and `full-batch`. Batch size `1` is a matched same-path control,
not a fifth frontier. Tuned PostgreSQL candidates are reported separately from
stock and keep `fsync`, `synchronous_commit`, and `full_page_writes` enabled:

```sh
CAPACITY_RUN_TUNED=0 make capacity-frontier-matrix
make capacity-frontier-matrix
```

For the first Outbox-only A/B, keep the batch consumer identical and compare
singleton Outbox ingress/relay (`consumer-batch`) with batch ingress/relay
(`full-batch`):

```sh
make capacity-outbox-batch-screen
```

The screen runs one `mixed`-payload cell per variant at 1,500 msg/s. Each cell
uses 30 seconds of warm-up, 60 seconds of measured load, and a 30-second drain
limit, so one variant normally completes in about two minutes and the pair in
about four to five minutes including the targeted batch-integration preflight
and Docker turnover. Override the initial rate or durations when needed:

```sh
CAPACITY_OUTBOX_SCREEN_RATE=1500 \
CAPACITY_OUTBOX_SCREEN_WARMUP=30s \
CAPACITY_OUTBOX_SCREEN_DURATION=60s \
make capacity-outbox-batch-screen
```

The screen may run from a dirty checkout because it is iteration evidence. It
records both dirty flags and commits under
`tmp/capacity/screens/<screen-id>/screen.json` and `screen.md`, labels the
result `development-screen`/`SCREEN_ONLY`. Both roles must reconcile with zero
pool replacements, cancelled acquires, unusable releases, or matching
connection-failure log lines. The full-batch candidate must also be sustainable
and average at least 10 Outbox messages per handler call; the singleton control
remains a baseline and may be unsustainable. The screen performs no repetitions
or frontier search and cannot support a `>=1.3x` claim.

The merge-decision proof fixes the normalized `o2-c2`/stock profile instead of
selecting a topology from the measured candidates. It builds `small` and
`mixed` frontiers for all four variants, then runs three interleaved repetitions
of every variant at 80% of the lowest frontier, rounded down to 50 msg/s:

```sh
make capacity-batch-proof
make capacity-batch-proof-verdict \
  PROOF_DIR=tmp/capacity/frontiers/<proof-id>
```

Both GoMessenger and the sibling Outbox checkout must be clean. Proof runs use
60 seconds of warm-up, 120 seconds of measured load, and a 60-second drain.
Before starting the first cell, the launcher runs Outbox `make check-all` and
GoMessenger `make check-workspace` plus `make test-batch-integration`; any
failed source or integration gate aborts the proof. The workspace gate resolves
the unreleased GoMessenger and Outbox contracts from the two local checkouts.
`make check` remains the separate `GOWORK=off` boundary and is not weakened or
used as a precondition for this checkout-local run.

Every manifest and verdict records `evidenceScope: "checkout-workspace"` and
the two clean checkout commits; verdict provenance also records the runtime
Outbox module as a local development replacement. The validator rejects a
different scope or a published Outbox module in this proof mode. Only
`release-readiness` plus the clean `test-consumer-release` probe can close the
later publication boundary.

`proof.json` and `proof.md` pass only when `relay-batch / consumer-batch` and
`full-batch / legacy` are at least 1.3x for both payloads. The matched-rate
series also rejects missing or non-zero connection-health failures, p95
regression above 10%, higher WAL/message,
transactions/message or claim execution time/message, actual batch averages
below 10, memory at 80% of a container limit, growing RSS, or three consecutive
`WALWrite`/`WALSync` samples. `CAPACITY_OUTBOX_BATCH_MAX_MESSAGES` and
`CAPACITY_CONSUMER_BATCH_MAX_MESSAGES` are separate axes in the frontier
runner; changing both is never required to isolate relay batching.

The heavy proof is intentionally local and opt-in. Hosted CI runs the
aggregator unit tests as part of the example module but does not start the
capacity matrix. Raw artifacts remain under `tmp/capacity/frontiers/<proof-id>/`;
after PASS, the launcher copies the compact Markdown verdict into this
directory as `<proof-id>.md` for review and commit.

One general frontier or matrix candidate run uses at least two minutes of
continuous precondition, waits for a completed checkpoint within five minutes,
measures for two minutes, and drains within 60 seconds. Sustainable means 3/3
with zero HTTP failures and
drops, at least 99% throughput, p95 at most two seconds, non-growing lags,
zero Outbox retry/defer/DLQ, zero broker redelivery/consumer DLQ, and exact
count/hash reconciliation. Live samples use stage-local application progress
rather than repeated full-table reconciliation; exact boundary SQL is retained
and runs in serial verifier sessions.

The following separate matrix reproduces the historical pool A/B topology:

```sh
test -z "$(git status --porcelain)"
git rev-parse HEAD

for pool in 9:1 8:2; do
  producer_connections="${pool%:*}"
  relay_connections="${pool#*:}"
  for rate in 1000 1500 2000; do
    for run in 1 2 3; do
      env \
        CAPACITY_RUN_ID="v0.2.2-performance-batch16-producer${producer_connections}-relay${relay_connections}-${rate}-r${run}" \
        CAPACITY_RATES="${rate}" \
        CAPACITY_MIN_RATE="${rate}" \
        OUTBOX_WORKERS=2 \
        OUTBOX_RESERVATION_BATCH_SIZE=16 \
        NATS_CONSUMER_CONCURRENCY=2 \
        OUTBOX_PRODUCER_MAX_CONNS="${producer_connections}" \
        OUTBOX_RELAY_MAX_CONNS="${relay_connections}" \
        DB_MAX_OPEN_CONNS=10 \
        make capacity-nats-site
    done
  done
done
```

The evidence bundle for a release should retain the complete run directories:
`report.json`, `report.md`, `environment.json`, `samples.jsonl`,
`resources.jsonl`, k6 summaries/logs, and compose logs. Archive the directories,
publish the archive with the release, and record its SHA-256 in the snapshot.

## Claim rules

A capacity statement must name the tested commit or tag, topology, payload,
measured-stage duration, rate, repetition count, and result. It must report
throughput, latency, backlog/lag, drain, integrity, dropped iterations, and
reconciliation together.

Do not:

- infer capacity from process exit code or post-drain reconciliation alone;
- describe a dirty run as release evidence;
- claim a rate when any clean repetition is unsustainable;
- claim a batch-size improvement without a matched control series;
- call exact test reconciliation an exactly-once delivery guarantee;
- present checkout-local Docker results as production readiness.
