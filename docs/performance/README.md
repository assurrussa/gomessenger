# Performance evidence

This directory keeps versioned, reviewable performance snapshots for GoMessenger.
It complements the raw capacity artifacts under `tmp/capacity/`; it does not
replace them or turn a checkout-local experiment into a production benchmark.

## Evidence levels

- `make bench-all` measures allocation-aware, synchronous local dispatch. It
  does not exercise PostgreSQL, Outbox, a broker, Inbox, or business effects.
- `make capacity-nats-site` measures the checkout-local durable path from HTTP
  acceptance through the committed PostgreSQL projection.
- A real-service pilot remains the production-readiness gate described by
  [ADR-0002](../decisions/0002-real-project-pilot.md).

Only compare runs with the same report specification, payload, topology,
software versions, stage duration, and host/container resources. Treat a
configuration as a measured capacity floor only when all three clean repetitions
are sustainable and pass post-drain reconciliation.

## Baselines

| Evidence | Status | Defensible result |
|---|---|---|
| [v0.2.2 clean batch matrix](v0.2.2-clean-batch-matrix.md) | Primary release-candidate, checkout-local | At exact clean commit `6836d1b3`, batch `100` completed the 1500 msg/s cell in 3/3 runs; batch `16` completed it in 1/3 |
| [v0.2.2 batch-16 pool A/B](v0.2.2-batch16-pool-ab.md) | Historical tuning baseline, checkout-local | Identified `8 producer + 2 relay` as the better pool split, but its batch-16 1500 msg/s result was not reproduced by the later clean interleaved matrix |

The machine-readable snapshots are
[`v0.2.2-clean-batch-matrix.json`](v0.2.2-clean-batch-matrix.json) and
[`v0.2.2-batch16-pool-ab.json`](v0.2.2-batch16-pool-ab.json). Each includes
every run's core metrics and SHA-256 of its source `report.json`.

## Reproduction contract

Start from a clean checkout and record the exact commit or immutable release
tag. The following matrix reproduces the historical pool A/B topology:

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
