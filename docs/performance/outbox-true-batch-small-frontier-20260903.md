# True Outbox batch small-payload frontier — 2026-09-03

## Conclusion

The matched `small`/`o2-c2`/stock PostgreSQL capacity series proves a 5.67x
repeatable frontier improvement for the complete true Outbox batch path. The
`consumer-batch` control, which keeps Outbox ingress and relay singleton while
holding the consumer in batch mode, sustained 450 msg/s in 3/3 fresh-volume
runs. The `full-batch` candidate, which batches Outbox ingress and relay while
keeping the same batch consumer, sustained 2,550 msg/s in 3/3 fresh-volume
runs.

The three 2,550 msg/s confirmations accepted and reconciled 918,200 business
effects. They had zero dropped k6 iterations, HTTP failures, redeliveries,
duplicate Inbox effects, invalid measurements, missing links, or DLQ messages.
Business p95 stayed between 135.938 and 137.530 ms, and drain completed between
1.018 and 1.664 seconds.

This is a complete, repeated capacity result for this exact matched profile. It
is not a claim that the service ran continuously at 2,550 msg/s for the full
benchmarking session. The operator-observed approximately eleven-hour campaign also included
source gates, image builds, multiple frontier candidates and controls, clean
Docker turnover, an invalid telemetry-sampling run, a harness correction, and
a partial restart. That wider campaign provides useful repeated and cold-start
evidence, but it is not an eleven-hour soak test.

## Matched frontier result

Both paths used the same source commits, small payload, fixed topology,
PostgreSQL profile, consumer batching, resource limits, 60-second warm-up,
120-second measured stage, 60-second drain limit, and fresh PostgreSQL/NATS
volumes for every run.

| Path | Outbox ingress / relay | Confirmed frontier | Confirmations | Business p95 | Relay throughput | Accepted effects | Dropped | Reconciliation |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `consumer-batch` control | single / single | 450 msg/s | 3/3 | 117.986–470.747 ms | 449.833–449.900 msg/s | 162,001 | 0 | 3/3 |
| `full-batch` candidate | batch / batch | 2,550 msg/s | 3/3 | 135.938–137.530 ms | 2,549.167–2,550.000 msg/s | 918,200 | 0 | 3/3 |

The confirmed-frontier ratio is `2550 / 450 = 5.67x`. This comparison isolates
the complete Outbox batching path more closely than `legacy` versus
`full-batch`: both rows above keep the downstream consumer in batch mode, while
the candidate changes producer staging and relay execution from singleton to
true batches.

The measured mechanism matches the configured path. At 2,550 msg/s the Outbox
batch handler, broker publish, and fenced finalization averaged 99.935–100
messages per call. At the 450 msg/s control frontier each of those operations
processed exactly one message per call.

## Boundary, not just a tested rate

The controller searched with a geometric ladder, narrowed the boundary in
50 msg/s steps, and then required three independent confirmations:

- the singleton Outbox control failed at 500 msg/s because business p95 reached
  2,908.149 ms, so 450 msg/s is its confirmed frontier;
- the full-batch candidate passed only 1/3 confirmations at 2,600 msg/s: one
  repetition exceeded the latency SLO and dropped two scheduled iterations,
  and another detected two producer-pool connection replacements;
- after reducing the candidate, 2,550 msg/s passed every sustainability and
  integrity condition in 3/3 repetitions.

Therefore 2,550 msg/s is the maximum repeatably demonstrated rate for this
profile, not merely the highest rate attempted once. The data do not support
rounding that result up to 2,600 msg/s.

## Provenance and topology

- Evidence scope: `checkout-workspace`.
- Capacity report specification: `2.1`.
- GoMessenger source: clean commit
  `dc33c969ee39ea387e98f091debe23492fc8cce2`.
- Outbox source: clean commit
  `e579abdc25804f1ae1439e8fcfa3fc42d727fb59`.
- Pipeline: batched HTTP business transaction and Outbox staging -> PostgreSQL
  -> batch relay -> NATS JetStream -> batch Inbox consumer -> PostgreSQL
  projection.
- Runtime: Go 1.27.1, PostgreSQL 18.6, NATS 2.12.3, Linux/arm64 containers on a
  Darwin/arm64 host.
- Fixed topology: two Outbox workers, two consumers, six producer connections,
  two relay connections, eight application DB connections, and maximum batch
  size 100 for both Outbox and consumer execution.
- Resource boundary: shared SUT cpuset `0-1`; PostgreSQL 1 GiB, NATS 512 MiB,
  API 512 MiB; swap disabled; JetStream file storage.

The compact machine-readable snapshot is
[`outbox-true-batch-small-frontier-20260903.json`](outbox-true-batch-small-frontier-20260903.json).
It records every claim-bearing run, core metrics, and SHA-256 of its source
`report.json`. Raw reports remain local under
`tmp/capacity/frontiers/outbox-batch-proof-20260903-r5/` and are intentionally
ignored by Git.

## Test boundary

The result proves a repeatable capacity improvement and exact post-drain
reconciliation for the recorded small-payload, single-node profile. The wider
eight-frontier `capacity-batch-proof` verdict did not complete: the first run
stopped later in the mixed-payload matrix after a transient telemetry sample
failed, and the corrected restart was stopped after the targeted evidence was
judged sufficient. Consequently this snapshot makes no mixed-payload frontier
or aggregate eight-frontier merge-decision verdict.

The campaign did not include these different test classes:

- a continuous multi-hour soak at 2,550 msg/s;
- restart, broker/database outage, network fault, or chaos injection while at
  the frontier;
- a multi-node deployment or production traffic distribution;
- the real-service pilot and its operational acceptance criteria.

Those missing test classes do not invalidate the measured 5.67x frontier
improvement. They bound where the result can be applied: checkout-local capacity
evidence, not a universal production-capacity guarantee.
