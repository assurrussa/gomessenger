# Kafka publication baseline: 2026-10-10

## Result and decision

The existing envelope batch API substantially amortizes Kafka transaction cost
in this bounded, single-node screen. All 30 cells passed exact read-committed
reconciliation. Keep the existing API; this evidence does not establish a
consumer need or reviewed error/atomicity contract for a new typed direct-batch
facade. No production implementation, dependency, or API changed.

The first typed-single cell on both runners was much slower than later cells.
There is strong run-order/broker-settling variation; the fixed warm-up does not
remove it. Consequently, do not infer typed-versus-envelope facade overhead,
precise general speedup ratios, or a Kafka-version performance ranking from
these results. Batch calls are also longer per call despite lower amortized
time per message.

## Provenance and execution

- Measured clean commit: `2ee7a2395c312a582ca5a447945ea0e6895c5595`.
- [Manual CI #156](https://github.com/assurrussa/gomessenger/actions/runs/38032694691),
  with `kafka_publish_compare=true`, `static_only=false`.
- Go 1.27.0, linux/amd64, Ubuntu 24.04 runner, kernel 6.17.0-1022-azure,
  four logical runner CPUs, `GOMAXPROCS=2`, Docker 28.0.4.
- Separate hosted runners for Kafka 4.1.2 and 4.3.1. Each measurement uses a fresh
  official single-node KRaft container capped at two CPUs and 2 GiB, swap
  disabled; the application is not CPU-pinned or memory-limited.
- One caller/transport and one partition per cell, replication factor/minimum
  ISR one, native envelopes, 256 repeated `x` payload bytes plus an
  eight-digit case. Three repetitions, middle mode order reversed.
- Each cell: 128 warm-up messages, 2,048 measured messages, one post-timing
  committed fence; exact read-committed reconciliation of all 2,177 records.
  Total: 61,440 measured and 3,870 warm-up/fence records, zero integrity failures.
- All 37,824 raw call durations, full environment text, image IDs/digests, and
  the two original archive SHA-256 values are retained in the
  [JSON snapshot](kafka-publish-20261010.json). Downloaded ZIP digests matched
  GitHub metadata. Every stored quantile, throughput, and amortization value was
  recomputed from raw timings and matched.
- Runtime validation passed: units/race/coverage/consumer/E2E, checkptr,
  PostgreSQL, and both transactional Kafka correctness/measurement jobs.
  CI #156's aggregate failed solely on a G703 warning for the explicitly
  host-selected report output file. Follow-up commit `421640b` adds only a
  line-scoped explanatory lint annotation, with no executable-code change;
  measurement provenance remains the original commit, not that later SHA.

## Closed-loop confirmed throughput

Values are messages/second. All repetitions are retained, including the slow
first cell. The median is per mode within one runner/version, not a pooled
cross-version estimate.

| Kafka | Mode | Repetition 1 | Repetition 2 | Repetition 3 | Median |
| --- | --- | ---: | ---: | ---: | ---: |
| 4.1.2 | typed-single | 69.4 | 733.7 | 730.8 | 730.8 |
| 4.1.2 | envelope-single | 452.7 | 558.4 | 679.5 | 558.4 |
| 4.1.2 | envelope-batch-1 | 489.8 | 571.1 | 814.9 | 571.1 |
| 4.1.2 | envelope-batch-16 | 6990.4 | 9237.2 | 10899.3 | 9237.2 |
| 4.1.2 | envelope-batch-64 | 21372.3 | 22608.9 | 27187.7 | 22608.9 |
| 4.3.1 | typed-single | 86.5 | 599.6 | 567.1 | 567.1 |
| 4.3.1 | envelope-single | 406.8 | 688.2 | 640.0 | 640.0 |
| 4.3.1 | envelope-batch-1 | 571.1 | 624.7 | 778.4 | 624.7 |
| 4.3.1 | envelope-batch-16 | 8571.2 | 8680.6 | 9328.6 | 8680.6 |
| 4.3.1 | envelope-batch-64 | 19742.9 | 24951.2 | 26444.7 | 24951.2 |

## Whole-call publish/commit completion

p95 is nearest rank for one cell. Values below are milliseconds per whole call,
including normal adapter work and the test wrapper. Batch-16 has 128 measured
calls per repetition; batch-64 has only 32, so its p95 is coarse. Other modes
have 2,048 calls. The last column is the median amortized producer service time
in microseconds/message; it is not message latency.

| Kafka | Mode | p95 repetition 1 | p95 repetition 2 | p95 repetition 3 | Median amortized µs/message |
| --- | --- | ---: | ---: | ---: | ---: |
| 4.1.2 | typed-single | 23.552 | 0.981 | 0.932 | 1368.25 |
| 4.1.2 | envelope-single | 21.551 | 1.341 | 1.056 | 1790.77 |
| 4.1.2 | envelope-batch-1 | 2.166 | 1.326 | 0.952 | 1750.80 |
| 4.1.2 | envelope-batch-16 | 2.448 | 1.928 | 1.639 | 108.24 |
| 4.1.2 | envelope-batch-64 | 3.803 | 3.836 | 2.866 | 44.22 |
| 4.3.1 | typed-single | 22.793 | 1.164 | 1.164 | 1763.12 |
| 4.3.1 | envelope-single | 21.623 | 1.154 | 1.073 | 1562.39 |
| 4.3.1 | envelope-batch-1 | 1.325 | 1.282 | 1.084 | 1600.65 |
| 4.3.1 | envelope-batch-16 | 2.389 | 2.198 | 1.882 | 115.19 |
| 4.3.1 | envelope-batch-64 | 4.471 | 3.040 | 3.261 | 40.07 |

Canonical measured envelope bytes were 1,294,085 in every 4.1.2 cell and
1,294,117 in every 4.3.1 cell. Independent fixtures differ slightly in timestamp
encoding length; comparisons are matched within each version, not pooled.
The full comparison tests completed in 139.48 s and 134.17 s respectively,
including untimed startup, warm-up, verification, and teardown. Batch-64's
actual measured windows were only 75–104 ms. These short windows and the
observed settling effect explicitly limit confidence in numerical ratios.

## Interpretation limits

The measured boundary is synchronous broker transaction commit, not bare
network RTT, consumer completion, or business effects. Throughput excludes
warm-up, setup, receipt validation, verification, and teardown, while including
the measured loop's timer/result bookkeeping. Every batch member still waits
for the whole commit; dividing p95 by batch size is invalid.

Already available full batches can share transaction overhead. This screen
does not measure how long a real application needs to fill a batch, contention,
multi-node replication, independent process repetitions, mixed or incompressible
payloads, faults, or a fixed offered rate. It establishes neither sustainable
capacity nor production readiness. The next API decision should be driven by
a real caller requiring typed direct batching; a matched open-loop workload and
agreed fill-latency/error semantics would be needed for that separate decision.

See the [reproduction and measurement contract](kafka-publish-comparison.md).
