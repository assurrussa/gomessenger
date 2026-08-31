# Batch consumer experiment

## Historical verdict

**Deferred at the time of the experiment.** This is an immutable historical
screening report, not the current API status and not release evidence.

ADR-0005 has an unsupported, reproducible PostgreSQL/NATS prototype, live
broker-fault coverage, a complete PostgreSQL-only matrix, direct-NATS control
screening, report spec 1.2, and a fail-closed cross-run verdict tool. The full
two-hour candidate/common/mixed matrix was deliberately stopped after the
requested screening evidence had been collected; the aggregate verdict
therefore reports missing evidence instead of inferring success.

That experimental checkout added no public API. ADR-0005 was accepted later and
the supported implementation replaced the prototype. The old harness and its
commands were intentionally removed to avoid maintaining a second consumer
engine; the measurements below continue to describe only the recorded source
and topology.

## PostgreSQL-only matrix

The complete checkout-local PostgreSQL matrix ran 1,000 warm-up and 20,000
measured logical messages per cell, with batch sizes `1/16/64/100`, concurrency
`1/4`, and three alternating repetitions. All 480,000 measured messages
reconciled exactly across business effects, completed Inbox identities, and
attempt rows. Run `batch-experiment-20260901-evidence-postgres` records commit
`edb35174b4e4c20e7886622c06c2d387591faea7`, dirty state hash
`34d57a72bc406da135116f50bd583068ddf195770856e691db675ac54616b090`,
Darwin/arm64 with 15 logical CPUs, PostgreSQL 17.9, Go 1.27.0, and four database
connections.

| Concurrency | Batch | Median throughput | vs batch 1 | Median p95 batch |
|---:|---:|---:|---:|---:|
| 1 | 1 | 466.05 msg/s | 1.00x | 4.167 ms |
| 1 | 16 | 11,948.01 msg/s | 25.64x | 1.783 ms |
| 1 | 64 | 20,889.95 msg/s | 44.82x | 3.714 ms |
| 1 | 100 | 17,195.28 msg/s | 36.90x | 8.219 ms |
| 4 | 1 | 3,896.69 msg/s | 1.00x | 2.202 ms |
| 4 | 16 | 37,755.77 msg/s | 9.69x | 3.803 ms |
| 4 | 64 | 61,752.16 msg/s | 15.85x | 7.082 ms |
| 4 | 100 | 65,287.33 msg/s | 16.75x | 8.784 ms |

This clears the PostgreSQL-only 2x gate for every candidate in this checkout;
batch 16 is the smallest passing size. It does not decide promotion because
the direct JetStream 1.3x candidate, shared-rate latency, and steady-state RSS
gates remain unmeasured. Dirty checkout-local provenance also prevents using
it as release evidence.

This retained matrix predates report spec 1.2 and therefore has no per-cell WAL,
statement, I/O, or wait telemetry. Its throughput/reconciliation numbers remain
valid historical screening, but it cannot satisfy the current promotion gate.

## Direct JetStream screening

The control-only screening used batch size 1, 1 MiB `MaxBytes`, 10 ms
`MaxWait`, 30 s warm-up, 120 s measurement, and 30 s drain deadline. Every
completed cell has the same commit and dirty state hash as the PostgreSQL
matrix and ran against NATS 2.12.3. A rate is considered sustainable only when
all three repetitions complete and reconcile exactly.

| Concurrency | Rate | Sustainable repetitions | Median p95 of recorded repetitions | Interpretation |
|---:|---:|---:|---:|---|
| 1 | 500 msg/s | 3/3 | 0.912 ms | sustainable |
| 1 | 630 msg/s | 3/3 | 1.263 ms | sustainable |
| 1 | 690 msg/s | 3/3 | 1.344 ms | sustainable boundary |
| 1 | 750 msg/s | 2/3 | 1.496 ms | failing upper bound |
| 1 | 1,000 msg/s | 2/3 | 11.682 ms | not sustainable |
| 4 | 500 msg/s | 3/3 | 0.857 ms | sustainable; one 4.786 s p95 outlier |
| 4 | 1,000 msg/s | 3/3 | 65.357 ms | sustainable |
| 4 | 2,000 msg/s | 2/3 | 4.825 ms | not sustainable |

Concurrency 1 has a valid boundary bracket: 690 msg/s sustainable and 750
msg/s failing, within 10%. Concurrency 4 reached a stable 1,000 msg/s, but its
upper bracket was not refined to within 10%. The 1,500 msg/s run was interrupted
after its first warm-up failure and is retained only as a partial artifact.

The fail-closed aggregate verdict passes provenance and all six PostgreSQL
throughput gates. It remains `deferred` because the concurrency-4 boundary,
batch `16/64/100` candidate runs at the derived 900 msg/s target, common-rate
latency comparison, and mixed-profile RSS matrix are absent. These omissions
are intentional after stopping the long performance run; no promotion claim is
made from screening data.

## Archived reproduction boundary

The exact experimental source and runnable harness were deliberately removed
when the supported Inbox, NATS, and Kafka paths replaced them. These figures
can be audited against the recorded commit and artifact metadata, but the old
commands must not be presented as runnable against the current checkout. New
batch validation uses `make test-batch-integration` plus the normal repository,
PostgreSQL, and Kafka gates. Any new performance claim requires a new harness,
clean provenance, fixed resources, and a separately reviewed report contract.

## Artifacts and interpretation

Every run writes only beneath ignored `tmp/capacity/<run-id>/`:

- PostgreSQL-only `report.json` and `report.md`;
- direct NATS `nats-report.json`, including persisted failed/partial cases and
  exact measurement-window timestamps;
- `resources.jsonl` and `compose.log`;
- aggregate `verdict.json` and `verdict.md`;
- exact commit/dirty/Git-state hash, Go/PostgreSQL/NATS versions, limits, rate,
  payload, duration, throughput, p95, drain, reconciliation, and RSS window;
- per-cell `before/loadEnd/afterDrain` PostgreSQL statement, database, WAL and
  I/O boundaries; workload-scoped statement WAL bytes/message, MiB/s and
  records/message; cluster write/sync pressure; and bounded `WALWrite`/`WALSync`
  wait aggregates.

The direct harness marks a cell sustainable only when committed count equals
offered count, committed throughput reaches at least 98% of the target, no
unexpected delivery, duplicate, retry, or DLQ occurs, and drain stays within
the configured deadline. Promotion additionally requires the cross-cell 2x
PostgreSQL and 1.3x JetStream gates, 3/3 reconciliation, the latency rule, and
RSS below 80% of the equal 4 GiB limit without a rising steady-state trend. A
candidate also fails when median WAL bytes/message exceeds its matched control,
WAL write plus sync time reaches 80% of wall time, or `WALWrite`/`WALSync`
appears in three consecutive bounded samples.

The resource gate maps only the application container's samples into each
case's 120-second measurement window. It requires at least 30 samples, peak RSS
below 80% of the recorded 4 GiB limit, and last-third median growth no larger
than `max(32 MiB, 10% of first-third median)`. Cross-run provenance, medians,
paired p95, and resource gates are evaluated from retained artifacts; a process
exit code alone is never a passing verdict.
