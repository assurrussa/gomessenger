# Kafka single/batch publication screen

This bounded, opt-in experiment compares existing publication paths. It changes
no adapter behavior and introduces no typed direct-batch API. Kafka retains its
all-ISR, idempotent, transactional producer settings.

## Reproduce

From a clean checkout, with Go from `go.work` and Docker available:

```sh
KAFKA_TEST_MODE=publish-comparison make test-kafka
```

The existing launcher starts and removes official `apache/kafka:4.1.2` and
`apache/kafka:4.3.1` disposable containers sequentially. Each comparison broker
has two CPUs, 2 GiB memory, swap disabled, one KRaft node, and loopback networking.
The Go load process uses `GOMAXPROCS=2`; it is not CPU-pinned or memory-limited.
Each cell creates a fresh, single-partition topic with replication factor and
minimum ISR both one. There are no application consumers, Inbox, Outbox, broker
failures, TLS/SASL, external services, or production data in this screen.

The existing manual CI workflow accepts `kafka_publish_compare=true` with
`static_only=false`. Its two Kafka jobs first run their unchanged transactional
correctness tests under the race detector, then start fresh brokers for the
non-race measurement. No automatic CI trigger is added. Selecting static-only
does not run Kafka or constitute measurement evidence.

Reports go to ignored `tmp/kafka-publish-comparison/<run-id>/` by default;
`KAFKA_REPORT_DIR` selects another output directory. Each version writes its own
JSON report and environment record. CI uploads them as version-labelled
artifacts for seven days. Archive artifacts before expiry when using a result
as evidence. Compact per-cell summaries also remain in job logs.

## Matched experiment

Each of three repetitions runs these five modes, reversing the full mode order
on the middle repetition to reduce (not eliminate) order/warm-cache bias:

| Mode | API | Messages per transaction | Measured calls |
| --- | --- | ---: | ---: |
| typed-single | `Publisher.PublishMessage` | 1 | 2,048 |
| envelope-single | `Route.PublishEnvelope` | 1 | 2,048 |
| envelope-batch-1 | `Route.PublishEnvelopeBatch` | 1 | 2,048 |
| envelope-batch-16 | `Route.PublishEnvelopeBatch` | 16 | 128 |
| envelope-batch-64 | `Route.PublishEnvelopeBatch` | 64 | 32 |

Every cell receives the same 128 warm-up and 2,048 measured logical messages,
with identical pre-generated IDs, timestamps, keys, payloads, and canonical
envelope bytes. The synthetic JSON body has an eight-digit case and 256 repeated
`x` bytes. This is a small, compressible payload profile, not arbitrary traffic.
`envelopeBytes` records the exact total canonical measured bytes, excluding Kafka
record framing and compression. Compare only equal totals.

Metadata generation and input preparation are outside timing. The typed path
still performs its normal descriptor/metadata validation and payload/envelope
encoding during the call; the envelope paths begin with prepared bytes. The
typed mode deliberately uses explicit metadata, so it does not measure the
facade's automatic ID/time generation. All modes use one synchronous caller and
one transport; there is no concurrent admission contention or batch-fill wait.
All cells use a new producer and topic, and warm-up uses that cell's actual API
and transaction size. The process, broker, and filesystem caches remain shared
between cells. Results are within-run screening rather than independent-host
statistical estimates.

## Metrics and integrity

- `callNanos`, `p50CallNanos`, and `p95CallNanos` measure call entry through
  synchronous publication/transaction-commit completion, including adapter
  work and the small test wrapper. These are not bare network RTT, consumer
  latency, or end-to-end business latency. Percentiles use nearest rank; the
  batch-64 p95 has only 32 call samples per repetition and is a coarse estimate.
- `elapsedNanos` covers the complete measured closed-loop publish loop,
  including timer/result recording and immediate error checks, but excludes
  warm-up, setup, full receipt validation, reconciliation, and shutdown.
  `messagesPerSecond` is 2,048 broker-confirmed messages divided by this wall
  time. It is not a sustainable offered-rate capacity measurement.
- `publishNanosPerMessage` is the sum of measured call durations divided by
  2,048. It is amortized producer service time, not individual-message latency.
  In a batch, every member waits for the whole call. Never divide batch p95 by
  batch size and describe that quotient as per-message p95.
- Every receipt must match its input ID and be `broker_confirmed`; top-level
  or per-item failures fail the screen. The harness adds no retries.
- After timing, a final single-record committed fence closes the one-partition
  stream. A fresh `read_committed` reader verifies every warm-up, measured, and
  fence record in exact order, comparing complete canonical bytes (including
  IDs) and keys. Missing, duplicate, reordered, unexpected, or corrupt records
  fail. `reconciled` must be 2,177 for every cell. This is controlled prefix
  reconciliation, not an exactly-once delivery guarantee.
- Each cell has a three-minute publication/verification context, a separate
  30-second verifier bound, bounded startup/shutdown, and the complete Go test
  has a 12-minute timeout. Reports are checkpointed before the first cell and
  after each completed cell with `complete=false`; only all 15 successful cells
  set it true. Failure or timeout leaves an incomplete report (a process/storage
  failure can also interrupt the checkpoint write). Partial runs are not a PASS
  and must not be used as a complete comparison. Counts never adapt to speed.

Reports record the checkout commit, Go/platform/CPU information, Kafka version,
immutable image ID, and all raw per-call durations. The environment text also
records image repository digests, Docker version, and actual broker limits.
`TestKafkaPublishStatistics` checks percentile, amortization, throughput, and
raw sample preservation without a broker.

## Decision boundary

Compare `envelope-batch-1` with `envelope-single` to expose same-size API overhead,
then batch 16/64 with those controls to see transaction-cost amortization. Use
`typed-single` to keep the existing typed boundary visible. Report all three
repetitions and medians per Kafka version, alongside call latency and integrity.
Do not pool versions or select only the fastest repetition.

Higher closed-loop throughput alone does not establish that callers can fill
batches at their real offered rate or tolerate fill/commit latency. Choosing a
typed direct-batch API also requires an application use case and a reviewed
atomicity/error contract. No such API or production optimization follows
automatically from this screen. Multi-node replication, failure recovery,
contention, mixed payloads, open-loop latency, and production capacity remain
outside its scope. An unexecuted harness is preparation, not measured evidence.
