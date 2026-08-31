# Implementation notes

## 2026-09-03 — PR #24 review fixes (batch consumer, lifecycle readiness, retry accounting, and workspace graph)

- [P1 - Makefile]: Defaulted `CHECK_GOWORK ?= $(abspath go.work)` so standard branch gates and CI use the workspace graph while nested modules import unpublished packages (`internal/batchruntime`), adding `check-published` (`GOWORK=off`) to verify the published graph separately.
- [P1 - batchruntime]: Context cancellation and timeouts now clear non-empty handler results and propagate context errors directly instead of wrapping in `ErrInvalidBatchResult`, preserving consumer rollback/retry semantics per ADR-0005.
- [P2 - outbox]: Restored `RetryAt` on single `RelayJob` (reserving `DeferAt` for batch relay) so single relay continues consuming attempts and respects `MaxAttempts`/DLQ progression per ADR-0006:73.
- [P2 - batchruntime]: Replaced plain boolean next-once guard with `atomic.Bool.CompareAndSwap(false, true)` in batch middleware invocation.
- [P1 - kafka batch rewind]: In `runKafkaBatchSession`, when all messages are deferred, `rewindKafkaBatch(session, batch, batch.firstDeferred)` now rewinds all polled partitions in `batch.all` before pausing the deferred partition.
- [P1 - kafka batch readiness]: Dynamic per-worker readiness tracking clears consumer readiness when batch workers encounter recoverable session failures and enter backoff, and re-establishes readiness once workers rejoin the consumer group.
- [P1 - nats batch readiness]: Worker pull loop readiness in `runNATSBatchConsumer` now requires explicit signals from inside established pull loops, blocks until all workers are established, and immediately propagates fatal startup errors without reporting ready.
- [P1 - attempt generation batch splitting]:
  - In `adapters/kafka/batch_consumer.go`: record selection stops and leaves subsequent records unprocessed if a record for the same `(Source, MessageID)` arrives with a differing attempt generation.
  - In `adapters/nats/batch_consumer.go`: `collectNATSBatch` calls `NakWithDelay` on messages for the same `(Source, MessageID)` with differing attempt generations, keeping them out of the active batch.
  - In `adapters/inbox/pgsql` and `adapters/inbox/sqlite`: `ProcessBatchAttempt` partitions items into sub-batches by attempt generation, executing them sequentially without returning `ErrInvalidBatchResult` or failing closed.
- Verified with `make check-workspace` (all modules build, vet, lint with 0 issues, race, checkptr, coverage, consumer, and E2E) and `make test-batch-integration`.

## 2026-09-03 — published Outbox v0.13.0 integration and workspace gate verification

- Pinned published Outbox `v0.13.0` across `adapters/outbox`, `testdata/consumer`,
  `testdata/e2e`, and `examples/durable-postgres-nats`, including `backends/pgsql v0.13.0`
  and `backends/sqlite v0.13.0`.
- Verified that clean consumer and E2E modules resolve and pass tests under `GOWORK=off`
  against published Outbox `v0.13.0`.
- Confirmed that `make check-workspace` passes completely with zero lint issues, all unit,
  race, and checkptr tests, and 91.1% root statement coverage.
- Restored development `replace` directives in nested module `go.mod` files (as documented
  in `docs/release.md`), allowing `make`, `make prepare`, and `make check` to run cleanly
  during feature development before the release-wave `make release-ready` drops them.
- Confirmed `make`, `make prepare`, `make check`, and `make check-workspace` all pass cleanly.

## 2026-09-02 — bounded Outbox claims and connection-health evidence

- Adopted the checkout-local Outbox byte-bounded claim capability in the
  observed PostgreSQL repository wrapper, preserving claim-to-handler stage
  attribution while reducing a true batch to one claim round trip.
- Replaced `PrepareConn`/`AfterRelease` accounting with the balanced pgxpool
  acquire/release tracer. Capacity report spec `2.1` records pool expansion,
  replacement connections, cancelled acquires, unusable releases, and the
  acquired-connection high-water mark for producer and relay pools. Any pool
  churn makes a stage unsustainable and the proof verifier rejects missing
  connection-health data.
- The short Outbox screen now defaults to 1,500 msg/s, counts PostgreSQL
  cancellation and broken/reset/lost-connection signatures in `compose.log`,
  requires zero log and pool churn for both roles, and requires the candidate
  to be sustainable with an average Outbox batch of at least 10. The control
  remains a baseline and may be unsustainable; the result remains
  `SCREEN_ONLY` and cannot support a `>=1.3x` claim.
- Frontier and proof launchers now compact overlong generated capacity run IDs
  to 64 characters with a stable hash suffix. The human-facing proof/frontier
  IDs and paths remain unchanged while every container run satisfies the
  capacity service's fail-closed ID bound.

## 2026-09-02 — fail-closed true-batch capacity proof

- Added a fixed PostgreSQL 18/NATS `o2-c2` proof workflow for `small` and
  `mixed` payloads. It compares all four pipeline variants, requires three
  fresh-volume frontier confirmations, and follows with three interleaved
  matched common-rate repetitions.
- Split Outbox and consumer maximum batch sizes in the frontier launcher so the
  isolated `consumer-batch` to `relay-batch` comparison changes only the relay
  path. One reusable cell launcher now owns variant, topology, and PostgreSQL
  profile wiring.
- Added a tested Go verdict aggregator. It rejects incomplete or dirty evidence,
  provenance/configuration drift, less than 1.3x isolated or end-to-end
  frontier improvement, missing actual batches, p95/PostgreSQL cost regression,
  memory pressure or growth, and repeated WAL waits. It always writes compact
  `proof.json` and `proof.md` artifacts before returning a failed verdict.
- The multi-hour capacity workflow remains opt-in and local. Existing hosted
  tests compile and exercise the aggregator without starting Docker capacity
  runs.
- The launcher owns the required pre-measurement gates: Outbox `make check-all`
  and GoMessenger `make check-workspace` plus `make test-batch-integration`.
  The proof manifest and both verdict formats explicitly identify the result as
  `checkout-workspace` evidence and retain both clean checkout commits.
- `make check-workspace` passed with zero lint findings, all module/race/checkptr
  tests, 91.1% root coverage, and the durable E2E. The unchanged `make check`
  still stops at the expected unpublished `internal/batchruntime` boundary.
- Added a short Outbox-only development screen at a default 1,500 msg/s: one
  30-second warm-up plus 60-second measured cell for singleton ingress/relay
  and one for full-batch ingress/relay, with the consumer fixed in batch mode.
  It permits dirty checkout iteration but records both commit/dirty identities,
  verifies exact runtime modes, reconciliation, and an exercised candidate
  batch, and emits only `SCREEN_ONLY` artifacts rather than a performance verdict.

## 2026-09-01 — transactional producer/relay batches and capacity frontier

- Added the transactional-Outbox-only `BatchRoute` facade with typed command
  and event batch methods, bound DI interfaces, full metadata validation,
  duplicate-ID rejection, one atomic unique staging call, and receipts in input
  order. Existing local, NATS, Kafka, and Outbox single routes retain their
  prior behavior; unsupported direct batch calls fail explicitly.
- Added Outbox true-batch registration and execution. One homogeneous ordered
  batch has count/payload/wait limits, one handler call, an exact keyed result,
  top-level no-attempt backoff, capability-wide defer, one fenced atomic outcome
  transaction, heartbeat coverage, and bounded drain-tail release. PostgreSQL,
  MySQL, and SQLite implement atomic staging/finalization; Picodata supports the
  singleton control and rejects a larger batch.
- Added batch relay composition. NATS uses bounded async publication with one
  deterministic message ID and PubAck future per item. Kafka publishes the
  valid subset in one multi-record transaction and retries the subset after an
  abort or commit failure. Retry delays from brokers map to no-attempt Outbox
  defer.
- Reworked the durable capacity demo to generate message IDs in the initial
  business insert, bulk-stage up to 100 orders in one transaction, batch-insert
  successful projections, and persist envelope measurements through a bounded
  asynchronous UNLOGGED recorder whose failure invalidates the run without
  changing delivery outcomes.
- Capacity report spec 2.1 records runtime-confirmed ingress/relay/consumer
  modes, actual handler/publish/finalization calls and batch sizes, outcomes,
  normalized SQL/transaction/WAL/checkpoint costs, resource and image
  provenance, pprof, and PostgreSQL `EXPLAIN (ANALYZE, BUFFERS, WAL)` evidence.
  Frontier scripts compare the three fixed 2-vCPU/2-GiB topologies, four
  pipeline variants, two payload profiles, and separately labelled stock/tuned
  PostgreSQL 18 runs using ladder, bisection, and three fresh-volume
  confirmations.
- A five-second full-batch smoke exercised one producer, relay, and consumer
  invocation per five messages and reconciled 105 business effects with no
  retries, redeliveries, or DLQ. It is plumbing evidence only, not a frontier
  or production-capacity claim. No commit, push, tag, release, or deployment was
  performed.
- Fixed the first normalized frontier run's measurement failure at 1,750 msg/s.
  Live samples now use stage-local accepted, durable relay-success, and
  committed-consumer counters instead of rescanning all business tables every
  second. Exact load-window and post-drain SQL remains authoritative, while all
  controller sessions set `max_parallel_workers_per_gather=0` so verifier
  queries cannot exhaust Docker shared memory or occupy both SUT CPUs. Failed
  incomplete reports now set `integrityPassed=false`; a completed exact run
  that only misses an explicit minimum-rate gate preserves its integrity result.
- A fresh-volume O1/C1, stock PostgreSQL 18, mixed-payload, full-batch diagnostic
  repeated the formerly invalid 1,750 msg/s cell successfully: 210,100 accepted
  and exactly reconciled messages, 1,748.33 relay/consumer msg/s, 227.22 ms
  business p95, 0.846 s drain, zero drops/retries/redeliveries/DLQ, and batch
  size 100 throughout. The growing business snapshot fell from 121 calls and
  12.6 s aggregate execution in the failed warm-up to boundary-only queries;
  the exact final integrity query completed serially. This is one dirty-checkout
  diagnostic proving the harness fix, not the required clean 3/3 frontier claim.

## 2026-09-01 — capacity single versus true consumer-batch A/B

- Added an explicit capacity-demo selector: `CONSUMER_MODE=single` preserves
  `NewEventConsumer`; `CONSUMER_MODE=batch` uses `NewBatchEventConsumer` with
  configurable message, canonical-byte, and wait limits. No automatic handler
  adapter or implicit migration was introduced.
- `batch + MaxMessages=1` is the same-path control and `batch + 100` is the
  real-batch candidate. Outbox reservation width remains an independent axis.
- The batch handler classifies every item before executing projection SQL and
  writes only the successful subset in the shared Inbox/business transaction.
  The observing Inbox wrapper now forwards the optional batch capability.
- Capacity report spec `1.4` records consumer mode and limits. The controller
  verifies `/benchmark/stats` runtime configuration against its own environment
  before starting load, preventing mislabeled A/B artifacts. Stage reports also
  record actual batch-handler calls, average/maximum batch size, and handler
  duration so configured and exercised batching are independently visible.
- Three isolated 50 msg/s route-smokes reconciled exactly. Legacy `single`
  reported zero batch calls; `batch + MaxMessages=1` handled 150 messages in
  150 calls; `batch + MaxMessages=100` handled 151 messages in 29 calls with
  average 5.21 and maximum 6. The two-second warm-up and three-second measured
  window are deliberately too short for a capacity claim; these runs prove
  routing, runtime labeling, actual batching, and reconciliation only.
- Targeted demo/capacity tests, their race variants, example-module lint, and
  all-module tests through the local workspace passed. The maximal local gate
  is now `make check-workspace`; it resolves the unreleased root, Inbox, and
  Outbox contracts through `go.work` and is the proof launcher's source
  precondition. The original `make check` remains `GOWORK=off` and correctly
  stops at the unpublished module boundary. An exact compatible Outbox release
  and dependency-ordered GoMessenger tags are still required before that gate
  and the clean release-consumer probe can establish published compatibility.

## 2026-09-01 — Supported end-to-end batch consumers

- Accepted ADR-0005 and added a separate consumer-only batch API. Existing
  single-message handlers, constructors, producers, Outbox staging, and relay
  behavior remain source-compatible.
- The root contract now includes exact keyed results, batch-only middleware,
  zero-value batch bounds, `DeferAfter`, and bounded batch observations.
- Inbox exposes an optional `BatchAttemptBackend` without expanding the older
  backend interfaces. PostgreSQL uses deterministic locks plus set-based
  `unnest`; SQLite uses one serializable transaction with bounded multi-row
  statements. Both implement coalescing, completed/terminal prefiltering,
  individual attempts, partial outcomes, and full rollback for top-level
  failures. Singleton `ProcessAttempt` now also preserves attempts on defer.
- NATS batches commands and native/structured/binary events by count, canonical
  bytes, or wait. One heartbeat owns a batch, drain flushes partial work, ACK
  confirmation is bounded to 16 parallel operations, and deterministic
  publish-confirmed DLQ handoff remains per slot.
- Kafka batches only an ascending contiguous range from one topic-partition.
  Other polled records are rewound, rebalances remain blocked through the
  outcome boundary, and one Kafka transaction publishes retry/DLQ records with
  the processed offset. Top-level failures rewind without consuming attempts;
  unusable sessions are recreated by the worker supervisor.
- Added root, Inbox, NATS, Kafka, clean-consumer, and full
  Outbox-to-NATS-to-SQLite batch coverage, including a batch-1 versus real-batch
  invocation control. The supported gate is `make test-batch-integration`.
- Removed the checkout-local `batchexperiment` implementation and its capacity
  runner so there is one maintained engine. The old performance report remains
  explicitly historical and its earlier throughput/WAL/RSS thresholds are not
  an API completion gate.

## 2026-08-31 — Experimental NATS/PostgreSQL batch consumer

Superseded by the supported implementation above. This section records the
prototype work as history.

- Started the Proposed ADR-0005 experiment without changing the public facade,
  `HandlerConfig`, supported adapters, migrations, release tags, or hosted CI.
- The implementation lives only under the durable example's `internal` tree
  and tests one native `orders.created` descriptor. It uses the existing
  namespaced PostgreSQL Inbox tables with a distinct consumer identity.
- The key correctness boundary is explicit: successful members may commit in
  one transaction, but the handler must leave no business writes for failed or
  deferred members. Batch-wide failures roll back the handler savepoint.
- The JetStream collector uses bounded pull requests rather than iterator
  prefetch: one message starts `MaxWait`, then the remaining request carries
  both message and byte limits. One heartbeat loop is owned by each active
  batch slot.
- Inbox identity insert, deterministic lock/read, attempt increments, completed
  markers, and terminal markers are set-based PostgreSQL statements. Handler
  input is restored to broker order after the sorted lock phase; the benchmark
  handler uses one `INSERT ... SELECT FROM unnest` for its successful subset.
- Added opt-in `make test-batch-experiment`, `make capacity-batch-postgres`, and
  `make capacity-batch-nats` paths. `make capacity-batch-experiment` now runs
  the complete resumable control/candidate/mixed workflow, while
  `make capacity-batch-verdict` re-evaluates retained evidence. Their Docker
  stacks exclude Outbox and keep raw provenance, reports, Compose logs,
  resource samples, and aggregate verdicts only under ignored
  `tmp/capacity/<run-id>`.
- Live PostgreSQL/NATS correctness covers exact and middleware result
  validation, broker-order restoration, lost-ACK duplicate suppression,
  interrupted-DLQ terminal replay, identity conflicts, mixed
  ACK/retry/defer/DLQ, batch-wide rollback, attempt exhaustion, all three
  collector limits, singleton oversize dispatch, startup failure, partial
  drain, forced-cancellation rollback/redelivery, heartbeat, concurrency
  bounds, and trace links. The isolated Compose gate passes. Its canonical-byte
  test also exposed and fixed legacy NATS `FetchBatch` returning the normal
  `MaxBytes` boundary as an immediate error. The live Docker correctness binary
  is built and run with the race detector.
- Added an internal broker-finalization seam used only by live tests. The gate
  now loses the first ACK and observes real broker redelivery with one handler
  call, interrupts source ACK after confirmed DLQ publication and verifies
  deterministic JetStream deduplication after restart, and blocks one DLQ slot
  while the second slot commits. Atomic active-worker and active-heartbeat
  gauges are asserted at zero after normal and forced shutdown.
- Direct NATS artifact spec 1.2 persists every case, including partial failure,
  with exact measurement timestamps, ACK/handler reconciliation, and explicit
  sustainability reasons. PostgreSQL and NATS reports include a checkout-state
  SHA-256 covering tracked diff plus untracked source. The verdict aggregator
  rejects provenance drift, incomplete repetitions, missing boundary brackets,
  latency/RSS gaps, and duplicate case evidence instead of inferring success.
- A full-matrix rehearsal exposed that an expected overloaded control case at
  the candidate target stopped the process before later batch sizes ran. The
  NATS runner now persists that failed case, continues every remaining matrix
  cell and repetition, then returns the joined case errors only after the
  report is complete. A deterministic unit test fixes the continuation and
  alternating-order contract.
- The first complete direct-NATS matrix then correctly failed closed on its
  evidence harness: ambient `CAPACITY_GIT_*` values labeled NATS reports with
  an earlier checkout, and the verdict case key treated required
  screening/candidate/common repetitions as duplicates. Batch launchers now
  overwrite provenance from the actual checkout and host, reports carry an
  explicit evidence phase, and every gate selects only its phase. A poisoned
  ambient-environment live probe proved the recorded commit, dirty-state hash,
  host, and phase match the checkout rather than the caller environment.
- The final checkout-local PostgreSQL matrix completed 24/24 exactly reconciled
  cells with one provenance tuple. Median throughput for batch 16 was 25.64x
  the batch-1 control at concurrency 1 and 9.69x at concurrency 4; batch 16 is
  the smallest size that clears the PostgreSQL 2x gate.
- Direct-NATS control screening completed the concurrency-1 bracket at 690
  msg/s sustainable 3/3 and 750 msg/s failing 1/3. Concurrency 4 was sustainable
  3/3 at 1,000 msg/s, but its upper bracket was not refined to 10%. At the
  user's request the remaining long candidate/common/mixed run was stopped;
  its raw partial artifacts remain ignored under `tmp/capacity`, and the
  aggregator correctly reports `deferred` with the missing evidence listed.
  ADR-0005 stays Proposed and the public API remains absent.
- Closed the batch measurement gap by reusing the existing `pgtelemetry`
  snapshotter through a separate one-connection probe pool. Both PostgreSQL-only
  and direct-NATS cells now record `before/loadEnd/afterDrain` statement,
  database, WAL and I/O deltas; workload-scoped statement WAL bytes/message,
  MiB/s and records/message; cluster write/sync cost; and bounded aggregated
  wait samples. This split avoids the publication lag of global `pg_stat_wal`
  counters on short cells while keeping global WAL pressure visible.
  PostgreSQL-only sampling uses 100 ms and direct-NATS load/drain sampling uses
  one second. The verdict fails closed on missing spec-1.2 telemetry, WAL
  amplification, write+sync wall fraction at or above 80%, or three consecutive
  `WALWrite`/`WALSync` samples. Direct-NATS drain duration now covers the whole
  post-load catch-up plus consumer shutdown instead of shutdown alone.
- Live PostgreSQL 17 smoke cells verified report spec 1.2 and non-zero
  workload-scoped WAL: 32 messages produced 1,462 B/message for batch 1 and
  1,411 B/message for batch 16 with exact reconciliation. A direct JetStream
  smoke at 100 msg/s reconciled 120/120 for batch 1 and batch 4, recorded all
  three telemetry boundaries, load/drain wait samples, and non-zero WAL and
  write/sync metrics. These tiny runs validate instrumentation only and are not
  performance evidence.

## 2026-08-29 — v0.2.2 release preparation

- Published the complete synchronized `v0.2.2` module graph and GitHub Release.
  The clean external consumer downloaded every module and installed
  `gomessengerctl@v0.2.2` without local replacements, then passed its compile
  and test probe. The public release keeps the checkout-local batch-100 result
  separate from production readiness; the raw archive was verified locally but
  not uploaded because it contains environment, resource, and Compose logs.
- Published the reviewed root `v0.2.2` tag at merge commit `757a995` after the
  local and hosted source gates passed. Nested modules are promoted in reviewed
  dependency layers so every exact `GOWORK=off` requirement resolves before
  its dependent module is tagged; the previous one-shot preparation attempted
  to pin transports before Inbox existed and could not pass the clean build.
- Final-layer preparation now fails before mutation unless root, Inbox, NATS,
  and Kafka resolve at the requested version. It tidies the Outbox adapter and
  CLI against those published modules instead of a temporary root replacement,
  preserving the exact published checksums in their release graphs.
- Prepared the root `v0.2.2` release while retaining the published nested
  GoMessenger graph at `v0.2.1` and the Outbox root, PostgreSQL, and SQLite
  modules at `v0.12.0`. The release changes no public Go API and keeps the site
  capacity defaults at reservation batch `1` and consumer concurrency `1`.
- Added the evidence-scoped changelog entry for the PostgreSQL Inbox
  fresh-success optimization, the checkout-local capacity harness, report spec
  `1.3`, and the supervised publication recorder. It deliberately makes no
  sustainable `2000 msg/s` or production-readiness claim.
- `make prepare`, `make check`, `make test-integration`, the PostgreSQL 18
  integration gate, the official Kafka 4.1.2/4.3.1 compatibility gate, and
  `make bench-all` passed. A local `release-ready VERSION=v0.2.2
  OUTBOX_VERSION=v0.12.0` probe and the matching `release-readiness` gate
  verified the intended exact-version graph, but PR CI correctly rejected that
  graph before the root tag existed. The committed release therefore follows
  the real dependency order: root first, then nested module pins and tags only
  after each dependency resolves through the Go proxy.

## 2026-08-29 — Published Outbox v0.12.0 integration

- Pinned the Outbox adapter, durable PostgreSQL example, clean consumer, and
  SQLite E2E module to the published Outbox root and matching backend
  `v0.12.0` tags. Local joint-development replacements remain confined to
  `go.work`; source and release gates use `GOWORK=off`.
- The durable example now resolves the same unified version-aware fenced batch
  contract used during capacity development without local Outbox replacements.
  Reservation batch size remains a host option in `1..1000`, with the site
  default still `1` because the confirmation A/B did not establish a higher
  repeatable capacity floor.
- `release-ready VERSION=v0.2.1 OUTBOX_VERSION=v0.12.0` resolved the published
  root, PostgreSQL, and SQLite tags and refreshed the four affected module
  graphs. Repository-wide verification follows on that published graph.
- Review follow-up made the publication recorder a required supervised runtime,
  evicts confirmations after a successful flush, and keeps an in-flight batch
  retryable without recording normal runner cancellation as a telemetry
  failure. Flush ownership is context-aware so a final flush cannot outlive an
  already-expired shutdown deadline. Deterministic tests cover both shutdown
  boundaries.
- Relay pool high-water measurement now uses `pgxpool` prepare/release hooks at
  the actual acquisition boundary instead of relying on one-second HTTP
  snapshots. Producer and relay pools are both constructed from the parsed
  `pgxpool.Config`, so adding their distinct `application_name` values preserves
  URL, keyword/value, and Unix-socket PostgreSQL connection strings. A short
  PostgreSQL 17 + NATS smoke with pools `8 + 2` completed exact reconciliation,
  drained successfully, and recorded relay `maxAcquiredConnections=2`; its
  deliberately short stage is connectivity/measurement evidence, not a
  capacity result.
- The published-graph branch passes `make check` with zero lint findings, 91.0%
  root coverage, race/checkptr, clean consumer, and durable E2E coverage. The
  real capacity smoke also proved that the observed relay pool preserves UUID
  scanning, fenced claims, PubAck, and drain behavior.
- Release preparation now tidies the Outbox adapter through a temporary local
  root replacement and removes it before returning, so it does not try to
  resolve an unpublished GoMessenger root tag. An isolated `v0.2.2` pre-tag
  probe passed both `release-ready` and `release-readiness`; the prepared
  adapter required `v0.2.2` with no remaining module replacement.

## 2026-08-28 — Capacity publication-recorder batch boundary

- Fixed the asynchronous broker-confirmation recorder so a size-trigger writes
  only complete batches. Confirmations that arrive while that SQL is running
  remain queued for the next complete batch or 50 ms interval instead of being
  chased immediately as a stream of small `UPDATE ... FROM unnest(...)`
  statements. Interval and final flushes persist a snapshot of the pending
  tail in bounded batches.
- Added deterministic concurrency coverage that blocks the first batch write,
  records a partial tail during that write, and proves the size-trigger leaves
  the tail pending until an explicit flush. The capacity report contract and
  production relay behavior are unchanged; this narrows observer interference
  before repeating the 2,000 msg/s batch-16/consumer-2 profile.
- The first 60-second AC screening repetition after the fix used Outbox workers
  `2`, reservation batch `16`, consumer concurrency `2`, and pools `9 + 1`.
  Relay/consumer throughput reached `1998.583/1998.383 msg/s`, Outbox/consumer
  lag ended at `85/12`, business p95 was `1275.932 ms`, drain was `0.625 s`,
  and reconciliation passed without drops, redelivery, or DLQ. Publication
  measurement writes fell from `5,180` calls at `22.3` rows/call and `17.155 s`
  total execution in the preceding failed C2 run to `1,196` calls at `100.2`
  rows/call and `0.546 s`. This is checkout-local screening evidence, not the
  required three published-Outbox confirmation repetitions.
- Recorder unit tests, targeted demo/capacity tests, the recorder race test,
  targeted `go vet`, and new-diff `golangci-lint` pass. The patched isolated
  capacity checkout also passes its `GOWORK=off` recorder tests.

## 2026-08-28 — Queue snapshot and capacity pipeline boundaries

- Replaced the capacity-only `/benchmark/stats` Outbox age query with the
  single `QueueStats` snapshot supplied by Outbox. The response uses the
  snapshot `ObservedAt`, derives the global oldest-ready timestamp from the
  minimum non-zero capability timestamp, returns `null` when no ready job
  exists, and exposes every exact `(name, schemaVersion)` backlog with a
  process-local `supported` flag. `stats.go` no longer imports `database/sql`
  or executes a `SELECT MIN(created_at)` query.
- Raised the capacity artifact contract to spec `1.3`. Relay throughput is the
  published delta divided by the load window; consumer throughput and MiB/s
  use committed projections; Outbox and consumer lag are reported separately
  as `staged - published` and `published - committed`. JSON, Markdown,
  structured logs, and sustainability diagnostics no longer use the ambiguous
  committed-only `effective*` fields. Drain remains outside all denominators.
- Capacity environment JSON and Markdown now record the Outbox module version
  from Go build metadata. A published build reports its exact tag; a workspace
  path replacement reports `devel (local replace)` instead of pretending that
  the required tag supplied the binary.
- Release preparation/readiness now updates, tidies, and validates the durable
  example's matching Outbox root plus PostgreSQL requirements in addition to
  the adapter, clean consumer, and SQLite E2E requirements.
- Targeted `go test ./internal/demo ./internal/capacity` passes against the
  workspace Outbox candidate. As of this check, the Go proxy still exposes
  only `v0.11.0` for Outbox root, PostgreSQL, and SQLite; therefore no
  `v0.12.0` pins, `GOWORK=off` capacity runs, default changes, final evidence
  rewrite, shared-wiki verification date, or repository-wide `make check` are
  claimed yet.

## 2026-08-28 — Outbox reservation-batch capacity control

- Added `OUTBOX_RESERVATION_BATCH_SIZE` to the durable PostgreSQL/NATS service
  and capacity runner. The accepted range is `1..1000`; correctness, quick,
  full, and site defaults remain `1` until repeatable capacity evidence selects
  a higher minimum.
- The split Outbox runtime forwards the value to
  `outbox.WithReservationBatchSize`. Workers remain the handler-concurrency
  boundary; a batch changes only reservation/prefetch and keeps PubAck,
  conditional delete, retry, and DLQ per-job.
- Capacity artifact spec `1.2` recorded the exact value in JSON and Markdown so
  batch runs cannot be compared as if they had the same topology.
- The A/B held site topology at workers `2`, producer/relay pools `9 + 1`, and
  consumer `1`. Short screening ran `1/16/32/64/100` for 30 seconds at
  `500/650`; every size passed exact integrity with zero redelivery/DLQ. At
  650, business p95 was 93.459-99.604 ms, maximum Outbox backlog was 61-68,
  and drain was 0.369-0.432 seconds. The two lowest-p95 candidates, `64` and
  `100`, advanced to confirmation.
- Three two-minute confirmation repetitions used the scheduled
  `500/650/800/1000` ladder and stopped each run at its first unsustainable
  stage. The control passed 500 in all three and failed 650 in all three:
  business p95 rose to 6.371-8.633 seconds and backlog to 5,609-7,189 while
  throughput remained approximately 649.6 msg/s. `64` passed 500 once; its
  other two repetitions dropped 2 and 246 scheduled iterations at 500, and
  its first repetition dropped 7 at 650. `100` reached maxima of 500, 650,
  and 500; its best repetition passed 650 at 649.967 msg/s, 120.933 ms p95,
  backlog 189, and 0.604-second drain, then dropped 8 iterations at 800.
- Every screening and confirmation stage reconciled exactly with zero broker
  redelivery and zero DLQ. Neither candidate sustained a higher stage than the
  control in all three repetitions, so the site default remains the minimum
  `1`; larger batches remain an explicit opt-in rather than a capacity claim.
  The durable reports are under ignored `tmp/capacity/batch-screen-*` and
  `tmp/capacity/batch-confirm-*`.
- Workspace-aware targeted Go tests passed against the local Outbox checkout.
  The repository-wide `make check` intentionally uses `GOWORK=off` and still
  resolves published Outbox `v0.11.0`, so it stops at the new option until a
  later Outbox release is published and pinned. The capacity images therefore
  used an isolated temporary source copy with local module replacements; the
  checked-in module graph was not rewritten, and no tag or publication was
  performed.

## 2026-08-28 — Producer to Outbox relay pre-batch optimization

- Removed the per-message `UPDATE demo.envelope_measurements` from the relay
  success path. A successful JetStream `PubAck` now records the actual
  publication instant in memory; a recorder deduplicates message IDs and uses
  one `UPDATE ... FROM unnest(...)` for up to 256 confirmations or every 50 ms.
  Recorder failures do not retry or DLQ the delivered message, but they make the
  benchmark unhealthy and fail final integrity. Shutdown stops the relay before
  a bounded recorder flush, and load-window counts are rebuilt from the flushed
  timestamps after drain.
- The three PostgreSQL 17.9 recorder-only controls retained one shared Outbox
  pool. All passed reconciliation without redelivery or DLQ; 250 msg/s was
  sustainable in all three runs, while 325 msg/s was sustainable in one of
  three. The two failing 325 stages reached maximum Outbox backlogs of 1,142 and
  2,486; the successful run reached 636. This control therefore remained too
  variable to establish a higher capacity floor.
- Split the example into host-owned producer and relay pgx pools with distinct
  `application_name` values. Site defaults are producer `min=1/max=9` and relay
  `min=1/max=1`; the controller rejects a site profile whose pgx maxima do not
  sum to ten. `StageOrder` begins the business transaction on the producer pool
  while the relay-bound repository stages through the `pgx.Tx` carried in
  context, preserving atomic business-row plus Outbox-job commit/rollback.
  Capacity telemetry reports both pool sizes, acquire counts/durations, empty
  acquires, and observed maximum acquisition separately.
- In the three comparable 9+1 candidate repetitions, 250 msg/s was sustainable
  in all three and 325 msg/s in two of three. At 325, effective throughput was
  324.758-324.825 msg/s, no iteration was dropped, and every run passed exact
  integrity with zero redelivery and DLQ. The failing repetition reached a
  1,139-job maximum Outbox backlog and 2,866 ms business p95; the two passing
  repetitions reached 249/51 jobs and 217/104 ms. The second repetition was
  intentionally stopped after its completed 325 stage when the default matrix
  began an out-of-scope 350 stage; its completed 250/325 results remain intact.
- Producer saturation reached the configured nine connections in every
  candidate run, while relay acquisition remained bounded to its guaranteed
  connection. Through the 325 boundary, cumulative relay acquire duration was
  0.171-0.321 seconds versus approximately 2.505 seconds for the shared-pool
  control, and the complete pgx budget remained ten. This proves the intended
  fairness/isolation boundary, but the remaining 325 variability means no new
  capacity floor is claimed and batch reservation remains out of scope.
- A direct clean-checkout verification then sustained both 250 and 325 msg/s
  with the same one-worker `9 + 1` topology. At 325 it delivered 324.725 msg/s,
  reached a 224-job maximum backlog, drained in 0.521 seconds, and retained
  exact reconciliation with zero redelivery and DLQ.
- The first pre-batch throughput change was host-only worker concurrency. Two
  relay workers sharing the guaranteed relay connection sustained a direct
  500 msg/s stage at 499.958 msg/s, with a 383-job maximum backlog, 182.292 ms
  business p95, a 0.490-second drain, and exact reconciliation. ACK/delete and
  reservation remain individual; the library worker default is unchanged.
- A same-budget `8 + 2` A/B run removed relay-pool acquisition wait but shifted
  contention into the producer pool: producer acquire wait rose to 3.411
  seconds, maximum backlog reached 1,941, and business p95 reached 2.412
  seconds, failing the site SLO. The site profile therefore defaults to two
  relay workers with the proven `9 + 1` pool split. This targeted run selects a
  benchmark topology; it is not a general 500 msg/s capacity-floor claim.

## 2026-08-27 — PostgreSQL Inbox fresh-success optimization

- Replaced the fresh `ProcessAttempt` preparation with namespace-aware data-modifying CTEs for the default and explicit
  attempt-generation tables. A fresh identity now creates its initial attempt in the same statement; conflicts retain
  the existing identity lock, fingerprint validation, duplicate handling, and stored-attempt path. No schema, migration,
  public API, transaction ownership, or ACK-ordering contract changed.
- Removed explicit savepoint release before outer commit. The PostgreSQL and SQLite success paths commit directly; a
  failed handler still executes `ROLLBACK TO SAVEPOINT`, persists the durable failure outcome outside the rolled-back
  handler work, and lets the outer commit close the savepoint. PostgreSQL finalization/cancellation/concurrency tests and
  a dedicated SQLite rollback-then-success regression cover these boundaries.
- Recorded the candidate at commit `629bf011f910fff1b965073afa015530ab55e7bf` (`gitDirty=false`) on the same host and
  container topology as the baseline. PostgreSQL-only statement telemetry observed exactly 20,000 calls each to
  `BEGIN`, the combined identity/attempt CTE, `SAVEPOINT`, the one-row handler insert, completion update, and `COMMIT`.
  The missing-attempt read, separate attempt insert, and successful `RELEASE SAVEPOINT` disappeared, confirming the
  intended reduction from nine to six sequential database interactions.

  | PostgreSQL-only profile | Baseline median | Candidate median | Change |
  | --- | ---: | ---: | ---: |
  | C1 throughput | 1,643.40 ops/s | 2,739.97 ops/s | +66.7% |
  | C1 Inbox p50/p95/p99 | 0.564/0.760/1.284 ms | 0.346/0.429/0.617 ms | -38.7%/-43.6%/-51.9% |
  | C4 throughput | 4,968.82 ops/s | 7,232.74 ops/s | +45.6% |
  | C4 Inbox p50/p95/p99 | 0.752/1.138/1.574 ms | 0.529/0.664/0.991 ms | -29.7%/-41.6%/-37.0% |

- The three PostgreSQL 17 site-shaped repetitions produced the following candidate medians. Every reached stage passed
  exact post-drain reconciliation with no redelivery or DLQ.

  | Target | Passed/reached | Median effective | Median Inbox p50/p95/p99 | Median business p95 | Median drain |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | 250 msg/s | 3/3 | 249.950 msg/s | 0.545/0.862/1.309 ms | 100.553 ms | 0.510 s |
  | 325 msg/s | 2/3 | 324.733 msg/s | 0.530/1.036/2.215 ms | 322.772 ms | 0.452 s |
  | 350 msg/s | 2/2 | 348.558 msg/s | 0.528/0.930/1.863 ms | 225.465 ms | 0.787 s |
  | 400 msg/s | 2/2 | 399.838 msg/s | 0.527/0.985/2.220 ms | 607.252 ms | 0.448 s |
  | 500 msg/s | 0/2 | 463.042 msg/s | 0.554/1.456/2.234 ms | 10,572.612 ms | 10.502 s |

  Two repetitions sustained every stage through 400 msg/s and stopped at 500. The remaining repetition sustained 250
  and stopped at 325 after the Outbox backlog reached 6,973, k6 dropped 117 iterations, and business p95 reached
  19.14 seconds; its Inbox p95 was only 1.84 ms, and integrity still passed. The 350 target therefore passed every run
  that reached it, but not all three scheduled repetitions reached that target. Under the strict all-repetition reading,
  this local matrix does not yet authorize describing a patch release as a proven 350 msg/s performance fix.
- The unchanged PostgreSQL 18 O4/C4/32 quick profile sustained 500 msg/s in all three candidate repetitions. At 500,
  median effective throughput was 499.700 msg/s and median Inbox p50/p95/p99 was 1.001/1.544/2.107 ms versus the
  baseline 499.667 msg/s and 1.380/2.155/3.207 ms; median business p95 was 95.039 ms and median drain was 0.384 s.
- Historical resource peaks were not directly comparable after the local Docker/host state shifted. A detached
  contemporaneous baseline control measured 53.70 MiB peak application RSS and 40.02 MiB PostgreSQL-only runner RSS;
  candidate medians were 52.09 MiB and 29.07 MiB respectively. The control also retained slower Inbox latency, so the
  candidate has no observed greater-than-5% RSS or C4 latency regression in the contemporaneous comparison. Raw
  candidate and control evidence remains ignored under `tmp/capacity/`.

## 2026-08-27 — PostgreSQL Inbox measurement baseline

- Added a named PostgreSQL 17 site-shaped capacity profile while preserving the existing PostgreSQL 18 quick/full
  defaults. The new profile fixes Outbox/consumer concurrency at `1/1`, the business pool at `10`, uses a small
  deterministic payload by default, and keeps the existing 80/15/5 mix as an explicit override.
- Enabled capacity-only `pg_stat_statements`, query IDs, utility tracking, I/O timing, and WAL I/O timing. Each full-path
  stage records before/load-end/post-drain statement, database, WAL, and version-tolerant `pg_stat_io` snapshots plus
  sampled relevant waits. Controller SQL is marked and excluded from Inbox classification.
- Attached the existing NATS `OperationHandle` and `OperationBrokerAck` observations without adding consumer SQL. The
  producer registers `message_id -> run/stage` inside its transaction before commit and removes the mapping on failure.
- Added a PostgreSQL-only `ProcessAttempt` runner with one transactional handler insert, prebuilt identities and
  fingerprints, default C1/C4 `20,000`-operation cases, three repetitions, exact integrity checks, and statement/WAL/I/O
  deltas.
- Both isolated runners now write `resources.jsonl` with container CPU, RAM, and cumulative Block I/O alongside ignored
  reports and raw artifacts. Hosted CI remains unchanged and does not execute performance workloads.
- Recorded the final comparable baseline at commit `5bbe6521e717db52693ac2dff76986b737362235` (`gitDirty=false`) on a
  MacBook Pro `Mac17,9`, Apple M5 Pro (15 cores), 24 GB RAM, macOS 26.5.1 arm64. The Linux arm64 containers saw 12 CPUs
  and a 3.824 GiB memory limit; the toolchain was Go 1.27.0, NATS 2.12.3, k6 2.2.0, PostgreSQL 17.9 for the site and
  Inbox-only profiles, and PostgreSQL 18.6 for the existing quick profile. JetStream used file storage. Every recorded
  run passed exact integrity reconciliation with no lost or duplicate business effect.

  | Profile | Target | Passed/reached | Median effective | Median Inbox p50/p95/p99 | Median business p95 | Median drain | Median peak app RSS |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | PostgreSQL-only | C1 | 3/3 | 1,643.40 ops/s | 0.564/0.760/1.284 ms | — | — | 22.55 MiB¹ |
  | PostgreSQL-only | C4 | 3/3 | 4,968.82 ops/s | 0.752/1.138/1.574 ms | — | — | 22.55 MiB¹ |
  | PostgreSQL 17 site-shaped O1/C1 | 250 msg/s | 2/3 | 249.817 msg/s | 0.736/1.650/2.623 ms | 103.833 ms | 0.908 s | 26.09 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 325 msg/s | 1/2² | 324.825 msg/s | 0.728/1.539/2.579 ms | 1,317.814 ms | 0.736 s | 26.09 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 350 msg/s | 1/1² | 349.183 msg/s | 0.696/1.435/2.516 ms | 726.282 ms | 0.740 s | 26.09 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 400 msg/s | 0/1² | 388.375 msg/s | 0.734/1.678/2.730 ms | 4,235.479 ms | 3.772 s | 26.09 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 500 msg/s | 0/0² | — | — | — | — | 26.09 MiB |
  | PostgreSQL 18 quick O4/C4 | 50 msg/s | 3/3 | 49.967 msg/s | 1.811/2.959/4.430 ms | 99.445 ms | 0.607 s | 31.97 MiB |
  | PostgreSQL 18 quick O4/C4 | 100 msg/s | 3/3 | 99.733 msg/s | 1.673/2.493/3.068 ms | 98.650 ms | 0.515 s | 31.97 MiB |
  | PostgreSQL 18 quick O4/C4 | 250 msg/s | 3/3 | 249.900 msg/s | 1.543/2.297/3.148 ms | 97.011 ms | 0.656 s | 31.97 MiB |
  | PostgreSQL 18 quick O4/C4 | 500 msg/s | 3/3 | 499.667 msg/s | 1.380/2.155/3.207 ms | 93.410 ms | 0.562 s | 31.97 MiB |

  ¹ The PostgreSQL-only runner samples one process across all six C1/C4 cases, so its single peak applies to both rows.
  ² The controller stops a repetition at its first unsustainable stage. One PostgreSQL 17 run stopped at 325 because
  business p95 reached 2,339.54 ms; another stopped at 250 after a local scheduling stall dropped 54 k6 iterations. The
  remaining run passed 350 and stopped at 400 with 388.375 committed msg/s and 4,235.48 ms business p95. Later targets
  therefore have fewer observations, and an unmeasured target is reported as `0/0` rather than as a failure.
- This final three-run PostgreSQL 17 set does not justify a `capacity >= ...` claim that passed every repetition: 250
  msg/s passed 2/3, while 350 passed its sole reached run. This variability is itself baseline evidence for the
  adapter-only candidate. PostgreSQL 18 compatibility was stable in all repetitions and supports the checkout-local
  statement `capacity >= 500 msg/s` for the unchanged O4/C4 quick profile.
- PostgreSQL-only statement telemetry observed exactly 20,000 calls for each adapter-owned fresh-success statement per
  repetition: identity insert, missing-attempt select, attempt insert, savepoint, successful savepoint release, and
  completion update. Together with `BEGIN`, the one-row handler insert, and `COMMIT`, this confirms the expected nine
  sequential database interactions that the adapter-only follow-up will compare against.
- Review and follow-up self-review tightened all runtime boundaries before the final matrix: backlog regression excludes
  samples after the load window; the PostgreSQL load-end snapshot is independently scheduled from the first offered
  request rather than from k6 process exit; Outbox and consumer runners are both supervised after readiness; and drain
  duration starts at the exact load boundary rather than after k6 graceful stop. Across the final full-path reports,
  PostgreSQL load-end snapshots landed 3-20 ms after the boundary. The corrected drain medians in the table include the
  complete post-window tail.

## 2026-08-27 — reproducible NATS capacity experiment

- Extended `examples/durable-postgres-nats` into a shared application runtime with the original deterministic
  retry/Inbox/DLQ correctness command, a long-lived capacity HTTP service, and an independent Go capacity controller.
  The measured path is `HTTP -> PostgreSQL business transaction + Outbox -> JetStream -> Inbox -> business projection`;
  the root public API and root dependency boundary are unchanged.
- Added deterministic 80/15/5 order sizes, transaction-local exact canonical envelope byte/SHA-256 measurements,
  broker-confirmation marking after JetStream `PubAck`, unique message identities on business/projection records, and
  generator `offered_at` plus server `accepted_at` timestamps. Throughput uses a half-open timestamped load window;
  k6 summary and bounded drain cannot improve the result.
- Added a Go controller around pinned `grafana/k6:2.2.0` open-loop stages. It records one-second PostgreSQL, Outbox,
  JetStream, application, and pool samples; applies the 99% throughput, backlog-slope, p95, drain, redelivery, and DLQ
  criteria; and performs exact post-drain reconciliation before advancing. Integrity failures are distinct from an
  expected unsustainable capacity boundary.
- Added a dedicated file-backed JetStream/PostgreSQL Compose stack with named volumes, quick/full and minimum-rate Make
  contracts, automatic isolated cleanup, optional diagnostic retention, and ignored JSON/Markdown/raw artifacts under
  `tmp/capacity/<run-id>/`. Heavy capacity execution remains explicit and outside hosted CI.
- Targeted unit tests and strict lint pass. A clean low-rate Docker smoke passed the exact window and post-drain checks:
  25 unique projections committed inside a five-second 5 msg/s window, and all 26 HTTP-accepted boundary requests
  reconciled after drain with no redelivery or DLQ. The standard quick profile then passed every 30-second stage through
  500 msg/s. Its final stage committed 14,996 unique projections inside the load window (499.87 effective msg/s and
  2.350 effective MiB/s), reconciled all 15,001 accepted orders after drain, and observed no dropped iteration,
  redelivery, or DLQ. The result is only `capacity >= 500 msg/s` for the recorded dirty checkout and local host.
- The refactored correctness demo passed its retry rollback, Inbox duplicate suppression, permanent DLQ, replay, and
  broker-deduplication scenarios. The completed batch passed `make check`: formatting, build, vet, lint, unit,
  race, checkptr, 91.0% root coverage, the clean consumer probe, and the durable transactional JetStream E2E.

## 2026-08-26 — v0.2.1 lifecycle hardening

- Kept NATS readiness false until its pull iterator and worker pool exist, cleared it as soon as the pull loop stops,
  and tied it to the live run context so cancellation during startup cannot produce a transient healthy result.
- Made recovered managed-service `BeginDrain` panics deterministic lifecycle failures: every service still receives the
  drain request, the active run context is force-cancelled, and `Run`/`Shutdown` retain the safe structural panic error.
- Added channel-synchronised regressions for the NATS startup window, cancelled startup, and runtime drain-panic path;
  targeted root and NATS tests passed under the race detector. The final `make check` passed with clean static analysis,
  all module unit/race/checkptr gates, 91.0% root coverage, the clean consumer probe, and durable embedded-NATS E2E.
- Added repository, pull-request-template, changelog, and release-runbook gates for an enabled GitHub Codex Code Review
  result on the current pull-request head. The runbook checks included feature pull requests before local preparation
  and the final release pull-request head only after the release contents are fixed.

## 2026-08-26 — v0.2.0 multi-module release

- Selected `v0.2.0` rather than a patch release because the compatible hardening batch adds public `PanicReporter`,
  `FailureSanitizer`, `Runtime.Liveness`, `Runtime.DeepHealth`, and shutdown-timeout contracts in addition to fixing
  durable middleware completion.
- Extended the clean published-consumer probe to compile the new root and independently versioned NATS/Kafka failure
  contracts without local replacements.
- Published immutable `v0.2.0` tags in dependency order for root `74826a7`, Inbox/Outbox/observability `5fa2088`,
  NATS/Kafka `44631a2`, and `gomessengerctl` `e2fa92e`. PR #11 merged the complete graph as `cf85951`.
- The final `make check` passed against the published graph with 90.8% root coverage; `make bench-all` and the clean
  published-consumer probe also passed. The consumer downloaded every library module, compiled the new cross-module
  contracts, installed `gomessengerctl`, and used no local replacements.
- The local Kafka container could not create `/tmp/kafka-logs` because the Docker internal filesystem was full, before
  broker readiness or Go test execution. The exact release PR supplied the missing evidence: PostgreSQL 18, Kafka
  4.1.2/4.3.1, static analysis, unit/race, checkptr, aggregate Full gate, and benchmark comparison all passed.
- GitHub Release `v0.2.0` is published. The separate real-service pilot remains pending, so the release is not described
  as production-proven.

## 2026-08-26 — upgrade-logic-base review fixes

- Replaced the branch-specific `.hardening` patch staging with the actual NATS/Kafka source and regression tests. The
  benchmark workflow is read-only, does not persist checkout credentials or push back to pull-request branches, and
  uploads only benchmark results instead of source, toolchain, and module-cache snapshots.
- Kept recovered panic values and stacks out of ordinary errors, observations, logs, and DLQ records. Trusted hosts may
  opt in through `PanicReporter`; adapter failure text defaults to a panic-isolated conservative sanitizer that retains
  `errors.Is`/`errors.As` through its error wrapper.
- Preserved parent cancellation when a custom context propagator returns nil, rejected typed-nil panic reporters, and
  made NATS standalone shutdown force-cancel its owned run context once the shutdown deadline expires.
- Durable middleware completion now rechecks the exact context passed to the handler after the full middleware chain,
  so a replacement-context deadline cannot be swallowed into an Inbox commit. `HandlerPanicError` is a structural safe
  interface implemented by independently versioned adapters, and the root sanitizer classifies it without adapter
  source depending on an unreleased root symbol.
- Clarified that install commands and the Go Reference badge describe published `v0.1.0`, while the remainder of the
  README and the checkout-only PostgreSQL/NATS demo may describe unreleased follow-up changes.
- Split handler execution observations from broker ACK, Kafka offset commit, retry hand-off, and DLQ hand-off. NATS now
  accepts `ErrMsgAlreadyAckd` as confirmed finalization, while both adapters expose expensive topology validation through
  `DeepHealth` and keep ordinary readiness lightweight.
- Restored `WithClock` as the source of normalized Messenger receipt timestamps and propagated lifecycle contexts through
  runtime drain reporting instead of detaching them with `context.Background()`.
- The completed batch passed `make check`: isolated `GOWORK=off` build/vet/lint, unit, race, checkptr, 90.8% root
  coverage, the clean consumer probe, and the durable embedded-JetStream E2E. The opt-in Docker Kafka compatibility gate
  was not rerun as part of this review fix.

## 2026-08-26 — v0.1.0 multi-module release

- Published immutable `v0.1.0` tags in dependency order for the root, Inbox, Outbox, observability, NATS, Kafka, and
  `gomessengerctl` modules. Each published module now uses exact `v0.1.0` requirements and contains no development
  `replace`; local pre-tag replacements remain confined to `go.work` and checkout-only fixtures.
- PR #6 passed the hosted aggregate gate, including static analysis, unit/race/checkptr, PostgreSQL 18, Kafka 4.1.2 and
  4.3.1, durable E2E, coverage, and benchmark comparison. The final local `make check` passed with 91.4% root coverage.
- The clean post-publication consumer downloaded every library module, installed `gomessengerctl`, and compiled the
  public facade without local replacements. GitHub Release `v0.1.0` is published; the separate real-service pilot
  remains pending, so the release is not described as production-proven.

## 2026-08-26 — upgrade-sql review fixes

- Made Kafka retry preflight expiry-aware without moving application code into the rebalance-blocked section. The
  bounded preflight now validates the native envelope, descriptor, record key, and retry window before scheduling;
  `not-before` at or beyond `ExpiresAt`, already expired messages, and malformed envelopes proceed to post-release
  terminal handling without pausing a partition. Custom codec decoding remains after `AllowRebalance`, and valid early
  retries still retain only partition/deadline state until refetch.
- Made the checkout demo's build read-only by compiling both main modules to `/dev/null`, ignoring the demo executable in
  Git and Docker contexts, and removing the accidentally tracked workstation binary.
- The review-fix batch passed the Kafka module tests, the full `make check` gate with 91.4% root coverage, and the live
  transactional Kafka pipeline against official 4.1.2 and 4.3.1 images. The build gate also left the demo executable
  absent from the worktree.

## 2026-08-25 — adoption and positioning surface

- Reframed the README around `Typed durable messaging for Go`, with an immediate problem statement, honest pre-release
  status, local quickstart, guarantees, non-goals, route-selection table, and durable demo path. Release-dependent
  `go get`, latest-release, and pkg.go.dev claims remain deferred until the dependency-ordered tags pass the clean
  published-consumer probe.
- Added a use-case comparison that states when GoBus, raw JetStream, raw franz-go/Kafka, Watermill, a workflow engine,
  synchronous RPC, or a custom Outbox/Inbox is the smaller or more appropriate boundary. It avoids exactly-once,
  universal broker, workflow, or production-proven claims.
- Added `examples/durable-postgres-nats`, a compose-managed one-shot application using PostgreSQL 18, NATS JetStream,
  the PostgreSQL Outbox backend, a namespaced PostgreSQL Inbox, intentional retry rollback, distinct duplicate broker
  delivery, permanent DLQ hand-off, and deterministic confirmed replay. The checkout-local module is included in
  formatting, build, vet, lint, unit, race, and checkptr gates without presenting its local replacements as release
  evidence.
- Recorded the post-publication README/badge/launch checklist and linked the runnable demo from usage, E2E, migration,
  and README navigation. GitHub About and topics were already aligned with the recommended positioning; no release,
  tag, commit, or push is part of this batch.
- The live compose smoke rejected an initially invalid 50 ms Outbox idle interval, then passed after using the backend's
  supported 200 ms value. The successful rerun reused the already migrated PostgreSQL container and proved idempotent
  migration plus retry, one Inbox-suppressed distinct delivery, permanent DLQ hand-off, first replay commit, and
  deterministic duplicate replay. Demo containers, network, and volume were removed afterward.
- The completed adoption batch passed `make check` outside the filesystem sandbox: formatting, build, vet, lint, unit,
  race, checkptr, 91.4% root coverage, clean consumer, and the durable embedded-JetStream E2E. A first sandboxed run was
  discarded because its local-socket policy prevented every embedded NATS server from becoming ready.

## 2026-08-25 — native transactional Kafka adapter

- Added the independent `adapters/kafka` module on franz-go v1.21.6 with adapter-owned clients, required stable process
  identity, one static group member and transactional ID per worker, read-committed consumption, disabled auto-commit,
  and all-ISR transactional direct publish.
- Kept native command/event envelope v1 as the only Kafka wire contract. Source topics are
  `namespace.kind.descriptor.vN`; reserved kind segments keep dotted namespaces unambiguous, and the record key is the
  domain key or message ID. Queries, CloudEvents modes, and arbitrary payloads remain outside the adapter.
- Added consumer-specific retry tiers, replay ingress, DLQ topics, exact not-before control metadata, durable Inbox
  attempts, and Kafka transactions that atomically commit source offsets with retry or DLQ records. Retry topics require
  unlimited time and size retention; later source records may overtake retries.
- Added bounded Kafka DLQ v1 inspection and deterministic protected replay. Replay validates canonical bytes, descriptor
  source topic, record key, consumer target, and attempt generation without exposing payload or handler error in plans;
  bounded failure text is normalized and truncated on UTF-8 boundaries so emitted records always decode.
- Aligned adapter-owned franz-go producer batch caps with the full envelope plus duplicated Kafka record key for
  source/retry/replay and the larger DLQ message bound. Source and replay-ingress record timestamps now represent
  publication time while logical creation time remains immutable in the envelope.
- Hardened transport lifecycle around pre-start drain, active direct transactions, and fresh bounded abort cleanup;
  serialized transaction admission honors caller cancellation, while shutdown waits for admitted finalization until its
  deadline and then force-closes the client.
- Removed worker-wide Kafka retry head-of-line blocking with `BlockRebalanceOnPoll`, a fast record preflight, exact
  leader-epoch/offset rewind, and a bounded per-worker deadline heap that retains no payload. An early retry pauses only
  its concrete topic-partition, preserves foreign pauses, fails the worker closed if the rewind is not confirmed, and
  is fetched again after the scheduler-owned pause resumes. The blocked section performs bounded control and native
  envelope/descriptor/key/timing validation plus any exact pause/rewind; custom `Codec.Decode`, handler, Inbox, and Kafka
  transaction work run after `AllowRebalance`. Valid early retries do not invoke the codec until refetch. Unit
  regressions cover multiple deadlines, other-partition progress, offset safety, resume/refetch, blocking codecs,
  foreign pauses, cancellation, drain, rebalance release, and fail-closed rewind; the live Kafka scenario uses two
  partitions and one worker to prove a barrier completes before a two-second retry deadline and exactly one second
  attempt follows.
- Added validated and SQL-quoted Inbox namespaces while preserving existing calls without options and the
  `gomessenger_` defaults. PostgreSQL and SQLite accept `WithTablePrefix`; PostgreSQL additionally accepts `WithSchema`
  and qualifies all runtime and migration relations without creating the schema. Statements are rendered once per
  backend, migrations use the same resolved names, and changing namespace intentionally creates a separate Inbox without
  copying prior history. The full-handler SQL transaction and host-owned `database/sql` pool backpressure remain intact.
- This namespace/rebalance follow-up passed `make check`, the PostgreSQL 18 DSN gate in a disposable container, and the
  transactional Kafka pipeline against official 4.1.2 and 4.3.1 images. The shared wiki lint passed with only its
  pre-existing stale-raw warnings; these controlled gates do not establish production readiness.
- Wired `TransportConfig.Logger` to adapter-owned startup/readiness, producer and consumer transaction, abort/fencing,
  topology, and retry partition deferral events. Structured attributes remain infrastructure-only and exclude record
  keys, payloads, and headers; franz-go client logging remains a separate explicit connection option.
- Closed follow-up review gaps by cloning retained broker input, replacing raw franz-go options with sealed
  connection-only wrappers, validating topology before starting consumer workers, and deriving rebalance timeout from
  bounded broker finalization instead of handler duration.
- Closed final Kafka review gaps: direct publishes recheck expiry after serialized admission and classify deterministic
  topic incompatibility as permanent for Outbox relay; consumer transaction finalization now preserves force
  cancellation; hooks cannot receive mutable records or the live client; consumer-only wiring registers the shared
  transport explicitly for managed shutdown.
- Removed naming collisions in consumer service topics/groups and transactional IDs. Kafka source derivation now
  reserves the `gm` segment and every service-name helper revalidates its canonical source; transactional IDs use a
  versioned SHA-256 digest of the framed group, instance, and worker tuple.
- Made full-jitter retry delays strictly positive so a valid retry configuration cannot randomly terminate a worker.
- Added declarative topology spec `1.0` for partitions, replication factor, minimum ISR, retention time/bytes, and
  maximum message bytes. Apply creates missing topics or strengthens managed configurations only; partition drift is an
  ordering-sensitive conflict, replication is verified across every partition, unsafe drift is refused, and no
  resource is deleted/recreated.
- Extended `gomessengerctl` under `kafka` while preserving existing NATS commands. TLS, CA/mTLS, PLAIN, and SCRAM
  connection flags are available; SASL passwords are read from a named environment variable.
- Added clean-consumer and release-module coverage plus `make test-kafka`. The local gate exposed and fixed an invalid
  franz-go idempotence option, a Kafka 4.x inter-broker transaction-listener requirement in the harness, poll-timeout
  classification, and a pause-state restoration bug. The complete direct/Outbox/Inbox/retry/DLQ/replay pipeline passed
  against official Kafka 4.1.2 and 4.3.1 images, including source envelopes and expanded DLQ records above franz-go's
  default record-batch limit.
- Split hosted `Full gate` work into parallel static, test/race, and checkptr shards while preserving `CI / Full gate`
  as their required aggregate result. The static shard uses the pinned official golangci-lint action, a prebuilt binary,
  and its analysis cache; local `make lint` and `make check` retain the complete read-only module-isolation contract.
- The completed source batch passed `make check`: formatting, build, vet, lint, unit, race, checkptr, 91.4% root
  coverage, clean consumer probes, and the durable transactional E2E.

## 2026-08-24 — typed local query facade

- Added `KindQuery`, `Query[Q,R]`, `Querier[Q,R]`, payload and message handlers, typed query middleware, builder
  registration/routing, and `Messenger.Query` without adding a serialized result contract.
- Query identity now checks request descriptor metadata plus exact Go request/result types. Build and manifest
  validation require exactly one handler and one sealed local route; manifest spec stays `1.0` and omits result type.
- Local sync and bounded async routes register an internal GoBus result executor. Async query admission, execution, and
  waiting retain caller cancellation; result delivery is buffered for safe drain. Existing async command/event
  execution remains runtime-owned after admission.
- Query metadata carries generated identity, lineage, source/time, and trace propagation. Global middleware still owns
  a result-less terminal and reports `ErrQueryResultMissing` when successful completion produced no `R`; typed query
  middleware can synthesize a result.
- Added handler and whole-query observations without payload/result/header fields. Native envelopes, Outbox, NATS
  subjects/routes, and CloudEvents remain command/event-only and explicitly reject `KindQuery`.
- Preserved a valid `nil` interface query request across the internal type-erasure boundary and covered the regression.
- Removed trace-header validation allocations for mixed-case keys. A stack-based DLQ replay-header duplicate scan was
  benchmarked but rejected because it regressed the 64-header bound; the existing map remains on that failure/replay path.
- Made the Inbox transaction finalization grace configurable through `HandlerConfig.FinalizationTimeout` (5-second
  default), while documenting the already-required host database pool headroom.
- Replaced the former command/event-only decision with ADR-0001, selected the additive `site` article-publication audit
  in ADR-0002 without claiming implementation, and added ADR-0003 for the still-unimplemented distributed contract.
- Targeted root unit and query race tests, the clean checkout consumer, Outbox rejection, and an embedded-NATS query
  rejection test passed during implementation. The completed batch then passed `make check`: formatting, build, vet,
  lint, unit, race, checkptr, 91.4% root coverage, clean checkout consumer, and the durable JetStream/Inbox E2E.

## 2026-08-24 — initial typed durable messenger suite

- Built the complete typed GoMessenger messaging suite for Go 1.27+ as a multi-module repository without turning GoBus into a broker abstraction.
- Root core (`github.com/assurrussa/gomessenger`) owns descriptors, envelope v1, message identity and lineage (`uuid.NewV7`, UTC timestamps), local sync/async GoBus routes, runtime lifecycle aggregation, global/typed middleware chains, and manifest generation.
- Nested modules own transactional outbox (`adapters/outbox`), NATS JetStream delivery and topology (`adapters/nats`), durable SQL inbox (`adapters/inbox`), Prometheus and OpenTelemetry tracing (`observability`), and operational CLI (`tools/gomessengerctl`).
- Pinned Outbox root and SQLite backend dependencies together at `v0.11.0`; local joint-development overrides remain strictly in `go.work`, while clean consumer and E2E modules verify published tags with `GOWORK=off`.
- Established explicit at-least-once delivery with end-to-end idempotent processing: transactional outbox staging, broker-confirmed JetStream delivery, and durable SQL inbox deduplication with atomic business handler transactions via `inbox.SQLTxFromContext`.
- Descriptors use explicit kind (`command`/`event`), stable wire names, positive schema versions, content types, explicit data encodings (`json`/`text`/`binary`), and optional schema URIs. Implicit reflection types, package paths, and trailing JSON values are rejected.
- Defined strict canonical envelope v1 (max 1 MiB, max 64 headers, max 16 KiB header payload, deterministic JSON, SHA-256 fingerprinting) with UTC-normalized `Time`, `NotBefore`, and `ExpiresAt`.
- Added CloudEvents 1.0 structured and binary mode support for events, enforcing required `dataencoding` extension, payload preservation, and deterministic UUIDv7 fallback for omitted event timestamps.
- Implemented pull-based JetStream consumers with explicit concurrency bounds, timeout, `AckWait` (>=100 ms) with background progress heartbeats (`AckWait / 3`), and recoverable pull heartbeat handling (`ErrNoHeartbeat`).
- Added durable attempt accounting in SQLite and PostgreSQL (`gomessenger_inbox_attempts` and `gomessenger_inbox_attempt_generations`): handler invocations, savepoints, and permanent outcomes persist across restarts independently of transient broker delivery snapshots. Pre-handler infrastructure failures and `NotBefore` delays do not consume handler attempts.
- Established terminal hand-off and DLQ reliability: permanent failures and attempt exhaustion publish to DLQ and confirm with `DoubleAck` before source acknowledgement. Consumer-scoped attempt generations ensure explicit DLQ replay starts a clean bounded counter without allowing broker redelivery to reset `MaxAttempts`.
- Hardened service lifecycle: single-owner atomic `BeginDrain` closes admission immediately, concurrent deterministic `Shutdown` propagates deadlines to active handlers, never-started consumers close synchronously, and delivery boundaries recover panics to protect supervised execution.
- Implemented transport-neutral logging (`messenger.Logger`, `messenger.AdaptSlog`), additive panic-isolated observers, and W3C Trace Context (`traceparent`, `tracestate`) propagation while strictly excluding payloads and arbitrary headers from logs and metric labels.
- Added declarative, non-destructive JetStream topology management (`gomessengerctl`) supporting additive stream/consumer synchronization, semantic wildcard subject widening, dedicated DLQ streams (`DevDLQStream`), startup payload-limit validation, and payload-redacted offline/confirmed DLQ inspection and replay.
- Recorded the original query boundary in ADR-0001 and the required real-project pilot before production-proven claims in ADR-0002; both decisions are superseded by the typed-local-query section above.
- At the initial typed durable messenger snapshot, the read-only `make check` gate passed with zero lint findings, 91.6% root coverage, unit, race, and checkptr tests across all modules, plus the Docker-free transactional E2E covering rollback, commit/acknowledgement, lost ACK, retry, permanent DLQ, replay deduplication and Inbox suppression, and drain/redelivery. The later typed-query gate and its current coverage are recorded in the section above.
- `make test-postgres` remains the separate DSN-gated PostgreSQL 18 integration gate for migrations, conflict and prune, rollback/retry, durable attempts/outcomes/generations, and concurrency. It is not part of `make check`; the final review batch compiled its regressions but did not include a live database rerun.
