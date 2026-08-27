# Implementation notes

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
- Recorded the comparable baseline at commit `175d83a6ad5504b1d6ed4584f28b42f25db75979` (`gitDirty=false`) on a
  MacBook Pro `Mac17,9`, Apple M5 Pro (15 cores), 24 GB RAM, macOS 26.5.1 arm64. The Linux arm64 containers saw 12 CPUs
  and a 3.824 GiB memory limit; the toolchain was Go 1.27.0, NATS 2.12.3, k6 2.2.0, PostgreSQL 17.9 for the site and
  Inbox-only profiles, and PostgreSQL 18.6 for the existing quick profile. JetStream used file storage. Every recorded
  run passed exact integrity reconciliation with no lost or duplicate business effect.

  | Profile | Target | Passed/reached | Median effective | Median Inbox p50/p95/p99 | Median business p95 | Median drain | Median peak app RSS |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | PostgreSQL-only | C1 | 3/3 | 1,722.11 ops/s | 0.554/0.696/1.216 ms | — | — | 19.50 MiB¹ |
  | PostgreSQL-only | C4 | 3/3 | 5,194.48 ops/s | 0.723/1.078/1.595 ms | — | — | 19.50 MiB¹ |
  | PostgreSQL 17 site-shaped O1/C1 | 250 msg/s | 3/3 | 249.892 msg/s | 0.682/1.456/2.439 ms | 103.116 ms | 0.019 s | 32.04 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 325 msg/s | 3/3 | 324.925 msg/s | 0.641/1.316/1.997 ms | 106.160 ms | 0.032 s | 32.04 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 350 msg/s | 2/3 | 349.650 msg/s | 0.690/1.295/2.268 ms | 511.827 ms | 0.031 s | 32.04 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 400 msg/s | 1/2² | — | — | — | — | 32.04 MiB |
  | PostgreSQL 17 site-shaped O1/C1 | 500 msg/s | 0/1² | — | — | — | — | 32.04 MiB |
  | PostgreSQL 18 quick O4/C4 | 50 msg/s | 3/3 | 49.933 msg/s | 1.914/3.774/4.670 ms | 99.053 ms | 0.004 s | 36.70 MiB |
  | PostgreSQL 18 quick O4/C4 | 100 msg/s | 3/3 | 99.800 msg/s | 1.748/3.653/4.536 ms | 98.579 ms | 0.006 s | 36.70 MiB |
  | PostgreSQL 18 quick O4/C4 | 250 msg/s | 3/3 | 249.533 msg/s | 1.665/3.576/4.222 ms | 96.236 ms | 0.010 s | 36.70 MiB |
  | PostgreSQL 18 quick O4/C4 | 500 msg/s | 2/3 | 499.400 msg/s | 1.311/2.248/3.794 ms | 85.961 ms | 0.021 s | 36.70 MiB |

  ¹ The PostgreSQL-only runner samples one process across all six C1/C4 cases, so its single peak applies to both rows.
  ² The controller stops a repetition at its first unsustainable stage. PostgreSQL 17 therefore reached 400 twice and
  500 once; incomplete targets deliberately have no cross-run median.
- The repeatable site-shaped floor is `capacity >= 325 msg/s`; 350 msg/s is the measured boundary, not a capacity claim:
  it passed two repetitions and failed one because k6 dropped six iterations during a checkpoint-shaped stall. The
  PostgreSQL 18 quick profile similarly passed 500 msg/s twice and failed once with 339 dropped iterations, so its
  repeatable claim remains `capacity >= 250 msg/s` despite a 499.400 msg/s median at the highest tested target.
- PostgreSQL-only statement telemetry observed exactly 20,000 calls for each adapter-owned fresh-success statement per
  repetition: identity insert, missing-attempt select, attempt insert, savepoint, successful savepoint release, and
  completion update. Together with `BEGIN`, the one-row handler insert, and `COMMIT`, this confirms the expected nine
  sequential database interactions that the adapter-only follow-up will compare against.

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
