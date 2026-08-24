# Implementation notes

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
