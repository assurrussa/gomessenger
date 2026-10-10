# ADR-0002: Pilot article publication audit in a real service

- Status: accepted; host implemented with historical local acceptance; selected-release and production acceptance remain separately qualified
- Date: 2026-08-24

## Context

Unit, race, checkptr, embedded JetStream, SQLite E2E, PostgreSQL integration, and clean-consumer tests prove library
contracts in controlled environments. They do not prove service integration cost, production-shaped throughput,
operational ownership, deployment behavior, or useful observability under a real workload.

The selected host is `sportsite` (formerly `site`). Its existing article-publication transaction already emits webhooks
with an immutable recipient snapshot. The pilot must not replace or reinterpret that path.

## Decision

The first real-project pilot is the additive one-way flow:

```text
article changes from draft to published
  -> existing webhook emission remains unchanged
  -> GoMessenger Outbox event content.article.published v1
  -> JetStream
  -> durable Inbox consumer
  -> article_publication_audit row
```

The event payload is `{articleId, slug, locale, publishedAt}`. The pilot is disabled by default for its initial dark
rollout and publishes only on the draft-to-published transition. Updates and unpublishing do not emit it. The audit
handler writes through `inbox.SQLTxFromContext`, so the audit row and Inbox completion marker commit in one PostgreSQL
transaction. The host owns its current enabled/disabled configuration.

The host implementation exists in `assurrussa/sportsite`. Every used GoMessenger module must remain pinned to the same
published release, with selected-version compatibility established by the consumer-update gates. Local `replace`
directives are forbidden in the host. The host uses Go 1.27, owns the NATS connection/topology and migrations, preserves
required-runner supervision, and pins Outbox root and PostgreSQL backend together.

## Compatibility constraints

- Existing webhook topics, payloads, recipient resolution, ordering, retry behavior, and immutable subscription
  revision snapshots stay unchanged. A regression test must prove the snapshot survives later subscription edits.
- Producer and consumer runtimes are long-lived and host-supervised. A restartable factory creates a fresh consumer
  runtime after failure; readiness, drain, logging, metrics, and tracing adapt to the host's existing systems.
- Topology uses namespace `site`, `SITE_EVENTS` on `site.event.>`, a bounded `SITE_DLQ`, and durable consumer
  `site-article-publication-audit-v1`; source payloads remain bounded to 1 MiB.
- Rollout is additive: migrations and topology land while disabled, then a canary enables the pilot. Rollback disables
  the flag or restores the runtime image; existing webhooks and additive tables remain safe.

## Acceptance

The pilot must prove business rollback, Outbox staging, JetStream `PubAck`, one audit row, real lost-ACK redelivery
suppression, transient retry, permanent DLQ and confirmed replay, process and persistent-NATS restart, PostgreSQL
outage/recovery, graceful and forced drain, operational telemetry, and a 15-minute 5 events/s soak. After the soak, lag
must reach zero within 30 seconds, no row may be missing or duplicated, DLQ must remain unexpectedly empty, p95 handler
latency must stay below 500 ms, and at least half of the pgx pool must remain free.

## Existing host evidence

The [historical host pilot report](https://github.com/assurrussa/sportsite/blob/3e59b2c6cf9a531092f369598ab6913709ef7325/docs/tasks/gomessenger-site-pilot.md)
records implementation, real local admin acceptance, and a 901.390-second local soak with 4,500 audit rows. The
[version-specific follow-up](https://github.com/assurrussa/sportsite/blob/3e59b2c6cf9a531092f369598ab6913709ef7325/docs/tasks/2026-09-06-gomessenger-v030.md)
records PostgreSQL/NATS E2E acceptance for GoMessenger v0.3.0 and Outbox v0.15.0. Preserve these historical passes;
they are committed reports, not raw current-version execution logs or production-readiness proof.

Current host sources and reusable opt-in E2E/soak commands are in
[sportsite](https://github.com/assurrussa/sportsite/blob/dfcf3f30e3c2e76593ed89ba0002a176ed117945/docs/backend/messaging.md).
Its runtime-restart fixture replaces a consumer inside one test process. The separate
[NATS process screen](../performance/nats-process-operations.md) addresses process/rolling behavior only in its stated
direct-NATS/SQLite scope. Neither a two-minute synthetic screen nor ordinary repository gates replaces host or
production acceptance. This ADR does not authorize deployment or a new production retention policy.
