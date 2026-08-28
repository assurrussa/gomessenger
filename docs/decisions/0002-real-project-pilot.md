# ADR-0002: Pilot article publication audit in a real service

- Status: accepted; target selected; publication prerequisite satisfied; `site` implementation remains pending
- Date: 2026-08-24

## Context

Unit, race, checkptr, embedded JetStream, SQLite E2E, PostgreSQL integration, and clean-consumer tests prove library
contracts in controlled environments. They do not prove service integration cost, production-shaped throughput,
operational ownership, deployment behavior, or useful observability under a real workload.

The selected host is `site`. Its existing article-publication transaction already emits webhooks with an immutable
recipient snapshot. A pilot must not replace or reinterpret that path.

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

The event payload is `{articleId, slug, locale, publishedAt}`. The pilot is disabled by default and publishes only on
the draft-to-published transition. Updates and unpublishing do not emit it. The audit handler writes through
`inbox.SQLTxFromContext`, so the audit row and Inbox completion marker commit in one PostgreSQL transaction.

Implementation belongs in a separate `site` project task and branch after every used GoMessenger module is published
as `v0.1.0` and passes the clean release-consumer gate. Local `replace` directives are forbidden. The host must upgrade
Go to 1.27, pin Outbox root and PostgreSQL backend together at `v0.12.0`, own the NATS connection/topology and
migrations, and preserve existing required-runner supervision.

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

The separate pilot task must prove business rollback, Outbox staging, JetStream `PubAck`, one audit row, real lost-ACK
redelivery suppression, transient retry, permanent DLQ and confirmed replay, process and persistent-NATS restart,
PostgreSQL outage/recovery, graceful and forced drain, operational telemetry, and a 15-minute 5 events/s soak. After
the soak, lag must reach zero within 30 seconds, no row may be missing or duplicated, DLQ must remain unexpectedly
empty, p95 handler latency must stay below 500 ms, and at least half of the pgx pool must remain free.

Repository gates alone do not satisfy this ADR, and this document does not claim that the pilot is implemented.
