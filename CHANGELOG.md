# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- typed command, local query/result, and event descriptors using Go 1.27 generic methods;
- deterministic native envelope v1 with UUIDv7 identity, lineage, scheduling, expiry, and bounded headers;
- standard-library UUIDv7 generation behind the strict `MessageID` and replaceable `IDGenerator` contract;
- synchronous and bounded asynchronous local routes backed by GoBus;
- global and typed command/event/query middleware plus payload-only registration helpers;
- one CQRS facade with `Query[Q,R]`, `Querier[Q,R]`, exact request/result type identity, required single-handler/local-route
  invariants, synchronous result dispatch, and bounded async result dispatch with caller cancellation;
- transport-neutral no-op logging, `slog` adapter, additive observers, panic isolation, and service-failure observations;
- supervised runtime with readiness, drain, parallel deterministic shutdown, and forced deadline cancellation;
- transactional outbox producer and relay integration;
- PostgreSQL and SQLite inbox deduplication;
- NATS JetStream routing, durable consumers, safe topology planning, CloudEvents modes, retry, configurable Inbox
  transaction finalization grace, and confirmed DLQ hand-off;
- optional Prometheus/OpenTelemetry observations and W3C Trace Context propagation across all wire modes and Outbox;
- manifest, topology, and safe offline/confirmed DLQ replay command-line tooling;
- a Docker-free transactional Outbox-to-JetStream-to-Inbox E2E gate covering
  rollback, lost ACK, retry, trace propagation, permanent DLQ, replay deduplication, inbox suppression, and
  drain/redelivery;
- an explicit local-versus-distributed query boundary, a versioned distributed request/reply ADR, and a selected
  article-publication audit pilot that remains pending until the library is published;
- clean local and published-consumer verification paths;
- GitHub Actions for the full read-only gate, PostgreSQL integration, and raw `benchstat` comparisons.

No release has been published yet.
