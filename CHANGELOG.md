# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- transport-neutral panic reporting and failure sanitizing with a structural `HandlerPanicError` contract that remains
  classifiable across independently versioned adapters.

### Fixed

- NATS and Kafka durable handlers now validate completion against the exact context supplied through middleware, even
  when an outer middleware swallows the handler's cancellation error.

## [0.1.0] - 2026-08-26

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
- native-envelope Kafka routing, adapter-owned franz-go clients, static transactional consumers, retry topics,
  atomic offset/retry/DLQ hand-off, protected replay, and non-destructive topic planning;
- optional Prometheus/OpenTelemetry observations and W3C Trace Context propagation across all wire modes and Outbox;
- manifest, topology, and safe offline/confirmed DLQ replay command-line tooling;
- a Docker-free transactional Outbox-to-JetStream-to-Inbox E2E gate covering
  rollback, lost ACK, retry, trace propagation, permanent DLQ, replay deduplication, inbox suppression, and
  drain/redelivery;
- an opt-in local Docker gate covering direct publish, Outbox relay, Inbox suppression, retry, DLQ, and replay against
  official Kafka 4.1.2 and 4.3.1 images;
- an explicit local-versus-distributed query boundary, a versioned distributed request/reply ADR, and a selected
  article-publication audit pilot that remains pending until the library is published;
- an evidence-driven Level 2 roadmap covering schema compatibility, broker capabilities, partition ordering, batching,
  and operational validation;
- local method benchmarks for command send, query execution, and event publication;
- clean local and published-consumer verification paths;
- a runnable PostgreSQL-to-Outbox-to-JetStream-to-Inbox demo with intentional retry, duplicate suppression, permanent
  DLQ hand-off, and confirmed replay;
- public positioning as typed durable messaging for Go, a pre-release-safe README path, and an explicit use-case
  comparison against smaller, broker-native, workflow, RPC, and custom alternatives;
- GitHub Actions for the full read-only gate, PostgreSQL and Kafka integration, and raw `benchstat` comparisons.

### Fixed

- early Kafka retry records now pause, rewind, and defer only their own topic-partition, preserving exact offsets and
  allowing the same worker to process other partitions until the retry deadline;
- the aggregate GitHub Actions `Full gate` now requires PostgreSQL and both supported Kafka integration jobs.
