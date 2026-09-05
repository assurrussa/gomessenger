# Choosing a messaging approach

GoMessenger is a good fit when a Go service needs typed commands, local queries, or events and the one-way path must
make database, broker, retry, and duplicate boundaries explicit. It is not the default answer for every distributed
system problem.

## Use-case comparison

| Approach | Best fit | What the application still owns |
|---|---|---|
| **GoMessenger** | typed Go contracts plus local dispatch or durable Outbox/Inbox delivery over JetStream or Kafka | host connections, migrations, topology policy, supervision, capacity, deployment, and idempotency outside the Inbox database |
| **GoBus** | small process-local command/event/result dispatch with minimal dependencies | every cross-process envelope, broker, durability, retry, deduplication, and lifecycle concern |
| **Raw NATS JetStream** | NATS-native systems that want direct control of subjects, streams, consumers, and acknowledgements | business-write atomicity, canonical contracts, SQL Inbox semantics, retry/DLQ policy, replay safety, and host lifecycle composition |
| **Raw franz-go / Kafka** | Kafka-native systems that need exact partition, record, group, and transaction control | application envelopes, descriptor compatibility, Inbox effects, retry/DLQ topics, replay policy, topology validation, and worker lifecycle |
| **Watermill** | applications that prefer a message router and broad Pub/Sub integration ecosystem | verification that the chosen plugins and middleware provide the exact transaction, deduplication, ordering, retry, and replay guarantees the service requires |
| **Temporal or another workflow engine** | durable multi-step workflows, timers, compensation, long-lived state, and activity orchestration | broker fan-out and event-log integration when those remain separate requirements |
| **HTTP or gRPC** | synchronous request/reply where the caller needs an immediate response and availability/error semantics are explicit | durable asynchronous hand-off, retained fan-out, consumer retry, DLQ, and replay when those are needed |
| **A custom Outbox/Inbox or CDC** | high-throughput platforms (10k–100k+ msg/s) or unusual storage constraints justifying full control | schema evolution, WAL capture / Debezium setup, locking, cleanup, replay, observability, operational tooling |
 
## Choose GoMessenger when
 
- a business mutation and outgoing command/event must share one transaction;
- a consumer's SQL effect and duplicate marker must share one transaction;
- typed descriptors should remain stable while a one-way route moves from local execution to Outbox, JetStream, or
  Kafka;
- bounded attempts, terminal classification, DLQ, replay, readiness, and drain should follow one documented contract;
- the team accepts explicit transport differences instead of expecting one lowest-common-denominator broker API.
 
## Choose something smaller when
 
- direct function calls or GoBus cover all process-local work;
- a service already has a small, well-tested NATS or Kafka integration and does not need the GoMessenger facade;
- adding a durable message would create more operational surface than the business requirement warrants.
 
## Choose something larger when
 
- work spans many durable steps, timers, human actions, or compensation and should be modeled as a workflow;
- event sourcing, stream processing, saga coordination, or distributed request/reply is the primary product boundary;
- platform throughput demands 50k–100k+ msg/s where relational polling outbox creates database contention: in this tier,
  implement GoMessenger's custom `Route` / `BatchRoute` with append-only streaming plus an external CDC engine
  (Debezium/WAL streamer) or direct broker streaming instead of the standard polling outbox.

GoMessenger [`v0.3.0`](https://github.com/assurrussa/gomessenger/releases/tag/v0.3.0) is the current release line.
Release completion requires its dependency-ordered tags and clean published-consumer probe; its real-service pilot is
still pending. Evaluate the checkout with the
[PostgreSQL + NATS durable demo](../examples/durable-postgres-nats), use the [contracts](contracts.md) to review exact
guarantees, and use the [release process](release.md) to distinguish source gates, published-module proof, and the
separate operational pilot.
