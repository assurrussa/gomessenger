# ADR-0004: Native transactional Kafka adapter

## Status

Accepted on 2026-08-25.

## Context

GoMessenger already has a complete JetStream route/consumer path, but Kafka needs a different reliability mechanism:
partitioned records and consumer offsets, read-committed isolation, stable group members, and Kafka transactions for
consume-transform-produce hand-off. Treating Kafka as a thin NATS implementation detail would either leak broker types
into the root facade or hide materially different ordering and retry behavior.

## Decision

- Add the independent module `github.com/assurrussa/gomessenger/adapters/kafka`, implemented with franz-go. Do not add
  a shared NATS/Kafka engine until repeated behavior proves a useful broker-neutral boundary.
- Keep the root facade and canonical native envelope unchanged. Kafka accepts only command/event envelope v1 in the
  first version; local queries, distributed request/reply, CloudEvents modes, and arbitrary payloads are excluded.
- Derive one source topic per descriptor as `namespace.kind.descriptor.vN`. Reserve `command` and `event` namespace
  segments so that mapping is injective. Reserve the `gm` segment in source namespaces and descriptors so the `.gm.`
  consumer-service boundary is unambiguous. Use `Metadata.Key`, falling back to message ID, as the Kafka record key.
- Require a stable host `InstanceID`. Use one static group member per worker. Derive each transactional ID with a
  versioned SHA-256 digest of the complete group, instance, and worker tuple, and use a separate transactional producer
  for direct publish and replay.
- Accept host connection customization only through sealed TLS, SASL, dialer, hook, rack, and client-logging options;
  reject hooks that receive mutable records or the live client, and do not expose raw franz-go options that can replace
  adapter-owned producer, consumer, group, or transaction policy.
- Commit successful consumed offsets transactionally. Commit retry or DLQ production atomically with the consumed
  offset. All consumers use read-committed isolation and disabled auto-commit.
- Use consumer-specific retry tiers, replay ingress, DLQ topics, and consumer groups. Default retry tiers are 1 second,
  10 seconds, 1 minute, and 5 minutes. Retry topics have unlimited time and size retention.
- Preserve exact `not-before` in control headers. Topic tier is a scheduling bucket, not the logical retry deadline.
- Verify topic presence, equal partition counts, and unlimited retry retention before starting workers. Bound group
  rebalance completion by broker transaction finalization rather than handler execution time.
- Preserve source ordering only until the first failure. Records routed through retry topics may be overtaken.
- Keep DLQ record v1 separate from the NATS DLQ format. Replay is protected, deterministic, payload-redacted in plans,
  and uses a fresh consumer attempt generation without bypassing completed Inbox identity.
- Manage topic partitions, replication factor, minimum ISR, retention time, retention bytes, and maximum message bytes
  declaratively. Service topics have the same partition count as their source. Apply creates topics or strengthens
  managed configurations only. Partition drift requires an explicit ordering-aware migration; heterogeneous or
  incompatible replication, reduction, cleanup-policy drift, deletion, and recreation require operator action.
- Extend `gomessengerctl` under the explicit `kafka` namespace. Preserve existing unprefixed NATS commands.
- Keep broker compatibility testing local and opt-in with official Kafka 4.1.2 and 4.3.1 images; do not add a hosted
  Kafka service to CI.

## Consequences

Kafka transactions close the consumed-offset versus retry/DLQ gap, and Inbox transactions close the handler database
versus offset gap. Delivery remains at-least-once and external side effects still need idempotency.

Retry topics trade strict per-key ordering for durable delayed redelivery. Unlimited retry retention prevents scheduled
work from disappearing but requires operators to monitor backlog and storage.

The adapter owns franz-go clients and exposes only sealed connection options so it can enforce transaction,
acknowledgement, isolation, group, and subscription policy. Hosts still own broker endpoints, TLS/SASL material,
process identity, topology policy, database connections, supervision, and deployment.

Separate NATS and Kafka implementations intentionally duplicate a small amount of lifecycle/observability safety code.
That duplication is cheaper than prematurely freezing a common abstraction around two different broker contracts.
