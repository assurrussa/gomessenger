# Kafka adapter

`github.com/assurrussa/gomessenger/adapters/kafka` is the native-envelope Kafka alternative for one-way commands and
events. It supports direct transactional publish, Outbox relay, transactional consume/offset commit, durable Inbox
attempts, retry topics, DLQ hand-off, protected replay, declarative topology, and managed lifecycle. It does not support
queries, CloudEvents modes, or arbitrary non-GoMessenger payloads.

The contract is at-least-once. Kafka transactions atomically join a consumed offset to retry or DLQ production, while
the Inbox transaction joins business database writes to completion. Neither boundary makes external effects exactly
once.

## Stable names and records

Source topics are descriptor-derived:

```text
<namespace>.<command|event>.<descriptor>.v<schema-version>
```

Use `kafka.Topic`, `RetryTopic`, `ReplayTopic`, `DLQTopic`, and `ConsumerGroup` instead of rebuilding these names. For a
source topic and stable `ConsumerID`, the adapter uses:

```text
<source>.gm.<consumer>.retry.t0
<source>.gm.<consumer>.retry.t1
...
<source>.gm.<consumer>.replay
<source>.gm.<consumer>.dlq
```

The Kafka record key is `Metadata.Key` when present and otherwise the canonical message ID. Retry and replay preserve
that key. Ordinary source records must not contain `gomessenger-*` control headers; the adapter owns them on retry and
replay records. Topic, consumer, instance, group, and transaction identifiers use bounded ASCII letters, digits, dots,
and hyphens. A namespace may contain dots, but `command` and `event` are reserved namespace segments so the
`namespace.kind.descriptor.vN` mapping remains unambiguous. The `gm` segment is reserved in source namespaces and
descriptor names so `.gm.` has exactly one source/consumer boundary. Service-name helpers validate that their source
argument is canonical instead of accepting a hand-built ambiguous topic.

`TransportConfig.InstanceID` is required and must be stable and unique for one running process replica. Each worker
derives a static group member from it. Its transactional ID is a versioned SHA-256 derivation of the complete group,
instance, and worker tuple, which preserves dotted identifier boundaries and stays below Kafka's identifier limit.
Starting two live replicas with the same instance ID fences one of them; changing it on every restart defeats static
membership.

## Declare and apply topology

Topology spec `1.0` explicitly declares partitions, replication factor, minimum in-sync replicas, retention time,
retention bytes, and maximum message bytes for every source, retry, replay, and DLQ topic. Every service topic must have
the same partition count as its source. Retry tiers must be contiguous from `t0` and use both
`retentionMillis: -1` and `retentionBytes: -1`; delayed work must not expire through time or size eviction.

Start from [the production-shaped example](examples/kafka-topology.json), then review its capacity and retention values
for the deployment. The four retry topics correspond by index to the default `RetryTiers` of 1 second, 10 seconds,
1 minute, and 5 minutes.

```sh
gomessengerctl kafka topology validate --file docs/examples/kafka-topology.json
gomessengerctl kafka topology plan \
  --file docs/examples/kafka-topology.json \
  --brokers kafka-1:9092,kafka-2:9092 \
  --instance-id ops-host-a
gomessengerctl kafka topology apply \
  --file docs/examples/kafka-topology.json \
  --brokers kafka-1:9092,kafka-2:9092 \
  --instance-id ops-host-a
```

Planning is read-only. Apply may create a missing topic or make a monotonic increase to minimum ISR, retention,
retention bytes, or maximum message bytes. Partition-count drift is a conflict because changing the count remaps keyed
records and requires an explicit operator drain/migration. Replication factor is checked across every partition;
heterogeneous assignments, cleanup-policy drift, decreases, or other unsafe drift refuse the entire plan before
mutation. Apply never deletes topics, reduces capacity, recreates a resource, or changes fields outside the managed
subset. A plan containing a conflict exits with code `3`.

## Compose transport, route, and consumer

The adapter owns franz-go clients. The host supplies seed brokers, stable identity, operation bounds, and typed
`ConnectionOption` values for TLS, SASL, dialers, dial timeouts, hooks, rack, and client logging. Hooks that receive
mutable producer or consumer records or the live client are rejected. Arbitrary `kgo.Opt` values are intentionally not
accepted.
Producer, transaction, group, subscribed topic, all-ISR acknowledgement,
auto-commit, read-committed, and record-batch size options are adapter-owned safety settings. Producer batch limits
cover the full source/retry/replay envelope, its duplicated Kafka record key, bounded control metadata, and the larger
DLQ bound.

```go
transport, err := kafkaadapter.NewTransport(kafkaadapter.TransportConfig{
    Name:       "kafka.orders",
    Brokers:    []string{"kafka-1:9092", "kafka-2:9092"},
    ClientID:   "billing-service",
    InstanceID: hostInstanceID,
    Logger:     messenger.AdaptSlog(slog.Default()),
    ConnectionOptions: []kafkaadapter.ConnectionOption{
        kafkaadapter.DialTLSConfig(tlsConfig),
        kafkaadapter.SASL(saslMechanism),
    },
})
if err != nil {
    return err
}

route, err := kafkaadapter.NewRoute(transport, kafkaadapter.RouteConfig{
    Name:      "kafka.orders",
    Namespace: "prod",
})
if err != nil {
    return err
}

consumer, err := kafkaadapter.NewEventConsumer(
    transport,
    inboxStore,
    contracts.OrderCreated,
    handleOrderCreated,
    kafkaadapter.HandlerConfig{
        Namespace:   "prod",
        ConsumerID:  "billing-order-projector",
        Concurrency: 8,
        Timeout:     30 * time.Second,
        MaxAttempts: 5,
    },
)
if err != nil {
    return err
}

builder := messenger.NewBuilder(messenger.WithSource("urn:service:billing"))
builder.Use(transport.Name(), transport)
builder.RouteEvent(contracts.OrderCreated, route)
builder.Use("consumer.billing-order-projector", consumer)
bus, runtime, err := builder.Build()
```

Register the transport explicitly even in consumer-only processes. Route registration contributes the same transport
automatically, and Builder safely deduplicates that identical service instance.

`TransportConfig.Logger` is the adapter-owned infrastructure channel. It reports transport startup/readiness,
producer and consumer transaction failures, abort/fencing outcomes, topology failures or applied changes, and retry
partition deferrals. Its attributes are limited to transport, consumer, route, topic, partition, deadline, operation,
action, counts, and errors; record keys, payloads, message bodies, and headers are never logged. `WithClientLogger` is a
separate opt-in to franz-go's internal logger and follows franz-go's own logging contract.

`NewCommandConsumer` has the same lifecycle and retry contract. The Inbox backend must support durable attempts and the
handler must use `inbox.SQLTxFromContext` for database writes that need atomic Inbox completion.

`NewBatchCommandConsumer` and `NewBatchEventConsumer` select the separate true-batch path. They take the same
`HandlerConfig` plus `messenger.BatchConfig`; single-message middleware is rejected. Each invocation contains only an
ascending contiguous range from one concrete topic-partition and uses one Inbox/business transaction. The result must
cover every handler message exactly once by source and message ID. One Kafka transaction then publishes all retry/DLQ
records and commits the range offset. Polled records outside that range are rewound. See
[ADR-0005](decisions/0005-batch-consumer.md) for partial-outcome and top-level retry semantics.

For a transactional producer database path, give the same Kafka route to `outboxadapter.NewRelayJob`; the relay sends
the persisted canonical bytes without rebuilding identity or timestamps. A direct route receipt becomes
`broker_confirmed` only after its Kafka transaction commits. The immutable logical creation time stays in the envelope;
source and replay-ingress record timestamps represent producer publication time so delayed Outbox records do not enter
source retention as already-old records.

## Retry, ordering, and terminal hand-off

One worker is one Kafka group member and processes either one record or one configured same-partition batch at a time.
On success, the consumed offset commits in a Kafka transaction. On a retryable failure, the same canonical bytes and key move to the selected retry topic with an
exact `not-before`, atomically with the source offset. Workers use franz-go's blocked-rebalance poll mode only for a
bounded preflight: adapter-owned control-header parsing plus native envelope structure, descriptor, record-key, and
expiry validation, followed by any exact pause/rewind. This bounded work never calls the application codec. A retry is
scheduled only when its exact `not-before` precedes `ExpiresAt`; an expired or impossible window proceeds to terminal
hand-off instead of pausing the partition. If a valid retry record arrives early, the worker snapshots existing
partition pauses, pauses only that topic-partition, rewinds the local and uncommitted cursor to the record's exact leader
epoch and offset, verifies the rewind, and then allows rebalancing. A failed verification terminates the worker without
processing later offsets. The worker retains only the topic-partition and deadline in a bounded scheduler, continues
polling other partitions, and resumes only pauses it owns when the nearest deadline is due. Canonicalization,
application-supplied `Codec.Decode`, handler, Inbox, and Kafka transaction execution start only after `AllowRebalance`.
Invalid control or envelope metadata and expired records enter transactional DLQ hand-off only after that release. A
valid early retry does not invoke its codec until it is fetched again. `MaxAttempts` counts application handler
invocations, not broker deliveries or `NotBefore` deferrals.

On permanent failure or exhaustion, a bounded Kafka DLQ v1 record and the consumed offset commit in the same
transaction. The DLQ record retains the original source position, key, canonical bytes, consumer, attempt generation,
failure class, bounded error, and failure time. CLI inspection and replay planning never print the original bytes or
handler error.

Ordering is preserved within each concrete topic-partition, including while an early retry record is deferred. Once a
failed record is moved to a different retry topic, later source records may overtake it. Choose handlers and domain
keys with that boundary in mind; this adapter does not claim strict per-key ordering across topics.

## Inspect and replay DLQ records

Save the DLQ record value as JSON, then use:

```sh
gomessengerctl kafka dlq inspect --file record.json
gomessengerctl kafka dlq replay --file record.json
gomessengerctl kafka dlq replay \
  --file record.json \
  --confirm \
  --brokers kafka-1:9092,kafka-2:9092 \
  --instance-id ops-host-a
```

Replay is offline unless `--confirm` is present. Confirmed replay validates the original canonical envelope, source
topic, record key, and deterministic consumer replay topic, then commits one record with a DLQ-specific attempt
generation. It never deletes or edits the DLQ record. A completed Inbox identity still suppresses a second business
effect; an incomplete terminal identity receives one fresh bounded attempt cycle.

The CLI supports system-root TLS, optional CA/mTLS files, and PLAIN or SCRAM SASL. Put the SASL password in
`GOMESSENGER_KAFKA_SASL_PASSWORD` or name another environment variable with `--sasl-password-env`; do not put passwords
on the command line.

## Lifecycle and verification

Attach the transport, route, and consumers to one `messenger.Runtime`. Producer-transaction admission is serialized and remains
cancellable by the caller context while waiting; after admission, bounded transaction finalization is runtime-owned.
`BeginDrain` rejects new route delivery and stops new polls; `Shutdown` waits for in-flight handler and Kafka
transaction finalization until the host deadline. Before starting any worker, `Consumer.Run` verifies required topic
presence, equal partition counts, and unlimited retry retention; topology drift fails startup without polling records.
Readiness repeats those checks with broker connectivity. Consumer rebalance timeout follows the bounded broker
finalization window rather than handler duration. Run topology plan separately to verify every managed field.

`make check` compiles, vets, lints, and tests the Kafka module without requiring Docker. The same script is the local
compatibility entry point, while hosted CI runs one independent matrix job per supported Kafka version:

```sh
make test-kafka
```

It runs the transactional direct/Outbox/Inbox/retry/DLQ/replay pipeline and the partial-outcome batch pipeline against
Kafka 4.1.2 and 4.3.1. Passing it proves
the tested checkout and scenarios, not production capacity, multi-broker failover, deployment, or a live smoke.

The design rationale and deliberately separate NATS/Kafka implementations are recorded in
[ADR-0004](decisions/0004-kafka-adapter.md).
