# Durable pipeline E2E

The checkout-level E2E module proves the complete durable path through public
GoMessenger and Outbox APIs:

```text
producer business transaction
  -> canonical envelope staged in Outbox
  -> real Outbox worker
  -> JetStream PubAck
  -> durable pull consumer
  -> Inbox and business transaction
  -> DoubleAck, retry, or confirmed DLQ
```

Run it from the repository root:

```sh
make test-e2e
```

The harness is deterministic and Docker-free. It starts an embedded JetStream
server, uses one file-backed SQLite database for producer state and Outbox, and
another for Inbox and consumer business state. Relay and consumer connections
are separate so a consumer connection can be closed after the Inbox commit but
before `DoubleAck` without interrupting the producer path.

## Scenarios

- producer rollback removes both the business write and staged envelope;
- committed Outbox work is published with its original message identity;
- a connection loss between Inbox commit and broker ACK causes redelivery, but
  the completed Inbox identity prevents a second business write, while W3C
  trace headers survive the Outbox and redelivery path;
- `RetryAfter` rolls back the first Inbox transaction, waits, redelivers, and
  commits exactly once;
- a permanent handler error rolls back business state, publishes a validated
  DLQ record, and then terminates the original message;
- offline DLQ replay makes no broker connection and exposes no wire data;
- confirmed replay waits for JetStream `PubAck`, uses deterministic broker
  deduplication, starts a fresh bounded attempt generation after terminal
  hand-off even when post-ACK cleanup fails, and a completed identity still
  suppresses a distinct broker delivery;
- native and CloudEvents structured/binary messages preserve trace context and
  global middleware order through a real embedded JetStream consumer;
- bounded shutdown makes the consumer unready, leaves unfinished work
  unacknowledged, and lets a replacement consumer commit it.

The module lives under `testdata/e2e`, runs with `GOWORK=off`, and uses explicit
local replacements only for the GoMessenger modules under test. Its Outbox root
and SQLite backend resolve as published `v0.11.0` modules. It is not a published
module. `make test-e2e` proves the local checkout;
`make test-consumer-release VERSION=vX.Y.Z` remains the separate proof that all
public GoMessenger tags resolve without local replacements.
