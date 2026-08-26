# Durable PostgreSQL + NATS demo

This checkout-level example makes GoMessenger's transaction and delivery boundaries visible in one run:

```text
PostgreSQL business transaction
  -> Outbox row
  -> relay
  -> JetStream PubAck
  -> PostgreSQL Inbox transaction
       |-- business projection
       `-- completion marker
  -> ACK, retry, or confirmed DLQ
```

It proves the following controlled scenarios:

1. a business `orders` insert and canonical event are staged in one PostgreSQL transaction;
2. the first consumer attempt writes a projection and returns `RetryAfter`; that write rolls back, then the second
   attempt commits once;
3. a second broker delivery with the same envelope identity reaches the consumer but the Inbox suppresses the handler;
4. a permanent handler failure rolls back its projection and reaches the DLQ;
5. confirmed replay starts a fresh bounded attempt generation and commits the projection;
6. repeating the same replay uses deterministic JetStream deduplication.

## Run

Requirements: Docker with Compose v2. From the repository root:

```sh
make demo-durable-postgres-nats
```

The target is equivalent to:

```sh
docker compose -f examples/durable-postgres-nats/compose.yaml \
  up --build --abort-on-container-exit --exit-code-from demo
```

A successful run ends with `durable demo passed`. Remove the stopped containers afterward:

```sh
make demo-durable-postgres-nats-down
```

The compose stack uses a dedicated PostgreSQL 18 container and a single-node NATS development topology. The app creates
the `demo` schema, applies embedded Outbox and namespaced Inbox migrations, and creates two demo business tables. It does
not drop or truncate data. Do not point this example at a shared database without reviewing those additive migrations.

## Scope

The example module uses local `replace` directives because it deliberately proves the current checkout, which may be
ahead of the published `v0.1.0` modules. It is compiled by `make check`; it does not prove published-module resolution.
Single-node NATS, development stream defaults, one consumer worker, and a short run do not establish production
capacity, failover, or operational readiness. The separate [release process](../../docs/release.md) and
[real-service pilot](../../docs/decisions/0002-real-project-pilot.md) remain required.
