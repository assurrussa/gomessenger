# ADR-0001: Include typed local queries and keep distributed request/reply separate

- Status: accepted for `v0.1.0`; distributed queries deferred
- Date: 2026-08-24

## Context

GoMessenger already provides one typed facade for commands and events while GoBus supplies the underlying local
dispatch engine. A complete local CQRS surface also needs request/reply without forcing applications to inject a second
bus API. Local query execution can reuse GoBus result dispatch without inheriting the durability semantics of a
cross-process query.

Remote queries have a different contract. They need a result envelope and result codec, responder availability,
deadlines and cancellation, bounded remote errors and responses, and an explicit consistency/retry policy. A staged
Outbox row, JetStream `PubAck`, Inbox record, receipt, retry, or DLQ does not mean that a query result exists.

## Decision

- GoMessenger `v0.1.0` includes `Query[Q,R]`, `Querier[Q,R]`, one typed handler, and one required local route.
- The query descriptor's codec, schema, content type, and encoding describe only request `Q`. Result `R` is part of the
  in-process compile-time identity and is not written to the manifest or a wire name.
- `LocalSyncRoute` delegates to GoBus `RegisterResult`/`DispatchResult`. `LocalAsyncRoute` delegates to bounded
  `SubmitResult`, retains caller cancellation for admission, execution, and waiting, and still exposes one synchronous
  `Messenger.Query` request/reply method.
- Local queries receive generated local metadata, lineage, trace propagation, global middleware, typed query
  middleware, handler/query observations, panic isolation, and runtime lifecycle behavior.
- A local query never becomes `Delivery`, never serializes a native envelope, and never uses `Receipt`, Outbox, NATS,
  Inbox, retry, DLQ, or replay.
- The manifest remains spec `1.0`: it records the request descriptor, required local route, and exactly one handler ID.
- Distributed request/reply is a separate future API governed by [ADR-0003](0003-distributed-queries.md). It does not
  change the local `Query` method.

## Consequences

- Applications can inject `Sender`, `Querier`, and `Publisher` from one CQRS facade while keeping local reads
  type-safe.
- Global middleware cannot manufacture `R`. A successful short-circuit or swallowed handler error without a result is
  `ErrQueryResultMissing`; typed query middleware may deliberately return a cached or synthetic result.
- Query result values, request payloads, and arbitrary headers never enter core observations or logs.
- `KindQuery` is valid for descriptors, metadata, and manifests, but invalid in the native envelope and every durable
  transport adapter.
- Distributed queries remain unimplemented until their own adapter contract, clean-consumer probe, and failure-focused
  end-to-end suite pass.
