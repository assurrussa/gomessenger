# ADR-0003: Distributed query request/reply contract

- Status: accepted design boundary; not implemented
- Date: 2026-08-24

## Context

Local `Query[Q,R]` has one process, one handler, and one caller context. Crossing a process boundary introduces
availability, correlation, cancellation, error safety, result serialization, response limits, and consistency choices
that one-way `Delivery`, `Receipt`, Outbox, Inbox, JetStream retry, and DLQ do not define.

## Decision

Distributed queries will use a separate versioned request/reply contract and adapter API. They will not change or
silently remote the local `Messenger.Query` method.

### Request and result envelope

The request carries its own ID, query name/version, correlation and causation IDs, caller identity, an absolute
deadline, bounded trace/application headers, and request bytes encoded by the query request codec.

The result envelope has its own version and ID and includes `requestId`, correlation and causation IDs, query
name/version, responder identity, and exactly one of `result` or `error`. A future distributed binding must declare an
explicit result codec and result schema/version; the local compile-time `R` identity is not a wire codec.

The complete encoded request or response is at most 1 MiB. Each envelope permits at most 64 headers and 16 KiB of
aggregate header key/value data. An oversized request is rejected before handler execution; an oversized response is a
bounded `resource_exhausted` error.

### Remote errors

A remote error contains one stable code from:

- `invalid_argument`
- `not_found`
- `conflict`
- `permission_denied`
- `resource_exhausted`
- `unavailable`
- `deadline_exceeded`
- `canceled`
- `internal`

Its message and structured details are safe and size-bounded. It may include a bounded `retryAfter`. Go error values,
wrapped implementation text, panic values, and stack traces are never serialized.

### Deadline, cancellation, and availability

The client sends one absolute deadline. Client cancellation stops local waiting and asks the transport to cancel on a
best-effort basis; it does not promise that a handler which already started has stopped or rolled back.

There is exactly one logical responder. No responder produces `unavailable` or deadline expiry. Multiple responders
without a queue group or one authoritative endpoint are a topology conflict, not a race whose first reply wins.

### Retry and consistency

Automatic retries are disabled. A future adapter may enable them only for a query explicitly declared idempotent,
inside the original absolute deadline, and with a declared consistency and maximum-staleness policy. `retryAfter` is
advice, not permission to exceed those bounds. A timeout never proves that the remote handler did not execute.

### Candidate transports

HTTP, gRPC, and core NATS request/reply remain candidates. JetStream durable messaging is not a query transport: it
does not provide the required online responder and deadline semantics, and Outbox/Inbox/DLQ are not reused to simulate
them.

## Completion boundary

Distributed queries remain unimplemented until one adapter contract and a clean consumer compile, and end-to-end tests
prove result decoding, absolute deadlines, best-effort cancellation, safe remote error mapping, zero/one/multiple
responder behavior, retry and staleness policy, and request/response/header size bounds.
