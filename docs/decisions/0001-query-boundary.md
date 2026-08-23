# ADR-0001: Keep queries outside the durable messenger contract

- Status: accepted for `v0.1.0`; distributed queries deferred
- Date: 2026-08-24

## Context

GoBus already provides type-safe in-process command, query/result, and event dispatch. GoMessenger extends commands and
events across local routes, transactional Outbox staging, JetStream delivery, durable Inbox handling, retry, and DLQ.
The current GoMessenger API has no query kind, query descriptor, typed response envelope, or request/reply route.

Queries have different lifecycle semantics from one-way durable messages. A remote query must define response typing,
correlation, deadline and cancellation propagation, remote error mapping, responder availability, response-size bounds,
and whether retries are safe. Outbox, Inbox, delivery receipts, and DLQ do not answer those questions.

## Decision

- GoMessenger `v0.1.0` supports commands and events only.
- In-process queries continue to use GoBus `RegisterResult` and `DispatchResult` directly.
- Queries do not use the existing durable `Route`, `Receipt`, Outbox, Inbox, retry, or DLQ contracts.
- A cross-process query API will be designed separately only after a concrete service use case supplies latency,
  availability, cancellation, response, and retry requirements.
- HTTP, gRPC, and NATS request/reply remain implementation options; none is selected by this decision.

## Consequences

- The durable core retains one-way at-least-once semantics without pretending that a staged or broker-confirmed request
  is a completed query.
- Applications that need local queries inject GoBus alongside GoMessenger.
- GoMessenger does not yet provide one facade for the complete local CQRS command/query/event surface.
- Adding distributed queries later is a public contract change and requires its own ADR, consumer probe, failure matrix,
  and compatibility plan.

## Revisit criteria

Revisit this decision when at least one real service needs a cross-process read and can state:

- the required latency and availability objective;
- deadline and cancellation behavior;
- response and error-envelope bounds;
- whether retry can return stale or inconsistent data;
- whether HTTP, gRPC, or NATS request/reply is the operationally owned transport.
