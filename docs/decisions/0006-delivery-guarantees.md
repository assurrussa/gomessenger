# ADR-0006: Terminal delivery protection and execution expiry

## Status

Accepted on 2026-09-05. Dependency updates and publication are separate work.

## Context

Replay validation previously prevented malformed NATS messages from entering DLQ. With a saturated `MaxAckPending`, one
such message could block subsequent deliveries. Automatic post-handoff Inbox deletion reopened a terminal generation
for a delivery already fetched by another worker. Local asynchronous admission also allowed expiry to pass while a
message waited for its worker, without checking expiry at execution.

## Decision

NATS single and batch consumers share bounded DLQ preparation. Replayable captures keep v1 and its existing digest and
replay identities. Captures that exceed replay header rules or DLQ capacity use non-replayable quarantine v2 in the same
DLQ subject. Quarantine records the rejection reason, original sizes and a SHA-256 digest of the exact source identity.
It stores complete original headers and payload when losslessly representable within the existing limit; otherwise it
omits both and explicitly marks the omission. Source ACK follows DLQ `PubAck`. Transient handoff failures retain
heartbeats and bounded shutdown; inability to prepare the minimal record is a fatal consumer lifecycle error.

PostgreSQL and SQLite add a terminal table keyed by logical identity and `AttemptFingerprint`. Permanent and exhausted
generations close atomically with attempt accounting. Restart or a larger `MaxAttempts` cannot reopen them. A new replay
generation has an independent budget, while a completed identity suppresses all generations.

`TerminalRetentionBackend` is optional. Consumers confirm terminal handoff after NATS ACK confirmation or Kafka
transaction commit. A newly observed terminal delivery clears prior eligibility; confirmation failure leaves protection
in place. No consumer calls `ForgetAttempt` automatically. That deprecated method remains an explicit destructive reset.

`PruneTerminalAttempts(ctx, before, limit)` removes at most `1..10000` eligible generations per transaction. It requires
confirmed handoff older than the cutoff, rechecks eligibility under identity locks, and preserves active generations and
necessary logical identities. Completed identities remain subject to the existing `Prune` contract, including removal
of their terminal records in the same transaction. No background retention or default TTL is installed. The host owns a
safe cutoff that accounts for broker retention and delayed deliveries. Explicit deletion ends generation protection.

The local executor checks expiry immediately before invoking a delivery. An expired command/event skips middleware and
handler and produces `Permanent(ErrMessageExpired)` and `OperationExpire` through the configured observer. Admission
receipts, caller-context detachment and drain behavior stay intact. Already started delivery execution is not interrupted
by message expiry. The public `Delivery` interface does not gain a method.

## Deployment and compatibility

1. Install the CLI that reads both DLQ versions and explicitly rejects quarantine replay, including offline planning.
2. Apply PostgreSQL/SQLite additive migrations with the host's existing schema/prefix options. Reapplication is idempotent.
3. Drain and stop all old consumers that can still delete terminal attempt state.
4. Start the new consumers. Confirm readiness and observe terminal handoff errors through existing logs/observers.
5. Enable terminal retention separately, only after the host selects its safe cutoff and scheduling policy.

An additive schema alone does not make mixed old/new consumers safe: old binaries can still erase attempt counters.
Historical permanent rows migrate without handoff confirmation. Historical exhausted counters do not encode their old
limit; they close when next observed at the configured limit. Previously deleted history cannot be restored. Migration
does not infer ACK success from age. A full quarantine record remains diagnostic evidence, never a replay candidate.

The controlled regression and broker gates prove these contracts, not operational production readiness. Real-service
pilot acceptance remains governed by [ADR-0002](0002-real-project-pilot.md).
