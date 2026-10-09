package atomicflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	outboxmigrator "github.com/assurrussa/outbox/backends/pgsql/migrator"
	outboxlogger "github.com/assurrussa/outbox/outbox/logger"
)

// Snapshot is committed database state for one incoming message and its output.
type Snapshot struct {
	BusinessRows int64 `json:"businessRows"`
	OutboxJobs   int64 `json:"outboxJobs"`
	OutboxKeys   int64 `json:"outboxKeys"`
	InboxRows    int64 `json:"inboxRows"`
	Completed    int64 `json:"completed"`
	Attempts     int64 `json:"attempts"`
}

// Report records the three checked transaction boundaries.
type Report struct {
	AfterFailure    Snapshot `json:"afterFailure"`
	AfterCommit     Snapshot `json:"afterCommit"`
	AfterRedelivery Snapshot `json:"afterRedelivery"`
	HandlerCalls    int      `json:"handlerCalls"`
}

// Migrate installs the official Outbox and Inbox migrations and the example's
// business table. The host owns db and must select a dedicated schema through
// its connection search_path before calling this function.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := outboxmigrator.RunEmbedded(ctx, db, outboxlogger.Discard(), outboxmigrator.WithCommand("up")); err != nil {
		return fmt.Errorf("migrate atomic Outbox: %w", err)
	}
	if err := inboxpgsql.Migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate atomic Inbox: %w", err)
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS atomic_order_projection (
		order_id TEXT PRIMARY KEY,
		message_id UUID NOT NULL UNIQUE,
		amount BIGINT NOT NULL CHECK (amount > 0)
	)`)
	return err
}

// Run proves rollback, commit, and post-commit/pre-ACK redelivery using the real
// PostgreSQL Inbox and Outbox backend. It does not run a broker or relay.
// The caller owns the database and migrations; proof rows are retained.
func Run(ctx context.Context, db *sql.DB) (Report, error) {
	var report Report
	message, envelope, err := newInput()
	if err != nil {
		return report, err
	}
	key := inputKey(message)
	fingerprint := inbox.FingerprintEnvelope(envelope)
	failedAfterStage := errors.New("injected failure after Outbox staging")

	// Recreate the Inbox store for every attempt, as on a process restart.
	process := func(fail bool) (inbox.Result, error) {
		store, createErr := inboxpgsql.New(db)
		if createErr != nil {
			return inbox.Result{}, createErr
		}
		return store.ProcessAttempt(ctx, key, fingerprint, 3, func(txCtx context.Context) error {
			report.HandlerCalls++
			receipt, handleErr := Handle(txCtx, message)
			if handleErr != nil {
				return handleErr
			}
			if receipt.State != messenger.ReceiptStaged {
				return fmt.Errorf("unexpected outgoing receipt state %q", receipt.State)
			}
			if fail {
				return failedAfterStage
			}
			return nil
		})
	}

	result, err := process(true)
	if !errors.Is(err, failedAfterStage) || result.Attempt != 1 || result.Duplicate {
		return report, fmt.Errorf("failed attempt: result=%+v error=%w", result, err)
	}
	report.AfterFailure, err = snapshot(ctx, db, message)
	if err != nil {
		return report, err
	}
	wantFailed := Snapshot{InboxRows: 1, Attempts: 1}
	if report.AfterFailure != wantFailed {
		return report, fmt.Errorf("failed attempt retained business/event data: %+v", report.AfterFailure)
	}

	result, err = process(false)
	if err != nil {
		return report, fmt.Errorf("retry failed: %w", err)
	}
	if result.Attempt != 2 || result.Duplicate {
		return report, fmt.Errorf("retry result: %+v", result)
	}
	report.AfterCommit, err = snapshot(ctx, db, message)
	if err != nil {
		return report, err
	}
	wantCommitted := Snapshot{BusinessRows: 1, OutboxJobs: 1, OutboxKeys: 1, InboxRows: 1, Completed: 1, Attempts: 2}
	if report.AfterCommit != wantCommitted {
		return report, fmt.Errorf("incomplete atomic commit: %+v", report.AfterCommit)
	}
	if err := verifyOutput(ctx, db, message); err != nil {
		return report, err
	}

	// Intentionally do not ACK. Deliver the exact canonical envelope again.
	// A real NATS/Kafka adapter performs its ACK/offset step only after success.
	result, err = process(false)
	if err != nil {
		return report, fmt.Errorf("redelivery failed: %w", err)
	}
	if !result.Duplicate || result.Attempt != 2 || report.HandlerCalls != 2 {
		return report, fmt.Errorf("redelivery invoked the handler: result=%+v calls=%d", result, report.HandlerCalls)
	}
	report.AfterRedelivery, err = snapshot(ctx, db, message)
	if err != nil {
		return report, err
	}
	if report.AfterRedelivery != report.AfterCommit {
		return report, fmt.Errorf("redelivery changed committed state: %+v", report.AfterRedelivery)
	}
	return report, nil
}

func newInput() (messenger.Message[OrderCreated], []byte, error) {
	id, err := messenger.UUIDv7Generator().New()
	if err != nil {
		return messenger.Message[OrderCreated]{}, nil, err
	}
	message := messenger.Message[OrderCreated]{
		Metadata: messenger.Metadata{
			ID: id, Kind: messenger.KindEvent, Name: "orders.created", SchemaVersion: 1,
			Source: inputSource, Time: time.Now().UTC(), CorrelationID: id, ContentType: "application/json",
		},
		Payload: OrderCreated{OrderID: "order-" + id.String(), Amount: 4200},
	}
	envelope, err := messenger.EncodeEventEnvelope(createdEvent(), message.Metadata, message.Payload)
	if err != nil {
		return messenger.Message[OrderCreated]{}, nil, err
	}
	// Take the same typed decode boundary as a native transport consumer.
	decoded, err := messenger.DecodeEvent(createdEvent(), envelope)
	return decoded, envelope, err
}

func inputKey(message messenger.Message[OrderCreated]) inbox.Key {
	return inbox.Key{ConsumerID: consumerID, Source: message.Metadata.Source, MessageID: message.Metadata.ID}
}

func snapshot(ctx context.Context, db *sql.DB, message messenger.Message[OrderCreated]) (Snapshot, error) {
	var state Snapshot
	err := db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM atomic_order_projection WHERE message_id = $1::text::uuid),
		(SELECT count(*) FROM jobs WHERE deduplication_key = $2),
		(SELECT count(*) FROM outbox_job_idempotency_keys WHERE deduplication_key = $2),
		(SELECT count(*) FROM gomessenger_inbox WHERE consumer_id = $3 AND source = $4 AND message_id = $1),
		(SELECT count(*) FROM gomessenger_inbox
			WHERE consumer_id = $3 AND source = $4 AND message_id = $1 AND completed_at IS NOT NULL),
		COALESCE((SELECT attempts FROM gomessenger_inbox_attempts
			WHERE consumer_id = $3 AND source = $4 AND message_id = $1), 0)`,
		message.Metadata.ID.String(), projectedID(message.Metadata).String(), consumerID, message.Metadata.Source,
	).Scan(&state.BusinessRows, &state.OutboxJobs, &state.OutboxKeys, &state.InboxRows, &state.Completed, &state.Attempts)
	if err != nil {
		return Snapshot{}, fmt.Errorf("inspect committed atomic state: %w", err)
	}
	return state, nil
}

func verifyOutput(ctx context.Context, db *sql.DB, message messenger.Message[OrderCreated]) error {
	var payload, name string
	var schema int
	if err := db.QueryRowContext(ctx, `SELECT name, schema_version, payload FROM jobs WHERE deduplication_key = $1`,
		projectedID(message.Metadata).String()).Scan(&name, &schema, &payload); err != nil {
		return fmt.Errorf("read staged canonical event: %w", err)
	}
	if name != "gomessenger.relay" || schema != 1 {
		return fmt.Errorf("unexpected relay capability: %s v%d", name, schema)
	}
	event, err := messenger.DecodeEvent(projectedEvent(), []byte(payload))
	if err != nil {
		return fmt.Errorf("decode staged output: %w", err)
	}
	if event.Metadata.ID != projectedID(message.Metadata) || event.Metadata.CausationID != message.Metadata.ID ||
		event.Metadata.CorrelationID != message.Metadata.CorrelationID || event.Metadata.Source != outputSource ||
		!event.Metadata.Time.Equal(message.Metadata.Time) ||
		event.Payload != (OrderProjected{OrderID: message.Payload.OrderID, Amount: message.Payload.Amount}) {
		return errors.New("staged output has unexpected identity, lineage, time, or payload")
	}
	return nil
}
