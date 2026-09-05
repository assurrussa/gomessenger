package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/adapters/inbox/internal/terminalstate"
)

func (b *backend) terminalSQL() terminalstate.SQL {
	return terminalstate.SQL{
		Terminal: b.names.terminal, Inbox: b.names.inbox,
		Attempts: b.names.attempts, Generations: b.names.attemptGenerations, LockIdentity: b.statements.readIdentity,
	}
}

func (b *backend) recordTerminal(ctx context.Context, tx *sql.Tx, key inbox.Key,
	fingerprint inbox.Fingerprint, attempt uint64, permanent bool,
) error {
	kind := inbox.FailureAttemptsExhausted
	if permanent {
		kind = inbox.FailurePermanent
	}
	return b.terminalSQL().Record(ctx, tx, []inbox.BatchItemOutcome{{
		Key: key, Fingerprint: fingerprint,
		Attempt: attempt, Outcome: inbox.BatchDLQ, FailureKind: kind,
	}}, b.clock().UTC())
}

func (b *backend) ConfirmTerminalHandoff(ctx context.Context, key inbox.Key, fingerprint inbox.Fingerprint) error {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("inbox/sqlite: begin handoff confirmation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := b.terminalSQL().Confirm(ctx, tx, key, fingerprint, b.clock().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("inbox/sqlite: commit handoff confirmation: %w", err)
	}
	return nil
}

func (b *backend) PruneTerminalAttempts(ctx context.Context, before time.Time, limit int) (int64, error) {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, fmt.Errorf("inbox/sqlite: begin terminal prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	removed, err := b.terminalSQL().Prune(ctx, tx, before, limit)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: commit terminal prune: %w", err)
	}
	return removed, nil
}

var _ inbox.TerminalRetentionBackend = (*backend)(nil)
