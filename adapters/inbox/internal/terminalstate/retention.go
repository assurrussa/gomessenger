package terminalstate

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

// Confirm records a completed broker handoff under the logical identity lock.
func (s SQL) Confirm(ctx context.Context, tx *sql.Tx, key inbox.Key, fingerprint inbox.Fingerprint, now time.Time) error {
	var stored []byte
	var completed sql.NullTime
	err := tx.QueryRowContext(ctx, s.LockIdentity, key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return inbox.ErrTerminalStateMissing
	}
	if err != nil {
		return fmt.Errorf("inbox: lock terminal handoff: %w", err)
	}
	if subtle.ConstantTimeCompare(stored, fingerprint[:]) != 1 {
		return inbox.ErrFingerprintConflict
	}
	derived := inbox.AttemptFingerprint(key, fingerprint)
	// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
	result, err := tx.ExecContext(ctx, "UPDATE "+s.Terminal+`
 SET handoff_confirmed_at = CASE WHEN handoff_confirmed_at > $1 THEN handoff_confirmed_at ELSE $1 END
 WHERE consumer_id=$2 AND source=$3 AND message_id=$4 AND fingerprint=$5`,
		now.UTC(), key.ConsumerID, key.Source, key.MessageID.String(), derived[:])
	if err != nil {
		return fmt.Errorf("inbox: confirm terminal handoff: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inbox: confirmed terminal rows: %w", err)
	}
	if rows == 0 {
		return inbox.ErrTerminalStateMissing
	}
	return nil
}

type candidate struct {
	consumer    string
	source      string
	message     string
	fingerprint []byte
}

// Prune selects a bounded batch and serializes its deletions with processing
// and confirmation. Eligibility is checked again after each identity lock.
func (s SQL) Prune(ctx context.Context, tx *sql.Tx, before time.Time, limit int) (int64, error) {
	candidates, err := s.candidates(ctx, tx, before, limit)
	if err != nil {
		return 0, err
	}
	var removed int64
	for _, item := range candidates {
		count, err := s.pruneCandidate(ctx, tx, item, before)
		if err != nil {
			return 0, err
		}
		removed += count
	}
	return removed, nil
}

func (s SQL) candidates(ctx context.Context, tx *sql.Tx, before time.Time, limit int) ([]candidate, error) {
	// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
	rows, err := tx.QueryContext(ctx, `SELECT consumer_id, source, message_id, fingerprint FROM (
 SELECT terminal.consumer_id, terminal.source, terminal.message_id, terminal.fingerprint
 FROM `+s.Terminal+` AS terminal JOIN `+s.Inbox+` AS identity
 ON identity.consumer_id=terminal.consumer_id AND identity.source=terminal.source AND identity.message_id=terminal.message_id
 WHERE terminal.handoff_confirmed_at < $1 AND identity.completed_at IS NULL
 ORDER BY terminal.handoff_confirmed_at, terminal.consumer_id, terminal.source, terminal.message_id, terminal.fingerprint
 LIMIT $2) AS candidates ORDER BY consumer_id, source, message_id, fingerprint`, before, limit)
	if err != nil {
		return nil, fmt.Errorf("inbox: select terminal retention batch: %w", err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]candidate, 0, limit)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.consumer, &item.source, &item.message, &item.fingerprint); err != nil {
			return nil, fmt.Errorf("inbox: scan terminal retention batch: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inbox: read terminal retention batch: %w", err)
	}
	return items, nil
}

func (s SQL) pruneCandidate(ctx context.Context, tx *sql.Tx, item candidate, before time.Time) (int64, error) {
	var fingerprint []byte
	var completed sql.NullTime
	err := tx.QueryRowContext(ctx, s.LockIdentity, item.consumer, item.source, item.message).Scan(&fingerprint, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("inbox: lock terminal retention identity: %w", err)
	}
	if completed.Valid {
		return 0, nil
	}
	// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
	result, err := tx.ExecContext(ctx, "DELETE FROM "+s.Terminal+`
 WHERE consumer_id=$1 AND source=$2 AND message_id=$3 AND fingerprint=$4 AND handoff_confirmed_at < $5`,
		item.consumer, item.source, item.message, item.fingerprint, before)
	if err != nil {
		return 0, fmt.Errorf("inbox: prune terminal generation: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inbox: pruned terminal rows: %w", err)
	}
	if removed == 0 {
		return 0, nil
	}
	for _, table := range []string{s.Attempts, s.Generations} {
		// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+
			" WHERE consumer_id=$1 AND source=$2 AND message_id=$3 AND fingerprint=$4",
			item.consumer, item.source, item.message, item.fingerprint); err != nil {
			return 0, fmt.Errorf("inbox: prune terminal counter: %w", err)
		}
	}
	var query strings.Builder
	query.WriteString("DELETE FROM ")
	query.WriteString(s.Inbox)
	query.WriteString(" WHERE consumer_id=$1 AND source=$2 AND message_id=$3 AND completed_at IS NULL")
	for _, table := range []string{s.Attempts, s.Generations, s.Terminal} {
		query.WriteString(" AND NOT EXISTS (SELECT 1 FROM ")
		query.WriteString(table)
		query.WriteString(" WHERE consumer_id=$1 AND source=$2 AND message_id=$3)")
	}
	if _, err := tx.ExecContext(ctx, query.String(), item.consumer, item.source, item.message); err != nil {
		return 0, fmt.Errorf("inbox: prune empty terminal identity: %w", err)
	}
	return removed, nil
}
