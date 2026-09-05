// Package terminalstate implements SQL terminal-generation persistence shared by Inbox backends.
package terminalstate

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

// SQL contains namespace-rendered terminal and identity statements. Identifiers
// are validated and quoted by the backend, never supplied as message data.
type SQL struct {
	Terminal     string
	Inbox        string
	Attempts     string
	Generations  string
	LockIdentity string
}

// Record persists terminal outcomes inside the caller's Inbox transaction.
// Conflict handling preserves the original reason, count and closing timestamp.
// A newly observed terminal delivery invalidates earlier retention eligibility
// until its broker handoff is confirmed again.
func (s SQL) Record(ctx context.Context, tx *sql.Tx, outcomes []inbox.BatchItemOutcome, now time.Time) error {
	const chunkSize = 100
	terminal := make([]inbox.BatchItemOutcome, 0, len(outcomes))
	seen := make(map[terminalKey]struct{}, len(outcomes))
	for _, item := range outcomes {
		if item.Outcome == inbox.BatchDLQ &&
			(item.FailureKind == inbox.FailurePermanent || item.FailureKind == inbox.FailureAttemptsExhausted) {
			key := terminalKey{
				consumer: item.Key.ConsumerID, source: item.Key.Source,
				message: item.Key.MessageID.String(), fingerprint: inbox.AttemptFingerprint(item.Key, item.Fingerprint),
			}
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				terminal = append(terminal, item)
			}
		}
	}
	for start := 0; start < len(terminal); start += chunkSize {
		end := min(start+chunkSize, len(terminal))
		var query strings.Builder
		query.WriteString("INSERT INTO ")
		query.WriteString(s.Terminal)
		query.WriteString(" (consumer_id, source, message_id, fingerprint, attempts, failure_kind, terminal_at) VALUES ")
		arguments := make([]any, 0, (end-start)*7)
		for index, item := range terminal[start:end] {
			if index != 0 {
				query.WriteByte(',')
			}
			query.WriteByte('(')
			for field := range 7 {
				if field != 0 {
					query.WriteByte(',')
				}
				query.WriteByte('$')
				query.WriteString(strconv.Itoa(index*7 + field + 1))
			}
			query.WriteByte(')')
			fingerprint := inbox.AttemptFingerprint(item.Key, item.Fingerprint)
			arguments = append(arguments, item.Key.ConsumerID, item.Key.Source, item.Key.MessageID.String(),
				fingerprint[:], item.Attempt, item.FailureKind, now.UTC())
		}
		query.WriteString(" ON CONFLICT (consumer_id, source, message_id, fingerprint) DO UPDATE SET handoff_confirmed_at = NULL")
		if _, err := tx.ExecContext(ctx, query.String(), arguments...); err != nil {
			return fmt.Errorf("inbox: persist terminal generations: %w", err)
		}
	}
	return nil
}

// Forget removes a selected terminal marker during an explicit destructive reset.
func (s SQL) Forget(ctx context.Context, tx *sql.Tx, key inbox.Key, fingerprint inbox.Fingerprint) error {
	derived := inbox.AttemptFingerprint(key, fingerprint)
	// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
	_, err := tx.ExecContext(ctx, "DELETE FROM "+s.Terminal+
		" WHERE consumer_id=$1 AND source=$2 AND message_id=$3 AND fingerprint=$4",
		key.ConsumerID, key.Source, key.MessageID.String(), derived[:])
	if err != nil {
		return fmt.Errorf("inbox: reset terminal generation: %w", err)
	}
	return nil
}

// PruneCompleted removes markers for the same bounded completed-identity batch
// selected by the existing backend prune statements. Its identity locks are held.
func (s SQL) PruneCompleted(ctx context.Context, tx *sql.Tx, before time.Time, limit int) error {
	// #nosec G202 -- Only backend-validated, quoted SQL identifiers are interpolated; values remain parameters.
	_, err := tx.ExecContext(ctx, "DELETE FROM "+s.Terminal+` WHERE (consumer_id, source, message_id) IN (
 SELECT consumer_id, source, message_id FROM `+s.Inbox+`
 WHERE completed_at < $1 ORDER BY completed_at, consumer_id, source, message_id LIMIT $2)`, before, limit)
	if err != nil {
		return fmt.Errorf("inbox: prune completed terminal markers: %w", err)
	}
	return nil
}

type terminalKey struct {
	consumer    string
	source      string
	message     string
	fingerprint inbox.Fingerprint
}
