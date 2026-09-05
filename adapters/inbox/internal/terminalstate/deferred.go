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

// PreserveDeferred keeps first-deferred generations visible to retention without
// consuming an attempt. The caller holds the corresponding logical identity locks.
func (s SQL) PreserveDeferred(ctx context.Context, tx *sql.Tx, outcomes []inbox.BatchItemOutcome, now time.Time) error {
	for _, replay := range []bool{false, true} {
		items := make([]inbox.BatchItemOutcome, 0)
		for _, item := range outcomes {
			if item.Outcome == inbox.BatchDefer && item.Attempt == 0 && (item.Key.AttemptGeneration != "") == replay {
				items = append(items, item)
			}
		}
		table := s.Attempts
		if replay {
			table = s.Generations
		}
		const chunkSize = 100
		for start := 0; start < len(items); start += chunkSize {
			chunk := items[start:min(start+chunkSize, len(items))]
			if err := preserveDeferredChunk(ctx, tx, table, chunk, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func preserveDeferredChunk(ctx context.Context, tx *sql.Tx, table string, items []inbox.BatchItemOutcome, now time.Time) error {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString(" (consumer_id, source, message_id, fingerprint, updated_at, attempts) VALUES ")
	arguments := make([]any, 0, len(items)*5)
	for index, item := range items {
		if index != 0 {
			query.WriteByte(',')
		}
		query.WriteByte('(')
		for field := range 5 {
			if field != 0 {
				query.WriteByte(',')
			}
			query.WriteByte('$')
			query.WriteString(strconv.Itoa(index*5 + field + 1))
		}
		query.WriteString(",0)")
		fingerprint := inbox.AttemptFingerprint(item.Key, item.Fingerprint)
		arguments = append(arguments, item.Key.ConsumerID, item.Key.Source, item.Key.MessageID.String(), fingerprint[:], now.UTC())
	}
	query.WriteString(" ON CONFLICT DO NOTHING")
	if _, err := tx.ExecContext(ctx, query.String(), arguments...); err != nil {
		return fmt.Errorf("inbox: preserve deferred generations: %w", err)
	}
	return nil
}
