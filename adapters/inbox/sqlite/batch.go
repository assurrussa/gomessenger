package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/internal/batchruntime"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

const sqliteBatchStatementItems = 100

type sqliteBatchPreparedItem struct {
	index int
	item  inbox.BatchItem
}

type sqliteBatchGroup struct {
	key      messenger.BatchItemKey
	entries  []sqliteBatchPreparedItem
	matching []sqliteBatchPreparedItem
	state    sqliteBatchAttemptState
	first    int
}

type sqliteBatchAttemptState struct {
	kind     string
	attempt  uint64
	terminal bool
}

type sqliteBatchClassified struct {
	kind  batchruntime.FailureKind
	delay time.Duration
	err   error
}

// ProcessBatchAttempt executes one partial-outcome batch in one SQLite transaction.
func (b *backend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	maxAttempts uint64,
	handler inbox.BatchHandler,
) (report inbox.BatchProcessResult, processErr error) {
	if len(items) == 0 {
		return report, nil
	}
	groups, err := prepareSQLiteBatchGroups(items)
	if err != nil {
		return report, err
	}
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return report, fmt.Errorf("inbox/sqlite: begin batch transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	report.Items = make([]inbox.BatchItemOutcome, len(items))
	lockOrder := append([]*sqliteBatchGroup(nil), groups...)
	sort.Slice(lockOrder, func(i, j int) bool {
		if lockOrder[i].key.Source != lockOrder[j].key.Source {
			return lockOrder[i].key.Source < lockOrder[j].key.Source
		}
		return lockOrder[i].key.MessageID.String() < lockOrder[j].key.MessageID.String()
	})
	active, err := b.prepareSQLiteBatchIdentities(ctx, tx, lockOrder, report.Items, maxAttempts)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	if len(active) == 0 {
		return b.commitTerminalBatch(ctx, tx, report)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].first < active[j].first })
	handlerItems := make([]inbox.BatchItem, len(active))
	expected := make([]messenger.BatchItemKey, len(active))
	for index, group := range active {
		handlerItems[index] = group.matching[0].item
		expected[index] = group.key
	}
	report.HandlerMessages = len(handlerItems)
	result, handlerErr := handler(inbox.ContextWithSQLTx(ctx, tx), handlerItems)
	if handlerErr != nil {
		if len(result.Items) != 0 || batchruntime.IsFailClosed(handlerErr) {
			return inbox.BatchProcessResult{}, fmt.Errorf(
				"%w: invalid top-level batch failure: %w", messenger.ErrInvalidBatchResult, handlerErr,
			)
		}
		return inbox.BatchProcessResult{}, handlerErr
	}
	itemErrors, err := batchruntime.ValidateResult(expected, result)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}

	classified := make([]sqliteBatchClassified, len(active))
	consuming := make([]*sqliteBatchGroup, 0, len(active))
	for index, group := range active {
		kind, delay := batchruntime.Classify(itemErrors[index])
		classified[index] = sqliteBatchClassified{kind: kind, delay: delay, err: itemErrors[index]}
		if kind != batchruntime.FailureDefer {
			consuming = append(consuming, group)
		}
	}
	attempts, err := b.incrementSQLiteBatchAttempts(ctx, tx, consuming)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	successful := make([]*sqliteBatchGroup, 0, len(active))
	permanent := make([]*sqliteBatchGroup, 0, len(active))
	for index, group := range active {
		item := classified[index]
		if item.kind == batchruntime.FailureDefer {
			assignSQLiteBatchOutcome(report.Items, group, inbox.BatchItemOutcome{
				Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchDefer,
				Attempt: group.state.attempt, Delay: item.delay, Err: item.err,
			})
			continue
		}
		attempt := attempts[group.key]
		outcome := inbox.BatchItemOutcome{
			Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchRetry,
			Attempt: attempt, Delay: item.delay, Err: item.err,
		}
		switch item.kind {
		case batchruntime.FailureSuccess:
			successful = append(successful, group)
			outcome.Outcome = inbox.BatchACK
		case batchruntime.FailurePermanent:
			permanent = append(permanent, group)
			outcome.Outcome = inbox.BatchDLQ
			outcome.FailureKind = inbox.FailurePermanent
			outcome.Delay = 0
		case batchruntime.FailureRetryAfter, batchruntime.FailureOrdinary:
			if attempt >= maxAttempts {
				outcome.Outcome = inbox.BatchDLQ
				outcome.FailureKind = inbox.FailureAttemptsExhausted
				outcome.Delay = 0
			}
		case batchruntime.FailureDefer:
			panic("unreachable defer classification")
		}
		assignSQLiteBatchOutcome(report.Items, group, outcome)
	}
	if err := b.markSQLiteBatchCompleted(ctx, tx, successful); err != nil {
		return inbox.BatchProcessResult{}, err
	}
	if err := b.markSQLiteBatchTerminal(ctx, tx, permanent); err != nil {
		return inbox.BatchProcessResult{}, err
	}
	return b.commitTerminalBatch(ctx, tx, report)
}

func (b *backend) commitTerminalBatch(ctx context.Context, tx *sql.Tx,
	report inbox.BatchProcessResult,
) (inbox.BatchProcessResult, error) {
	if err := b.terminalSQL().PreserveDeferred(ctx, tx, report.Items, b.clock().UTC()); err != nil {
		return inbox.BatchProcessResult{}, err
	}
	if err := b.terminalSQL().Record(ctx, tx, report.Items, b.clock().UTC()); err != nil {
		return inbox.BatchProcessResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return inbox.BatchProcessResult{}, fmt.Errorf("inbox/sqlite: commit batch transaction: %w", err)
	}
	return report, nil
}

func prepareSQLiteBatchGroups(items []inbox.BatchItem) ([]*sqliteBatchGroup, error) {
	byKey := make(map[messenger.BatchItemKey]*sqliteBatchGroup, len(items))
	groups := make([]*sqliteBatchGroup, 0, len(items))
	for index, item := range items {
		key := messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
		group, exists := byKey[key]
		if !exists {
			group = &sqliteBatchGroup{key: key, first: index}
			byKey[key] = group
			groups = append(groups, group)
		} else if group.entries[0].item.Key.AttemptGeneration != item.Key.AttemptGeneration {
			return nil, fmt.Errorf("%w: mixed attempt generations for %s/%s",
				messenger.ErrInvalidBatchResult, key.Source, key.MessageID)
		}
		group.entries = append(group.entries, sqliteBatchPreparedItem{index: index, item: item})
	}
	return groups, nil
}

func (b *backend) prepareSQLiteBatchIdentities(
	ctx context.Context,
	tx *sql.Tx,
	groups []*sqliteBatchGroup,
	outcomes []inbox.BatchItemOutcome,
	maxAttempts uint64,
) ([]*sqliteBatchGroup, error) {
	if err := b.insertSQLiteBatchIdentities(ctx, tx, groups); err != nil {
		return nil, err
	}
	active := make([]*sqliteBatchGroup, 0, len(groups))
	for _, group := range groups {
		representative := group.entries[0].item
		var stored []byte
		var completedAt sql.NullTime
		if err := tx.QueryRowContext(ctx, b.statements.readIdentity,
			representative.Key.ConsumerID, group.key.Source, group.key.MessageID.String(),
		).Scan(&stored, &completedAt); err != nil {
			return nil, fmt.Errorf("inbox/sqlite: read batch identity: %w", err)
		}
		if len(stored) != len(inbox.Fingerprint{}) {
			return nil, errors.New("inbox/sqlite: invalid stored batch identity")
		}
		for _, entry := range group.entries {
			if bytes.Equal(stored, entry.item.Fingerprint[:]) {
				group.matching = append(group.matching, entry)
				continue
			}
			outcomes[entry.index] = inbox.BatchItemOutcome{
				Key: entry.item.Key, Fingerprint: entry.item.Fingerprint, Outcome: inbox.BatchDLQ,
				FailureKind: inbox.FailureIdentityConflict, Err: inbox.ErrFingerprintConflict,
			}
		}
		if len(group.matching) == 0 {
			continue
		}
		group.first = group.matching[0].index
		state, err := b.readSQLiteBatchAttempt(ctx, tx, group.matching[0].item)
		if err != nil {
			return nil, err
		}
		group.state = state
		if completedAt.Valid {
			assignSQLiteBatchOutcome(outcomes, group, inbox.BatchItemOutcome{
				Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchACK,
				Attempt: state.attempt, Duplicate: true,
			})
			continue
		}
		if state.terminal || state.attempt >= maxAttempts || state.kind != "" {
			failureKind := inbox.FailureAttemptsExhausted
			var itemErr error
			itemErr = inbox.ErrAttemptsExhausted
			if state.terminal || state.kind == inbox.FailurePermanent {
				failureKind, itemErr = inbox.FailurePermanent, messenger.Permanent(inbox.ErrAttemptTerminal)
			}
			assignSQLiteBatchOutcome(outcomes, group, inbox.BatchItemOutcome{
				Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchDLQ,
				Attempt: state.attempt, FailureKind: failureKind, Err: itemErr,
			})
			continue
		}
		active = append(active, group)
	}
	return active, nil
}

func (b *backend) insertSQLiteBatchIdentities(
	ctx context.Context,
	tx *sql.Tx,
	groups []*sqliteBatchGroup,
) error {
	for start := 0; start < len(groups); start += sqliteBatchStatementItems {
		end := min(start+sqliteBatchStatementItems, len(groups))
		var query strings.Builder
		_, _ = query.WriteString("INSERT INTO ")
		_, _ = query.WriteString(b.names.inbox)
		_, _ = query.WriteString(" (consumer_id, source, message_id, fingerprint, created_at) VALUES ")
		arguments := make([]any, 0, (end-start)*5)
		for index, group := range groups[start:end] {
			if index != 0 {
				_ = query.WriteByte(',')
			}
			_, _ = query.WriteString("(?, ?, ?, ?, ?)")
			item := group.entries[0].item
			arguments = append(arguments, item.Key.ConsumerID, group.key.Source,
				group.key.MessageID.String(), item.Fingerprint[:], b.clock().UTC())
		}
		_, _ = query.WriteString(" ON CONFLICT (consumer_id, source, message_id) DO NOTHING")
		if _, err := tx.ExecContext(ctx, query.String(), arguments...); err != nil {
			return fmt.Errorf("inbox/sqlite: insert batch identities: %w", err)
		}
	}
	return nil
}

func (b *backend) readSQLiteBatchAttempt(
	ctx context.Context,
	tx *sql.Tx,
	item inbox.BatchItem,
) (sqliteBatchAttemptState, error) {
	state, err := b.readAttempt(ctx, tx, item.Key, inbox.AttemptFingerprint(item.Key, item.Fingerprint))
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteBatchAttemptState{}, nil
	}
	if err != nil {
		return sqliteBatchAttemptState{}, err
	}
	return sqliteBatchAttemptState(state), nil
}

func (b *backend) incrementSQLiteBatchAttempts(
	ctx context.Context,
	tx *sql.Tx,
	groups []*sqliteBatchGroup,
) (map[messenger.BatchItemKey]uint64, error) {
	result := make(map[messenger.BatchItemKey]uint64, len(groups))
	for _, generated := range []bool{false, true} {
		selected := selectSQLiteBatchGroups(groups, generated)
		for start := 0; start < len(selected); start += sqliteBatchStatementItems {
			end := min(start+sqliteBatchStatementItems, len(selected))
			var query strings.Builder
			_, _ = query.WriteString("INSERT INTO ")
			if generated {
				_, _ = query.WriteString(b.names.attemptGenerations)
			} else {
				_, _ = query.WriteString(b.names.attempts)
			}
			_, _ = query.WriteString(" (consumer_id, source, message_id, fingerprint, attempts, updated_at) VALUES ")
			arguments := make([]any, 0, (end-start)*6)
			for index, group := range selected[start:end] {
				if index != 0 {
					_ = query.WriteByte(',')
				}
				_, _ = query.WriteString("(?, ?, ?, ?, 1, ?)")
				item := group.matching[0].item
				fingerprint := inbox.AttemptFingerprint(item.Key, item.Fingerprint)
				arguments = append(arguments, item.Key.ConsumerID, group.key.Source,
					group.key.MessageID.String(), fingerprint[:], b.clock().UTC())
			}
			if generated {
				_, _ = query.WriteString(" ON CONFLICT (consumer_id, source, message_id, fingerprint) DO UPDATE")
			} else {
				_, _ = query.WriteString(" ON CONFLICT (consumer_id, source, message_id) DO UPDATE")
			}
			_, _ = query.WriteString(" SET attempts = attempts + 1, updated_at = excluded.updated_at")
			if _, err := tx.ExecContext(ctx, query.String(), arguments...); err != nil {
				return nil, fmt.Errorf("inbox/sqlite: increment batch attempts: %w", err)
			}
		}
	}
	for _, group := range groups {
		state, err := b.readSQLiteBatchAttempt(ctx, tx, group.matching[0].item)
		if err != nil {
			return nil, err
		}
		if state.attempt < 1 {
			return nil, errors.New("inbox/sqlite: invalid incremented batch attempt")
		}
		result[group.key] = state.attempt
	}
	return result, nil
}

func (b *backend) markSQLiteBatchCompleted(
	ctx context.Context,
	tx *sql.Tx,
	groups []*sqliteBatchGroup,
) error {
	for start := 0; start < len(groups); start += sqliteBatchStatementItems {
		end := min(start+sqliteBatchStatementItems, len(groups))
		var query strings.Builder
		_, _ = query.WriteString("WITH completed(source, message_id) AS (VALUES ")
		arguments := make([]any, 0, 2+(end-start)*2)
		for index, group := range groups[start:end] {
			if index != 0 {
				_ = query.WriteByte(',')
			}
			_, _ = query.WriteString("(?, ?)")
			arguments = append(arguments, group.key.Source, group.key.MessageID.String())
		}
		_, _ = query.WriteString(") UPDATE ")
		_, _ = query.WriteString(b.names.inbox)
		_, _ = query.WriteString(` AS identity SET completed_at = ? WHERE identity.consumer_id = ? AND EXISTS
            (SELECT 1 FROM completed WHERE completed.source = identity.source
             AND completed.message_id = identity.message_id)`)
		arguments = append(arguments, b.clock().UTC(), groups[start].matching[0].item.Key.ConsumerID)
		if _, err := tx.ExecContext(ctx, query.String(), arguments...); err != nil {
			return fmt.Errorf("inbox/sqlite: mark batch items complete: %w", err)
		}
	}
	return nil
}

func (b *backend) markSQLiteBatchTerminal(
	ctx context.Context,
	tx *sql.Tx,
	groups []*sqliteBatchGroup,
) error {
	for _, group := range groups {
		item := group.matching[0].item
		if err := b.markAttemptTerminal(ctx, tx, item.Key, item.Fingerprint,
			messenger.Permanent(errors.New("batch terminal"))); err != nil {
			return err
		}
	}
	return nil
}

func selectSQLiteBatchGroups(groups []*sqliteBatchGroup, generated bool) []*sqliteBatchGroup {
	selected := make([]*sqliteBatchGroup, 0, len(groups))
	for _, group := range groups {
		if (group.matching[0].item.Key.AttemptGeneration != "") == generated {
			selected = append(selected, group)
		}
	}
	return selected
}

func assignSQLiteBatchOutcome(
	outcomes []inbox.BatchItemOutcome,
	group *sqliteBatchGroup,
	outcome inbox.BatchItemOutcome,
) {
	for index, entry := range group.matching {
		assigned := outcome
		assigned.Key = entry.item.Key
		if index > 0 || outcome.Duplicate {
			assigned.Duplicate = true
		}
		outcomes[entry.index] = assigned
	}
}
