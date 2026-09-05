package pgsql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/internal/batchruntime"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

type batchPreparedItem struct {
	index int
	item  inbox.BatchItem
}

type batchGroup struct {
	key      messenger.BatchItemKey
	entries  []batchPreparedItem
	matching []batchPreparedItem
	state    batchAttemptState
	first    int
}

type batchAttemptState struct {
	kind     string
	attempt  uint64
	terminal bool
}

type batchStoredIdentity struct {
	fingerprint []byte
	completedAt sql.NullTime
}

type batchClassified struct {
	kind  batchruntime.FailureKind
	delay time.Duration
	err   error
}

// ProcessBatchAttempt executes one partial-outcome batch in one PostgreSQL
// transaction. Identity and attempt changes are set based and identities are
// locked in deterministic order.
func (b *backend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	maxAttempts uint64,
	handler inbox.BatchHandler,
) (report inbox.BatchProcessResult, processErr error) {
	if len(items) == 0 {
		return report, nil
	}
	groups, err := prepareBatchGroups(items)
	if err != nil {
		return report, err
	}
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return report, fmt.Errorf("inbox/pgsql: begin batch transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	report.Items = make([]inbox.BatchItemOutcome, len(items))
	lockOrder := append([]*batchGroup(nil), groups...)
	sort.Slice(lockOrder, func(i, j int) bool {
		if lockOrder[i].key.Source != lockOrder[j].key.Source {
			return lockOrder[i].key.Source < lockOrder[j].key.Source
		}
		return lockOrder[i].key.MessageID.String() < lockOrder[j].key.MessageID.String()
	})
	active, err := b.prepareBatchIdentities(ctx, tx, lockOrder, report.Items, maxAttempts)
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

	classified := make([]batchClassified, len(active))
	consuming := make([]*batchGroup, 0, len(active))
	for index, group := range active {
		kind, delay := batchruntime.Classify(itemErrors[index])
		classified[index] = batchClassified{kind: kind, delay: delay, err: itemErrors[index]}
		if kind != batchruntime.FailureDefer {
			consuming = append(consuming, group)
		}
	}
	attempts, err := b.incrementBatchAttempts(ctx, tx, consuming)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	successful := make([]*batchGroup, 0, len(active))
	permanent := make([]*batchGroup, 0, len(active))
	for index, group := range active {
		item := classified[index]
		if item.kind == batchruntime.FailureDefer {
			assignBatchOutcome(report.Items, group, inbox.BatchItemOutcome{
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
		assignBatchOutcome(report.Items, group, outcome)
	}
	if err := b.markBatchCompleted(ctx, tx, successful); err != nil {
		return inbox.BatchProcessResult{}, err
	}
	if err := b.markBatchTerminal(ctx, tx, permanent); err != nil {
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
		return inbox.BatchProcessResult{}, fmt.Errorf("inbox/pgsql: commit batch transaction: %w", err)
	}
	return report, nil
}

func prepareBatchGroups(items []inbox.BatchItem) ([]*batchGroup, error) {
	byKey := make(map[messenger.BatchItemKey]*batchGroup, len(items))
	groups := make([]*batchGroup, 0, len(items))
	for index, item := range items {
		key := messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
		group, exists := byKey[key]
		if !exists {
			group = &batchGroup{key: key, first: index}
			byKey[key] = group
			groups = append(groups, group)
		} else if group.entries[0].item.Key.AttemptGeneration != item.Key.AttemptGeneration {
			return nil, fmt.Errorf("%w: mixed attempt generations for %s/%s",
				messenger.ErrInvalidBatchResult, key.Source, key.MessageID)
		}
		group.entries = append(group.entries, batchPreparedItem{index: index, item: item})
	}
	return groups, nil
}

func (b *backend) prepareBatchIdentities(
	ctx context.Context,
	tx *sql.Tx,
	groups []*batchGroup,
	outcomes []inbox.BatchItemOutcome,
	maxAttempts uint64,
) ([]*batchGroup, error) {
	consumerID := groups[0].entries[0].item.Key.ConsumerID
	sources, messageIDs, fingerprints := batchGroupColumns(groups)
	if _, err := tx.ExecContext(ctx, b.names.render(`INSERT INTO {{inbox}}
        (consumer_id, source, message_id, fingerprint, created_at)
        SELECT $1, item.source, item.message_id, item.fingerprint, $5
        FROM unnest($2::text[], $3::text[], $4::bytea[]) AS item(source, message_id, fingerprint)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING`),
		consumerID, sources, messageIDs, fingerprints, b.clock().UTC()); err != nil {
		return nil, fmt.Errorf("inbox/pgsql: insert batch identities: %w", err)
	}
	rows, err := tx.QueryContext(ctx, b.names.render(`SELECT identity.source, identity.message_id,
        identity.fingerprint, identity.completed_at
        FROM {{inbox}} AS identity
        JOIN unnest($2::text[], $3::text[]) AS requested(source, message_id)
          ON requested.source = identity.source AND requested.message_id = identity.message_id
        WHERE identity.consumer_id = $1
        ORDER BY identity.source, identity.message_id FOR UPDATE OF identity`),
		consumerID, sources, messageIDs)
	if err != nil {
		return nil, fmt.Errorf("inbox/pgsql: lock batch identities: %w", err)
	}
	defer func() { _ = rows.Close() }()
	identities := make(map[messenger.BatchItemKey]batchStoredIdentity, len(groups))
	for rows.Next() {
		var source, messageID string
		var state batchStoredIdentity
		if err := rows.Scan(&source, &messageID, &state.fingerprint, &state.completedAt); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("inbox/pgsql: scan batch identity: %w", err)
		}
		key, err := scannedBatchKey(source, messageID)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		identities[key] = state
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("inbox/pgsql: close batch identity rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inbox/pgsql: iterate batch identities: %w", err)
	}

	matchingGroups := make([]*batchGroup, 0, len(groups))
	for _, group := range groups {
		identity, exists := identities[group.key]
		if !exists || len(identity.fingerprint) != len(inbox.Fingerprint{}) {
			return nil, errors.New("inbox/pgsql: missing or invalid batch identity")
		}
		for _, entry := range group.entries {
			if bytes.Equal(identity.fingerprint, entry.item.Fingerprint[:]) {
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
		matchingGroups = append(matchingGroups, group)
	}
	attempts, err := b.readBatchAttempts(ctx, tx, matchingGroups)
	if err != nil {
		return nil, err
	}
	active := make([]*batchGroup, 0, len(matchingGroups))
	for _, group := range matchingGroups {
		identity := identities[group.key]
		group.state = attempts[group.key]
		if identity.completedAt.Valid {
			assignBatchOutcome(outcomes, group, inbox.BatchItemOutcome{
				Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchACK,
				Attempt: group.state.attempt, Duplicate: true,
			})
			continue
		}
		if group.state.closed(maxAttempts) {
			failureKind := inbox.FailureAttemptsExhausted
			var itemErr error
			itemErr = inbox.ErrAttemptsExhausted
			if group.state.terminal || group.state.kind == inbox.FailurePermanent {
				failureKind, itemErr = inbox.FailurePermanent, messenger.Permanent(inbox.ErrAttemptTerminal)
			}
			assignBatchOutcome(outcomes, group, inbox.BatchItemOutcome{
				Fingerprint: group.matching[0].item.Fingerprint, Outcome: inbox.BatchDLQ,
				Attempt: group.state.attempt, FailureKind: failureKind, Err: itemErr,
			})
			continue
		}
		active = append(active, group)
	}
	return active, nil
}

func (b *backend) readBatchAttempts(
	ctx context.Context,
	tx *sql.Tx,
	groups []*batchGroup,
) (map[messenger.BatchItemKey]batchAttemptState, error) {
	result := make(map[messenger.BatchItemKey]batchAttemptState, len(groups))
	for _, generated := range []bool{false, true} {
		selected := selectBatchGroups(groups, generated)
		if len(selected) == 0 {
			continue
		}
		consumerID := selected[0].entries[0].item.Key.ConsumerID
		sources, messageIDs, fingerprints := batchAttemptColumns(selected)
		var rows *sql.Rows
		var err error
		if !generated {
			rows, err = tx.QueryContext(ctx, b.names.render(`SELECT attempt.source, attempt.message_id,
                attempt.attempts, attempt.terminal, COALESCE(closed.failure_kind, '')
                FROM {{attempts}} AS attempt
 LEFT JOIN {{terminal}} AS closed ON closed.consumer_id=attempt.consumer_id
 AND closed.source=attempt.source AND closed.message_id=attempt.message_id AND closed.fingerprint=attempt.fingerprint
                JOIN unnest($2::text[], $3::text[]) AS requested(source, message_id)
                  ON requested.source = attempt.source AND requested.message_id = attempt.message_id
                WHERE attempt.consumer_id = $1
                ORDER BY attempt.source, attempt.message_id FOR UPDATE OF attempt`),
				consumerID, sources, messageIDs)
		} else {
			rows, err = tx.QueryContext(ctx, b.names.render(`SELECT attempt.source, attempt.message_id,
                attempt.attempts, attempt.terminal, COALESCE(closed.failure_kind, '')
                FROM {{attempt_generations}} AS attempt
 LEFT JOIN {{terminal}} AS closed ON closed.consumer_id=attempt.consumer_id
 AND closed.source=attempt.source AND closed.message_id=attempt.message_id AND closed.fingerprint=attempt.fingerprint
                JOIN unnest($2::text[], $3::text[], $4::bytea[])
                  AS requested(source, message_id, fingerprint)
                  ON requested.source = attempt.source AND requested.message_id = attempt.message_id
                 AND requested.fingerprint = attempt.fingerprint
                WHERE attempt.consumer_id = $1
                ORDER BY attempt.source, attempt.message_id, attempt.fingerprint FOR UPDATE OF attempt`),
				consumerID, sources, messageIDs, fingerprints)
		}
		if err != nil {
			return nil, fmt.Errorf("inbox/pgsql: lock batch attempts: %w", err)
		}
		for rows.Next() {
			var source, messageID string
			var attempts int64
			var terminal bool
			var kind string
			if err := rows.Scan(&source, &messageID, &attempts, &terminal, &kind); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("inbox/pgsql: scan batch attempt: %w", err)
			}
			if attempts < 0 {
				_ = rows.Close()
				return nil, errors.New("inbox/pgsql: invalid stored batch attempt")
			}
			key, err := scannedBatchKey(source, messageID)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			result[key] = batchAttemptState{attempt: uint64(attempts), terminal: terminal, kind: kind}
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("inbox/pgsql: close batch attempt rows: %w", err)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("inbox/pgsql: iterate batch attempts: %w", err)
		}
	}
	return result, nil
}

func (b *backend) incrementBatchAttempts(
	ctx context.Context,
	tx *sql.Tx,
	groups []*batchGroup,
) (map[messenger.BatchItemKey]uint64, error) {
	result := make(map[messenger.BatchItemKey]uint64, len(groups))
	for _, generated := range []bool{false, true} {
		selected := selectBatchGroups(groups, generated)
		if len(selected) == 0 {
			continue
		}
		consumerID := selected[0].entries[0].item.Key.ConsumerID
		sources, messageIDs, fingerprints := batchAttemptColumns(selected)
		var rows *sql.Rows
		var err error
		if !generated {
			rows, err = tx.QueryContext(ctx, b.names.render(`INSERT INTO {{attempts}} AS current
                (consumer_id, source, message_id, fingerprint, attempts, updated_at)
                SELECT $1, item.source, item.message_id, item.fingerprint, 1, $5
                FROM unnest($2::text[], $3::text[], $4::bytea[])
                  AS item(source, message_id, fingerprint)
                ON CONFLICT (consumer_id, source, message_id) DO UPDATE
                SET attempts = current.attempts + 1, updated_at = EXCLUDED.updated_at
                RETURNING source, message_id, attempts`),
				consumerID, sources, messageIDs, fingerprints, b.clock().UTC())
		} else {
			rows, err = tx.QueryContext(ctx, b.names.render(`INSERT INTO {{attempt_generations}} AS current
                (consumer_id, source, message_id, fingerprint, attempts, updated_at)
                SELECT $1, item.source, item.message_id, item.fingerprint, 1, $5
                FROM unnest($2::text[], $3::text[], $4::bytea[])
                  AS item(source, message_id, fingerprint)
                ON CONFLICT (consumer_id, source, message_id, fingerprint) DO UPDATE
                SET attempts = current.attempts + 1, updated_at = EXCLUDED.updated_at
                RETURNING source, message_id, attempts`),
				consumerID, sources, messageIDs, fingerprints, b.clock().UTC())
		}
		if err != nil {
			return nil, fmt.Errorf("inbox/pgsql: increment batch attempts: %w", err)
		}
		for rows.Next() {
			var source, messageID string
			var attempts int64
			if err := rows.Scan(&source, &messageID, &attempts); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("inbox/pgsql: scan incremented batch attempt: %w", err)
			}
			if attempts < 1 {
				_ = rows.Close()
				return nil, errors.New("inbox/pgsql: invalid incremented batch attempt")
			}
			key, err := scannedBatchKey(source, messageID)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			result[key] = uint64(attempts)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("inbox/pgsql: close incremented batch attempts: %w", err)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("inbox/pgsql: iterate incremented batch attempts: %w", err)
		}
	}
	if len(result) != len(groups) {
		return nil, errors.New("inbox/pgsql: incomplete incremented batch attempt result")
	}
	return result, nil
}

func (b *backend) markBatchCompleted(ctx context.Context, tx *sql.Tx, groups []*batchGroup) error {
	if len(groups) == 0 {
		return nil
	}
	consumerID := groups[0].entries[0].item.Key.ConsumerID
	sources, messageIDs, _ := batchGroupColumns(groups)
	_, err := tx.ExecContext(ctx, b.names.render(`UPDATE {{inbox}} AS identity SET completed_at = $2
        FROM unnest($3::text[], $4::text[]) AS completed(source, message_id)
        WHERE identity.consumer_id = $1 AND identity.source = completed.source
          AND identity.message_id = completed.message_id`),
		consumerID, b.clock().UTC(), sources, messageIDs)
	if err != nil {
		return fmt.Errorf("inbox/pgsql: mark batch items complete: %w", err)
	}
	return nil
}

func (b *backend) markBatchTerminal(ctx context.Context, tx *sql.Tx, groups []*batchGroup) error {
	for _, generated := range []bool{false, true} {
		selected := selectBatchGroups(groups, generated)
		if len(selected) == 0 {
			continue
		}
		consumerID := selected[0].entries[0].item.Key.ConsumerID
		sources, messageIDs, fingerprints := batchAttemptColumns(selected)
		var err error
		if !generated {
			_, err = tx.ExecContext(ctx, b.names.render(`UPDATE {{attempts}} AS attempt
                SET terminal = TRUE, updated_at = $2
                FROM unnest($3::text[], $4::text[]) AS terminal(source, message_id)
                WHERE attempt.consumer_id = $1 AND attempt.source = terminal.source
                  AND attempt.message_id = terminal.message_id`),
				consumerID, b.clock().UTC(), sources, messageIDs)
		} else {
			_, err = tx.ExecContext(ctx, b.names.render(`UPDATE {{attempt_generations}} AS attempt
                SET terminal = TRUE, updated_at = $2
                FROM unnest($3::text[], $4::text[], $5::bytea[])
                  AS terminal(source, message_id, fingerprint)
                WHERE attempt.consumer_id = $1 AND attempt.source = terminal.source
                  AND attempt.message_id = terminal.message_id
                  AND attempt.fingerprint = terminal.fingerprint`),
				consumerID, b.clock().UTC(), sources, messageIDs, fingerprints)
		}
		if err != nil {
			return fmt.Errorf("inbox/pgsql: mark batch items terminal: %w", err)
		}
	}
	return nil
}

func selectBatchGroups(groups []*batchGroup, generated bool) []*batchGroup {
	selected := make([]*batchGroup, 0, len(groups))
	for _, group := range groups {
		if (batchGroupRepresentative(group).Key.AttemptGeneration != "") == generated {
			selected = append(selected, group)
		}
	}
	return selected
}

func batchGroupColumns(groups []*batchGroup) (sources []string, messageIDs []string, fingerprints [][]byte) {
	sources = make([]string, len(groups))
	messageIDs = make([]string, len(groups))
	fingerprints = make([][]byte, len(groups))
	for index, group := range groups {
		sources[index] = group.key.Source
		messageIDs[index] = group.key.MessageID.String()
		fingerprint := group.entries[0].item.Fingerprint
		if len(group.matching) != 0 {
			fingerprint = group.matching[0].item.Fingerprint
		}
		fingerprints[index] = append([]byte(nil), fingerprint[:]...)
	}
	return sources, messageIDs, fingerprints
}

func batchAttemptColumns(groups []*batchGroup) (sources []string, messageIDs []string, fingerprints [][]byte) {
	sources = make([]string, len(groups))
	messageIDs = make([]string, len(groups))
	fingerprints = make([][]byte, len(groups))
	for index, group := range groups {
		item := batchGroupRepresentative(group)
		sources[index] = group.key.Source
		messageIDs[index] = group.key.MessageID.String()
		fingerprint := inbox.AttemptFingerprint(item.Key, item.Fingerprint)
		fingerprints[index] = append([]byte(nil), fingerprint[:]...)
	}
	return sources, messageIDs, fingerprints
}

func batchGroupRepresentative(group *batchGroup) inbox.BatchItem {
	if len(group.matching) != 0 {
		return group.matching[0].item
	}
	return group.entries[0].item
}

func scannedBatchKey(source, messageID string) (messenger.BatchItemKey, error) {
	parsed, err := messenger.ParseMessageID(messageID)
	if err != nil {
		return messenger.BatchItemKey{}, fmt.Errorf("inbox/pgsql: parse stored batch message ID: %w", err)
	}
	return messenger.BatchItemKey{Source: source, MessageID: parsed}, nil
}

func assignBatchOutcome(outcomes []inbox.BatchItemOutcome, group *batchGroup, outcome inbox.BatchItemOutcome) {
	for index, entry := range group.matching {
		assigned := outcome
		assigned.Key = entry.item.Key
		if index > 0 || outcome.Duplicate {
			assigned.Duplicate = true
		}
		outcomes[entry.index] = assigned
	}
}

func (s batchAttemptState) closed(maxAttempts uint64) bool {
	return s.terminal || s.attempt >= maxAttempts || s.kind != ""
}
