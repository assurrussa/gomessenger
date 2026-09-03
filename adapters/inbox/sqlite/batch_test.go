package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
)

func TestStore_ProcessBatchAttemptMixedOutcomes(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	items := []inbox.BatchItem{
		batchItem(t, "018f4f2c-4a00-7000-8000-000000000071", "success"),
		batchItem(t, "018f4f2c-4a00-7000-8000-000000000072", "retry"),
		batchItem(t, "018f4f2c-4a00-7000-8000-000000000073", "defer"),
		batchItem(t, "018f4f2c-4a00-7000-8000-000000000074", "permanent"),
	}
	retryErr := errors.New("retry")
	deferErr := errors.New("defer")
	permanentErr := errors.New("permanent")
	report, err := store.ProcessBatchAttempt(t.Context(), items, 3, func(
		ctx context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		if len(active) != 4 {
			t.Fatalf("active items = %d, want 4", len(active))
		}
		tx, ok := inbox.SQLTxFromContext(ctx)
		if !ok {
			t.Fatal("missing SQL transaction")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`,
			active[0].Key.MessageID.String()); err != nil {
			return messenger.BatchResult{}, err
		}
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: batchResultKey(active[3]), Err: messenger.Permanent(permanentErr)},
			{Key: batchResultKey(active[1]), Err: messenger.RetryAfter(retryErr, 2*time.Second)},
			{Key: batchResultKey(active[0])},
			{Key: batchResultKey(active[2]), Err: messenger.DeferAfter(deferErr, 3*time.Second)},
		}}, nil
	})
	if err != nil {
		t.Fatalf("ProcessBatchAttempt() error = %v", err)
	}
	if report.HandlerMessages != 4 || effectCount(t, db) != 1 {
		t.Fatalf("report=%#v effects=%d", report, effectCount(t, db))
	}
	wantOutcomes := []inbox.BatchOutcome{inbox.BatchACK, inbox.BatchRetry, inbox.BatchDefer, inbox.BatchDLQ}
	wantAttempts := []uint64{1, 1, 0, 1}
	for index, outcome := range report.Items {
		if outcome.Outcome != wantOutcomes[index] || outcome.Attempt != wantAttempts[index] {
			t.Fatalf("outcome %d = %#v", index, outcome)
		}
	}
	if report.Items[1].Delay != 2*time.Second || report.Items[2].Delay != 3*time.Second ||
		report.Items[3].FailureKind != inbox.FailurePermanent {
		t.Fatalf("unexpected classified outcomes: %#v", report.Items)
	}
}

func TestStore_ProcessBatchAttemptTopLevelRollbackDoesNotConsumeAttempts(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	item := batchItem(t, "018f4f2c-4a00-7000-8000-000000000075", "top-level")
	wantErr := errors.New("whole batch retry")
	_, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{item}, 2, func(
		ctx context.Context,
		_ []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		tx, _ := inbox.SQLTxFromContext(ctx)
		_, _ = tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`, item.Key.MessageID.String())
		return messenger.BatchResult{}, messenger.RetryAfter(wantErr, time.Second)
	})
	if !errors.Is(err, wantErr) || effectCount(t, db) != 0 {
		t.Fatalf("top-level error=%v effects=%d", err, effectCount(t, db))
	}

	report, err := store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{item}, 2, func(
		context.Context,
		[]inbox.BatchItem,
	) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{{Key: batchResultKey(item)}}}, nil
	})
	if err != nil || report.Items[0].Attempt != 1 || report.Items[0].Outcome != inbox.BatchACK {
		t.Fatalf("retry report=%#v error=%v", report, err)
	}
}

func TestStore_ProcessBatchAttemptCoalescesAndConflicts(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	first := batchItem(t, "018f4f2c-4a00-7000-8000-000000000076", "same")
	duplicate := first
	conflict := first
	conflict.Fingerprint = inbox.FingerprintEnvelope([]byte("different"))
	report, err := store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{first, duplicate, conflict}, 3, func(
		_ context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		if len(active) != 1 {
			t.Fatalf("active items = %d, want 1", len(active))
		}
		return messenger.BatchResult{Items: []messenger.BatchItemResult{{Key: batchResultKey(active[0])}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.HandlerMessages != 1 || report.Items[0].Outcome != inbox.BatchACK ||
		report.Items[1].Outcome != inbox.BatchACK || !report.Items[1].Duplicate ||
		report.Items[2].Outcome != inbox.BatchDLQ ||
		report.Items[2].FailureKind != inbox.FailureIdentityConflict {
		t.Fatalf("report = %#v", report)
	}
}

func TestStore_ProcessBatchAttemptExhaustionAndTerminalReplay(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	retry := batchItem(t, "018f4f2c-4a00-7000-8000-000000000077", "retry-limit")
	for expectedAttempt := uint64(1); expectedAttempt <= 2; expectedAttempt++ {
		report, processErr := store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{retry}, 2, func(
			_ context.Context,
			active []inbox.BatchItem,
		) (messenger.BatchResult, error) {
			return messenger.BatchResult{Items: []messenger.BatchItemResult{{
				Key: batchResultKey(active[0]), Err: errors.New("retry"),
			}}}, nil
		})
		if processErr != nil || report.Items[0].Attempt != expectedAttempt {
			t.Fatalf("attempt %d report=%#v error=%v", expectedAttempt, report, processErr)
		}
		if expectedAttempt == 2 && (report.Items[0].Outcome != inbox.BatchDLQ ||
			report.Items[0].FailureKind != inbox.FailureAttemptsExhausted) {
			t.Fatalf("exhausted report = %#v", report)
		}
	}
	var calls int
	report, err := store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{retry}, 2, func(
		context.Context,
		[]inbox.BatchItem,
	) (messenger.BatchResult, error) {
		calls++
		return messenger.BatchResult{}, nil
	})
	if err != nil || calls != 0 || report.Items[0].Outcome != inbox.BatchDLQ ||
		report.Items[0].Attempt != 2 {
		t.Fatalf("exhausted replay report=%#v calls=%d error=%v", report, calls, err)
	}

	permanent := batchItem(t, "018f4f2c-4a00-7000-8000-000000000078", "permanent")
	_, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{permanent}, 3, func(
		_ context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{{
			Key: batchResultKey(active[0]), Err: messenger.Permanent(errors.New("bad")),
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{permanent}, 3, func(
		context.Context,
		[]inbox.BatchItem,
	) (messenger.BatchResult, error) {
		calls++
		return messenger.BatchResult{}, nil
	})
	if err != nil || report.Items[0].FailureKind != inbox.FailurePermanent || report.Items[0].Attempt != 1 {
		t.Fatalf("terminal replay report=%#v error=%v", report, err)
	}
}

func TestStore_ProcessBatchAttemptRejectsMixedAttemptGenerations(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	msgID := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000099")
	item1 := inbox.BatchItem{
		Key: inbox.Key{
			ConsumerID:        testConsumerID,
			Source:            testSource,
			MessageID:         msgID,
			AttemptGeneration: "gen-1",
		},
		Fingerprint: inbox.FingerprintEnvelope([]byte("same-fingerprint")),
	}
	item2 := inbox.BatchItem{
		Key: inbox.Key{
			ConsumerID:        testConsumerID,
			Source:            testSource,
			MessageID:         msgID,
			AttemptGeneration: "gen-2",
		},
		Fingerprint: inbox.FingerprintEnvelope([]byte("same-fingerprint")),
	}

	handlerInvocations := 0
	_, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{item1, item2}, 3, func(
		ctx context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		handlerInvocations++
		tx, _ := inbox.SQLTxFromContext(ctx)
		_, _ = tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`, active[0].Key.MessageID.String())
		return messenger.BatchResult{
			Items: []messenger.BatchItemResult{
				{Key: batchResultKey(active[0])},
			},
		}, nil
	})
	if err == nil {
		t.Fatal("expected error for mixed attempt generations, got nil")
	}
	if !errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("expected ErrInvalidBatchResult, got %v", err)
	}
	if handlerInvocations != 0 {
		t.Fatalf("expected handler not to be invoked, got %d invocations", handlerInvocations)
	}
	if effects := effectCount(t, db); effects != 0 {
		t.Fatalf("expected 0 business effects, got %d", effects)
	}
}

func batchItem(t *testing.T, id, fingerprint string) inbox.BatchItem {
	t.Helper()
	return inbox.BatchItem{Key: inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, id),
	}, Fingerprint: inbox.FingerprintEnvelope([]byte(fingerprint))}
}

func batchResultKey(item inbox.BatchItem) messenger.BatchItemKey {
	return messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
}
