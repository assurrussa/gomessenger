package retentiontest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

func testDeferredSibling(t *testing.T, db *sql.DB, factory Factory, batch, replay bool) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	key, fingerprint := identity(t, 20)
	old := key
	if replay {
		key.AttemptGeneration = "deferred-replay"
	} else {
		old.AttemptGeneration = "old-replay"
	}
	terminal(t, store, old, fingerprint)
	if err := store.ConfirmTerminalHandoff(t.Context(), old, fingerprint); err != nil {
		t.Fatal(err)
	}
	// Place the old confirmation strictly before the cutoff and the new defer after it.
	cutoff := time.Now().UTC().Add(-time.Hour)
	// #nosec G202 -- The fixture prefix is generated locally, never from message data.
	if _, err := db.ExecContext(t.Context(), "UPDATE "+prefix+"inbox_terminal SET handoff_confirmed_at=$1",
		cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	deferred := messenger.DeferAfter(errors.New("wait for prerequisite"), time.Minute)
	for range 2 {
		attempt, err := invokeDeferredTest(t, store, key, fingerprint, batch, deferred)
		if delay, ok := messenger.DeferDelay(err); !ok || delay != time.Minute || attempt != 0 {
			t.Fatalf("defer: attempt=%d, err=%v", attempt, err)
		}
	}
	assertPrune(t, store, cutoff, 1, 1)
	assertCount(t, db, prefix+"inbox", 1)
	assertCount(t, db, prefix+"inbox_terminal", 0)
	// A fresh Store must still reject canonical identity changes before invoking business code.
	store = factory(t, prefix)
	if _, err := store.ProcessAttempt(t.Context(), key, inbox.FingerprintEnvelope([]byte("changed")), 2,
		func(context.Context) error {
			t.Error("conflicting handler invoked")
			return nil
		}); !errors.Is(err, inbox.ErrFingerprintConflict) {
		t.Fatalf("identity conflict: %v", err)
	}
	// Switch processing paths after restart; both attempts in the original budget remain available.
	retry := errors.New("retry")
	if attempt, err := invokeDeferredTest(t, store, key, fingerprint, !batch, retry); attempt != 1 || !errors.Is(err, retry) {
		t.Fatalf("first consumed attempt: %d, %v", attempt, err)
	}
	if attempt, err := invokeDeferredTest(t, store, key, fingerprint, batch, nil); attempt != 2 || err != nil {
		t.Fatalf("last available attempt: %d, %v", attempt, err)
	}
	if removed, err := store.Prune(t.Context(), time.Now().Add(time.Hour), 1); removed != 1 || err != nil {
		t.Fatalf("completed prune: %d, %v", removed, err)
	}
	for _, suffix := range []string{"inbox", "inbox_attempts", "inbox_attempt_generations", "inbox_terminal"} {
		assertCount(t, db, prefix+suffix, 0)
	}
}

func invokeDeferredTest(t *testing.T, store *inbox.Store, key inbox.Key, fingerprint inbox.Fingerprint,
	batch bool, failure error,
) (uint64, error) {
	t.Helper()
	if !batch {
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error { return failure })
		return result.Attempt, err
	}
	report, err := store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{{Key: key, Fingerprint: fingerprint}}, 2,
		func(_ context.Context, active []inbox.BatchItem) (messenger.BatchResult, error) {
			return messenger.BatchResult{Items: []messenger.BatchItemResult{{
				Key: messenger.BatchItemKey{Source: active[0].Key.Source, MessageID: active[0].Key.MessageID}, Err: failure,
			}}}, nil
		})
	if err != nil {
		return 0, err
	}
	return report.Items[0].Attempt, report.Items[0].Err
}
