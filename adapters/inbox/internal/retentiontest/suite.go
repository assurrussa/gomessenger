// Package retentiontest exercises the shared terminal retention contract against SQL backends.
package retentiontest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/adapters/inbox/internal/terminalstate"
)

// Factory constructs and migrates a store using the supplied isolated prefix.
type Factory func(t *testing.T, prefix string) *inbox.Store

// Run checks single/batch closure, handoff confirmation and bounded retention.
func Run(t *testing.T, db *sql.DB, factory Factory) {
	t.Helper()
	for _, batch := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("PruneTerminalPreservesDeferredSiblingGeneration/batch_%t_replay_%t", batch, replay), func(t *testing.T) {
				testDeferredSibling(t, db, factory, batch, replay)
			})
		}
		for _, permanent := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch_%t_permanent_%t", batch, permanent), func(t *testing.T) {
				testClosure(t, db, factory, batch, permanent)
			})
		}
	}
	t.Run("retention eligibility and active generations", func(t *testing.T) {
		testEligibility(t, db, factory)
	})
	t.Run("migration preserves unknown handoff", func(t *testing.T) {
		testMigration(t, db, factory)
	})
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent_prune_complete_%t", complete), func(t *testing.T) {
			testConcurrentPrune(t, db, factory, complete)
		})
	}
	t.Run("bounded reset and fingerprint validation", func(t *testing.T) {
		testReset(t, db, factory)
	})
}

func fixturePrefix(t *testing.T, db *sql.DB) string {
	t.Helper()
	prefix := fmt.Sprintf("rt_%x_", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, suffix := range []string{"inbox_terminal", "inbox_attempt_generations", "inbox_attempts", "inbox"} {
			if _, err := db.ExecContext(context.Background(), "DROP TABLE "+prefix+suffix); err != nil {
				t.Errorf("drop test table: %v", err)
			}
		}
	})
	return prefix
}

func identity(t *testing.T, n int) (inbox.Key, inbox.Fingerprint) {
	t.Helper()
	id, err := messenger.ParseMessageID(fmt.Sprintf("018f4f2c-4a00-7000-8000-%012x", n))
	if err != nil {
		t.Fatal(err)
	}
	key := inbox.Key{ConsumerID: "retention-worker", Source: "urn:retention", MessageID: id}
	return key, inbox.FingerprintEnvelope([]byte("same"))
}

func terminal(t *testing.T, store *inbox.Store, key inbox.Key, fingerprint inbox.Fingerprint) {
	t.Helper()
	if _, err := store.ProcessAttempt(t.Context(), key, fingerprint, 1,
		func(context.Context) error { return messenger.Permanent(errors.New("permanent")) }); err == nil {
		t.Fatal("expected failure")
	}
}

func assertPrune(t *testing.T, store *inbox.Store, before time.Time, limit int, want int64) {
	t.Helper()
	got, err := store.PruneTerminalAttempts(t.Context(), before, limit)
	if err != nil || got != want {
		t.Fatalf("terminal prune: %d, %v; want %d", got, err, want)
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s: %d rows, want %d", table, count, want)
	}
}

func testClosure(t *testing.T, db *sql.DB, factory Factory, batch, permanent bool) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	key, fingerprint := identity(t, 1)
	var calls atomic.Int32
	failure := errors.New("terminal test failure")
	if permanent {
		failure = messenger.Permanent(failure)
	}
	invoke := func(current *inbox.Store, key inbox.Key, maxAttempts uint64, failure error) error {
		if !batch {
			_, err := current.ProcessAttempt(t.Context(), key, fingerprint, maxAttempts, func(context.Context) error {
				calls.Add(1)
				return failure
			})
			return err
		}
		report, err := current.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{{Key: key, Fingerprint: fingerprint}}, maxAttempts,
			func(_ context.Context, active []inbox.BatchItem) (messenger.BatchResult, error) {
				calls.Add(1)
				return messenger.BatchResult{Items: []messenger.BatchItemResult{{Key: messenger.BatchItemKey{
					Source: active[0].Key.Source, MessageID: active[0].Key.MessageID,
				}, Err: failure}}}, nil
			})
		if err != nil {
			return err
		}
		return report.Items[0].Err
	}
	// Model a delivery already fetched by another worker, stalled before Inbox admission.
	released := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(released) }) })
	waiting := make(chan struct{})
	late := make(chan error, 1)
	restarted := factory(t, prefix)
	go func() { close(waiting); <-released; late <- invoke(restarted, key, 100, nil) }()
	<-waiting
	if err := invoke(store, key, 1, failure); err == nil {
		t.Fatal("expected terminal outcome")
	}
	if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(released) })
	if err := <-late; err == nil || calls.Load() != 1 {
		t.Fatalf("late duplicate: calls=%d, err=%v", calls.Load(), err)
	}
	if !store.SupportsTerminalRetention() {
		t.Fatal("missing retention capability")
	}
	replay := key
	replay.AttemptGeneration = "explicit-replay"
	if err := invoke(store, replay, 1, nil); err != nil || calls.Load() != 2 {
		t.Fatalf("replay: %v, calls=%d", err, calls.Load())
	}
	if err := invoke(store, key, 100, nil); err != nil || calls.Load() != 2 {
		t.Fatalf("completed duplicate: %v", err)
	}
	if removed, err := store.Prune(t.Context(), time.Now().Add(time.Hour), 10); err != nil || removed != 1 {
		t.Fatalf("completed prune: %d, %v", removed, err)
	}
	assertCount(t, db, prefix+"inbox_terminal", 0)
}

func testEligibility(t *testing.T, db *sql.DB, factory Factory) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	key, fingerprint := identity(t, 2)
	terminal(t, store, key, fingerprint)
	cutoff := time.Now().Add(time.Hour)
	assertPrune(t, store, cutoff, 10, 0)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.ConfirmTerminalHandoff(cancelled, key, fingerprint); err == nil {
		t.Fatal("cancelled confirmation succeeded")
	}
	assertPrune(t, store, cutoff, 10, 0)
	if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}
	assertPrune(t, store, time.Unix(1, 0), 10, 0)
	// A new delivery revokes old confirmation; an ACK followed by failed confirmation cannot reuse it.
	if _, err := store.ProcessAttempt(t.Context(), key, fingerprint, 100, func(context.Context) error {
		t.Error("terminal redelivery invoked handler")
		return nil
	}); err == nil {
		t.Fatal("terminal redelivery succeeded")
	}
	if err := store.ConfirmTerminalHandoff(cancelled, key, fingerprint); err == nil {
		t.Fatal("cancelled re-confirmation")
	}
	assertPrune(t, store, cutoff, 10, 0)
	if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}

	active := key
	active.AttemptGeneration = "active"
	if _, err := store.ProcessAttempt(t.Context(), active, fingerprint, 3,
		func(context.Context) error { return errors.New("retry") }); err == nil {
		t.Fatal("expected retry")
	}
	assertPrune(t, store, cutoff, 1, 1)
	assertCount(t, db, prefix+"inbox", 1)
	if result, err := store.ProcessAttempt(t.Context(), active, fingerprint, 3,
		func(context.Context) error { return nil }); err != nil || result.Attempt != 2 {
		t.Fatalf("active generation: %+v %v", result, err)
	}
}

func testReset(t *testing.T, db *sql.DB, factory Factory) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	for i := 10; i < 13; i++ {
		key, fingerprint := identity(t, i)
		terminal(t, store, key, fingerprint)
		if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
			t.Fatal(err)
		}
	}
	assertPrune(t, store, time.Now().Add(time.Hour), 2, 2)
	assertCount(t, db, prefix+"inbox_terminal", 1)
	key, fingerprint := identity(t, 12)
	if err := store.ConfirmTerminalHandoff(t.Context(), key, inbox.Fingerprint{}); !errors.Is(err, inbox.ErrFingerprintConflict) {
		t.Fatalf("conflict: %v", err)
	}
	//nolint:staticcheck // Verify compatibility of the explicit destructive reset API.
	if err := store.ForgetAttempt(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, prefix+"inbox_terminal", 0)
	if _, err := store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func testMigration(t *testing.T, db *sql.DB, factory Factory) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	key, fingerprint := identity(t, 30)
	terminal(t, store, key, fingerprint)
	// Remove only the new additive table, reproducing a pre-migration permanent counter.
	if _, err := db.ExecContext(t.Context(), "DROP TABLE "+prefix+"inbox_terminal"); err != nil {
		t.Fatal(err)
	}
	store = factory(t, prefix)
	assertCount(t, db, prefix+"inbox_terminal", 1)
	assertPrune(t, store, time.Now().Add(time.Hour), 10, 0)
	if _, err := store.ProcessAttempt(t.Context(), key, fingerprint, 100, func(context.Context) error {
		t.Error("legacy permanent invoked handler")
		return nil
	}); !errors.Is(err, inbox.ErrAttemptTerminal) {
		t.Fatal(err)
	}
	if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}
	assertPrune(t, store, time.Now().Add(time.Hour), 10, 1)
}

func testConcurrentPrune(t *testing.T, db *sql.DB, factory Factory, complete bool) {
	t.Helper()
	prefix := fixturePrefix(t, db)
	store := factory(t, prefix)
	key, fingerprint := identity(t, 40)
	terminal(t, store, key, fingerprint)
	if err := store.ConfirmTerminalHandoff(t.Context(), key, fingerprint); err != nil {
		t.Fatal(err)
	}
	active := key
	active.AttemptGeneration = "concurrent-active"
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	processed := make(chan error, 1)
	go func() {
		_, err := store.ProcessAttempt(t.Context(), active, fingerprint, 3, func(context.Context) error {
			close(entered)
			<-release
			if !complete {
				return errors.New("retry active generation")
			}
			return nil
		})
		processed <- err
	}()
	<-entered
	candidatesQuery := make(chan struct{}, 1)
	candidatesSelected := make(chan int, 1)
	beforeLock := make(chan struct{}, 1)
	pruneCtx := terminalstate.WithPruneHooks(t.Context(), terminalstate.PruneHooks{
		OnCandidatesQuery: func(_ context.Context) error {
			candidatesQuery <- struct{}{}
			return nil
		},
		OnCandidatesSelected: func(_ context.Context, count int) error {
			candidatesSelected <- count
			return nil
		},
		OnBeforeLockIdentity: func(_ context.Context) error {
			beforeLock <- struct{}{}
			return nil
		},
	})

	pruned := make(chan error, 1)
	go func() {
		_, err := store.PruneTerminalAttempts(pruneCtx, time.Now().Add(time.Hour), 1)
		pruned <- err
	}()

	var pruneErr error
	if isSQLite(db) {
		// SQLite uses table/database serialization where competing queries retry or block
		// until the active write transaction commits.
		select {
		case <-candidatesQuery:
			// Prune transaction has begun and reached the candidate query while handler is active.
			once.Do(func() { close(release) })
			pruneErr = <-pruned
		case pruneErr = <-pruned:
			once.Do(func() { close(release) })
		}
	} else {
		// PostgreSQL supports MVCC candidate selection while holding row locks.
		select {
		case count := <-candidatesSelected:
			if count != 1 {
				t.Fatalf("selected candidates = %d, want 1", count)
			}
			select {
			case <-beforeLock:
				// Prune selected the in-flight candidate and reached the identity lock barrier.
				once.Do(func() { close(release) })
				pruneErr = <-pruned
			case pruneErr = <-pruned:
				once.Do(func() { close(release) })
			}
		case pruneErr = <-pruned:
			once.Do(func() { close(release) })
		}
	}

	if err := <-processed; (err == nil) != complete {
		t.Fatalf("processing: %v", err)
	}
	assertPruneResult(t, db, prefix, pruneErr, complete)
	assertCount(t, db, prefix+"inbox", 1)
	var calls int
	result, err := store.ProcessAttempt(t.Context(), active, fingerprint, 3, func(context.Context) error { calls++; return nil })
	if err != nil || (complete && (!result.Duplicate || calls != 0)) || (!complete && (result.Attempt != 2 || calls != 1)) {
		t.Fatalf("active after concurrent prune: %+v, calls=%d, %v", result, calls, err)
	}
}

func assertPruneResult(t *testing.T, db *sql.DB, prefix string, pruneErr error, complete bool) {
	t.Helper()
	if pruneErr != nil {
		if !isSQLite(db) {
			t.Fatalf("prune terminal attempts unexpected error: %v", pruneErr)
		}
		if !isExpectedSQLiteConflict(pruneErr) {
			t.Fatalf("prune terminal attempts unexpected SQLite error: %v", pruneErr)
		}
		// SQLite may reject a competing writer; a failed cleanup must leave all protection intact.
		assertCount(t, db, prefix+"inbox_terminal", 1)
		return
	}
	wantTerminal := 0
	if complete {
		wantTerminal = 1
	}
	assertCount(t, db, prefix+"inbox_terminal", wantTerminal)
}

func isSQLite(db *sql.DB) bool {
	if db == nil {
		return false
	}
	if _, ok := db.Driver().(*sqlite.Driver); ok {
		return true
	}
	return strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "sqlite")
}

func isExpectedSQLiteConflict(err error) bool {
	if err == nil {
		return false
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		code := sqliteErr.Code() & 0xff
		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	}
	return false
}
