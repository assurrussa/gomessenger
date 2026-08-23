package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	_ "modernc.org/sqlite"

	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
)

const (
	testConsumerID = "media-worker"
	testSource     = "urn:service:test"
)

func TestStore_ProcessIsAtomicAndIdempotent(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("one"))
	var calls atomic.Int32
	handler := func(ctx context.Context) error {
		calls.Add(1)
		tx, ok := inbox.SQLTxFromContext(ctx)
		if !ok {
			return errors.New("missing inbox transaction")
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`, key.MessageID.String())
		return err
	}
	result, err := store.Process(t.Context(), key, fingerprint, handler)
	if err != nil || result.Duplicate {
		t.Fatalf("first process = %#v, %v", result, err)
	}
	result, err = store.Process(t.Context(), key, fingerprint, handler)
	if err != nil || !result.Duplicate {
		t.Fatalf("duplicate process = %#v, %v", result, err)
	}
	if calls.Load() != 1 || effectCount(t, db) != 1 {
		t.Fatalf("calls=%d effects=%d", calls.Load(), effectCount(t, db))
	}
}

func TestStore_HandlerFailureRollsBackIdentityAndBusinessWrites(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000002"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("retry"))
	wantErr := errors.New("retry")
	_, err = store.Process(t.Context(), key, fingerprint, func(ctx context.Context) error {
		tx, _ := inbox.SQLTxFromContext(ctx)
		if _, err := tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`, key.MessageID.String()); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) || effectCount(t, db) != 0 {
		t.Fatalf("failed process error=%v effects=%d", err, effectCount(t, db))
	}
	result, err := store.Process(t.Context(), key, fingerprint, func(ctx context.Context) error {
		tx, _ := inbox.SQLTxFromContext(ctx)
		_, err := tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`, key.MessageID.String())
		return err
	})
	if err != nil || result.Duplicate || effectCount(t, db) != 1 {
		t.Fatalf("retry result=%#v error=%v effects=%d", result, err, effectCount(t, db))
	}
}

func TestStore_ProcessAttemptPersistsFailuresAcrossStoreInstances(t *testing.T) {
	db := openDatabase(t)
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000012"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("bounded-retry"))
	wantErr := errors.New("retry")
	for expectedAttempt := uint64(1); expectedAttempt <= 2; expectedAttempt++ {
		store, err := inboxsqlite.New(db)
		if err != nil {
			t.Fatalf("new store: %v", err)
		}
		result, processErr := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(ctx context.Context) error {
			tx, _ := inbox.SQLTxFromContext(ctx)
			if _, err := tx.ExecContext(ctx, `INSERT INTO business_effects (message_id) VALUES (?)`,
				key.MessageID.String()); err != nil {
				return err
			}
			return wantErr
		})
		if !errors.Is(processErr, wantErr) || result.Attempt != expectedAttempt || effectCount(t, db) != 0 {
			t.Fatalf("attempt %d result=%#v error=%v effects=%d",
				expectedAttempt, result, processErr, effectCount(t, db))
		}
	}
	restarted, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	result, err := restarted.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error {
		t.Fatal("handler ran past the durable attempt limit")
		return nil
	})
	if !errors.Is(err, inbox.ErrAttemptsExhausted) || result.Attempt != 2 {
		t.Fatalf("exhausted result=%#v error=%v", result, err)
	}
}

func TestStore_ProcessAttemptPersistsPermanentOutcomeAcrossStoreInstances(t *testing.T) {
	db := openDatabase(t)
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000016"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("permanent-outcome"))
	cause := errors.New("unsupported")
	var calls atomic.Int32
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(context.Context) error {
		calls.Add(1)
		return messenger.Permanent(cause)
	})
	if !messenger.IsPermanent(err) || !errors.Is(err, cause) || result.Attempt != 1 || calls.Load() != 1 {
		t.Fatalf("first permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}

	restarted, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	result, err = restarted.ProcessAttempt(t.Context(), key, fingerprint, 3, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if !messenger.IsPermanent(err) || !errors.Is(err, inbox.ErrAttemptTerminal) ||
		result.Attempt != 1 || calls.Load() != 1 {
		t.Fatalf("restored permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}

	if err := restarted.ForgetAttempt(t.Context(), key, fingerprint); err != nil {
		t.Fatalf("forget terminal attempt: %v", err)
	}
	result, err = restarted.ProcessAttempt(t.Context(), key, fingerprint, 3, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil || result.Attempt != 1 || calls.Load() != 2 {
		t.Fatalf("replayed permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}
}

func TestStore_AttemptGenerationStartsFreshBoundedCycle(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000017"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("generated-replay"))
	permanentErr := errors.New("permanent")
	if result, processErr := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error {
		return messenger.Permanent(permanentErr)
	}); !errors.Is(processErr, permanentErr) || result.Attempt != 1 {
		t.Fatalf("original attempt = %#v, %v", result, processErr)
	}

	retryErr := errors.New("retry")
	replayOne := key
	replayOne.AttemptGeneration = "gm-replay-one"
	replayTwo := key
	replayTwo.AttemptGeneration = "gm-replay-two"
	processRetry := func(replayKey inbox.Key, expectedAttempt uint64) {
		t.Helper()
		result, processErr := store.ProcessAttempt(t.Context(), replayKey, fingerprint, 2, func(context.Context) error {
			return retryErr
		})
		if !errors.Is(processErr, retryErr) || result.Attempt != expectedAttempt {
			t.Fatalf("replay %q attempt %d = %#v, %v", replayKey.AttemptGeneration, expectedAttempt, result, processErr)
		}
	}
	processRetry(replayOne, 1)
	processRetry(replayTwo, 1)
	processRetry(replayOne, 2)
	processRetry(replayTwo, 2)
	assertExhausted := func(replayKey inbox.Key) {
		t.Helper()
		result, processErr := store.ProcessAttempt(t.Context(), replayKey, fingerprint, 2, func(context.Context) error {
			t.Fatalf("handler ran past replay %q attempt limit", replayKey.AttemptGeneration)
			return nil
		})
		if !errors.Is(processErr, inbox.ErrAttemptsExhausted) || result.Attempt != 2 {
			t.Fatalf("exhausted replay %q = %#v, %v", replayKey.AttemptGeneration, result, processErr)
		}
	}
	assertExhausted(replayOne)
	assertExhausted(replayTwo)

	if err := store.ForgetAttempt(t.Context(), replayOne, fingerprint); err != nil {
		t.Fatalf("forget first replay generation: %v", err)
	}
	var identities int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM gomessenger_inbox
	    WHERE consumer_id = ? AND source = ? AND message_id = ?`, key.ConsumerID, key.Source,
		key.MessageID.String()).Scan(&identities); err != nil || identities != 1 {
		t.Fatalf("identity after generation cleanup = %d, %v", identities, err)
	}
	assertExhausted(replayTwo)

	key.AttemptGeneration = "gm-replay-three"
	if result, processErr := store.ProcessAttempt(
		t.Context(), key, fingerprint, 2, func(context.Context) error { return nil },
	); processErr != nil || result.Attempt != 1 {
		t.Fatalf("next replay generation = %#v, %v", result, processErr)
	}
}

func TestStore_ProcessAttemptCanFinalizeAfterHandlerContextCancellation(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000015"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("handler-timeout"))
	handlerContext, cancelHandler := context.WithCancel(t.Context())
	cancelHandler()
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(transactionContext context.Context) error {
		tx, ok := inbox.SQLTxFromContext(transactionContext)
		if !ok {
			return errors.New("missing inbox transaction")
		}
		return inbox.ContextWithSQLTx(handlerContext, tx).Err()
	})
	if !errors.Is(err, context.Canceled) || result.Attempt != 1 {
		t.Fatalf("cancelled attempt = %#v, %v", result, err)
	}
	result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error {
		t.Fatal("cancelled handler attempt was not persisted")
		return nil
	})
	if !errors.Is(err, inbox.ErrAttemptsExhausted) || result.Attempt != 1 {
		t.Fatalf("exhausted after cancellation = %#v, %v", result, err)
	}
}

func TestStore_PruneRemovesDurableAttemptCounter(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000013"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("pruned-attempt"))
	var calls atomic.Int32
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil || result.Attempt != 1 {
		t.Fatalf("first attempt = %#v, %v", result, err)
	}
	if deleted, pruneErr := store.Prune(t.Context(), time.Now().Add(time.Minute), 1); pruneErr != nil || deleted != 1 {
		t.Fatalf("prune = %d, %v", deleted, pruneErr)
	}
	result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil || result.Attempt != 1 || calls.Load() != 2 {
		t.Fatalf("post-prune attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}
}

func TestStore_ForgetAttemptAllowsTerminalReplay(t *testing.T) {
	db := openDatabase(t)
	store, err := inboxsqlite.New(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000014"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("terminal-replay"))
	wantErr := errors.New("terminal")
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) || result.Attempt != 1 {
		t.Fatalf("terminal attempt = %#v, %v", result, err)
	}
	if err := store.ForgetAttempt(t.Context(), key, fingerprint); err != nil {
		t.Fatalf("forget attempt: %v", err)
	}
	result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 1, func(context.Context) error { return nil })
	if err != nil || result.Attempt != 1 {
		t.Fatalf("replayed attempt = %#v, %v", result, err)
	}
}

func TestStore_RejectsFingerprintConflict(t *testing.T) {
	db := openDatabase(t)
	store, _ := inboxsqlite.New(db)
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000003"),
	}
	if _, err := store.Process(
		t.Context(), key, inbox.FingerprintEnvelope([]byte("one")), func(context.Context) error { return nil },
	); err != nil {
		t.Fatalf("first process: %v", err)
	}
	_, err := store.Process(t.Context(), key, inbox.FingerprintEnvelope([]byte("two")), func(context.Context) error {
		t.Fatal("conflicting handler ran")
		return nil
	})
	if !errors.Is(err, inbox.ErrFingerprintConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestStore_ConcurrentDuplicatesRunHandlerOnce(t *testing.T) {
	db := openDatabase(t)
	db.SetMaxOpenConns(1)
	store, _ := inboxsqlite.New(db)
	key := inbox.Key{
		ConsumerID: testConsumerID,
		Source:     testSource,
		MessageID:  mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000004"),
	}
	fingerprint := inbox.FingerprintEnvelope([]byte("same"))
	var calls atomic.Int32
	var workers sync.WaitGroup
	errorsChannel := make(chan error, 16)
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := store.Process(t.Context(), key, fingerprint, func(context.Context) error {
				calls.Add(1)
				return nil
			})
			errorsChannel <- err
		}()
	}
	workers.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d", calls.Load())
	}
}

func TestStore_PruneUsesBoundedBatch(t *testing.T) {
	db := openDatabase(t)
	store, _ := inboxsqlite.New(db)
	for index := range 3 {
		key := inbox.Key{
			ConsumerID: "worker", Source: "urn:test",
			MessageID: mustMessageID(t, "018f4f2c-4a00-7000-8000-00000000000"+string(rune('5'+index))),
		}
		if _, err := store.Process(
			t.Context(), key, inbox.FingerprintEnvelope([]byte{byte(index)}), func(context.Context) error { return nil },
		); err != nil {
			t.Fatalf("process %d: %v", index, err)
		}
	}
	deleted, err := store.Prune(t.Context(), time.Now().Add(time.Minute), 2)
	if err != nil || deleted != 2 {
		t.Fatalf("prune = %d, %v", deleted, err)
	}
}

func openDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := inboxsqlite.Migrate(t.Context(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE business_effects (
		message_id TEXT NOT NULL PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create business table: %v", err)
	}
	return db
}

func effectCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM business_effects`).Scan(&count); err != nil {
		t.Fatalf("count effects: %v", err)
	}
	return count
}

func mustMessageID(t *testing.T, value string) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID(value)
	if err != nil {
		t.Fatalf("parse message id: %v", err)
	}
	return id
}
