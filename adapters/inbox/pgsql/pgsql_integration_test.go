package pgsql_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
)

const postgresDSNEnvironment = "GOMESSENGER_POSTGRES_DSN"

func TestPostgresInboxIntegration(t *testing.T) {
	dsn := os.Getenv(postgresDSNEnvironment)
	if dsn == "" {
		t.Skip(postgresDSNEnvironment + " is not configured")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	database.SetMaxOpenConns(8)
	if err := database.PingContext(t.Context()); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if err := pgsql.Migrate(t.Context(), database); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	if err := pgsql.Migrate(t.Context(), database); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE IF NOT EXISTS gomessenger_inbox_test_business (
		name TEXT PRIMARY KEY,
		value BIGINT NOT NULL
	)`); err != nil {
		t.Fatalf("create business fixture: %v", err)
	}

	t.Run("commit duplicate conflict and prune", func(t *testing.T) { testPostgresCommitAndPrune(t, database) })
	t.Run("handler rollback can retry", func(t *testing.T) { testPostgresRollbackRetry(t, database) })
	t.Run("handler attempts survive restart", func(t *testing.T) { testPostgresDurableAttempts(t, database) })
	t.Run("permanent outcome survives restart", func(t *testing.T) { testPostgresPermanentOutcome(t, database) })
	t.Run("attempt generation starts fresh bounded cycle", func(t *testing.T) {
		testPostgresAttemptGeneration(t, database)
	})
	t.Run("concurrent identical commit runs one handler", func(t *testing.T) {
		testPostgresConcurrentCommit(t, database)
	})
	t.Run("concurrent rollback lets waiter commit", func(t *testing.T) {
		testPostgresConcurrentRollback(t, database)
	})
}

func testPostgresDurableAttempts(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	key := postgresKey(t, "durable-attempts")
	fingerprint := postgresFingerprint("durable-attempts")
	wantErr := errors.New("retry")
	for expectedAttempt := uint64(1); expectedAttempt <= 2; expectedAttempt++ {
		store := newPostgresStore(t, database)
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(ctx context.Context) error {
			if err := incrementPostgresBusiness(ctx, "durable-attempts"); err != nil {
				return err
			}
			return wantErr
		})
		if !errors.Is(err, wantErr) || result.Attempt != expectedAttempt ||
			postgresBusinessValue(t, database, "durable-attempts") != 0 {
			t.Fatalf("attempt %d result=%#v value=%d err=%v", expectedAttempt, result,
				postgresBusinessValue(t, database, "durable-attempts"), err)
		}
	}
	store := newPostgresStore(t, database)
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error {
		t.Fatal("handler ran past durable PostgreSQL attempt limit")
		return nil
	})
	if !errors.Is(err, inbox.ErrAttemptsExhausted) || result.Attempt != 2 {
		t.Fatalf("exhausted result=%#v err=%v", result, err)
	}
	if err := store.ForgetAttempt(t.Context(), key, fingerprint); err != nil {
		t.Fatalf("forget terminal attempt: %v", err)
	}
	result, err = newPostgresStore(t, database).ProcessAttempt(
		t.Context(), key, fingerprint, 2, func(context.Context) error { return nil },
	)
	if err != nil || result.Attempt != 1 {
		t.Fatalf("fresh attempt after terminal hand-off = %#v, %v", result, err)
	}
}

func testPostgresPermanentOutcome(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	key := postgresKey(t, "permanent-outcome")
	fingerprint := postgresFingerprint("permanent-outcome")
	cause := errors.New("unsupported")
	var calls atomic.Int32
	store := newPostgresStore(t, database)
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(context.Context) error {
		calls.Add(1)
		return messenger.Permanent(cause)
	})
	if !messenger.IsPermanent(err) || !errors.Is(err, cause) || result.Attempt != 1 || calls.Load() != 1 {
		t.Fatalf("first permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}

	result, err = newPostgresStore(t, database).ProcessAttempt(
		t.Context(), key, fingerprint, 3, func(context.Context) error {
			calls.Add(1)
			return nil
		},
	)
	if !messenger.IsPermanent(err) || !errors.Is(err, inbox.ErrAttemptTerminal) ||
		result.Attempt != 1 || calls.Load() != 1 {
		t.Fatalf("restored permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}

	if err := store.ForgetAttempt(t.Context(), key, fingerprint); err != nil {
		t.Fatalf("forget permanent outcome: %v", err)
	}
	result, err = newPostgresStore(t, database).ProcessAttempt(
		t.Context(), key, fingerprint, 3, func(context.Context) error {
			calls.Add(1)
			return nil
		},
	)
	if err != nil || result.Attempt != 1 || calls.Load() != 2 {
		t.Fatalf("replayed permanent attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}
}

func testPostgresAttemptGeneration(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	key := postgresKey(t, "attempt-generation")
	fingerprint := postgresFingerprint("attempt-generation")
	store := newPostgresStore(t, database)
	permanentErr := errors.New("permanent")
	if result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error {
		return messenger.Permanent(permanentErr)
	}); !errors.Is(err, permanentErr) || result.Attempt != 1 {
		t.Fatalf("original attempt = %#v, %v", result, err)
	}

	retryErr := errors.New("retry")
	replayOne := key
	replayOne.AttemptGeneration = "gm-replay-one"
	replayTwo := key
	replayTwo.AttemptGeneration = "gm-replay-two"
	processRetry := func(replayKey inbox.Key, expectedAttempt uint64) {
		t.Helper()
		result, err := store.ProcessAttempt(t.Context(), replayKey, fingerprint, 2, func(context.Context) error {
			return retryErr
		})
		if !errors.Is(err, retryErr) || result.Attempt != expectedAttempt {
			t.Fatalf("replay %q attempt %d = %#v, %v", replayKey.AttemptGeneration, expectedAttempt, result, err)
		}
	}
	processRetry(replayOne, 1)
	processRetry(replayTwo, 1)
	processRetry(replayOne, 2)
	processRetry(replayTwo, 2)
	assertExhausted := func(replayKey inbox.Key) {
		t.Helper()
		result, err := store.ProcessAttempt(t.Context(), replayKey, fingerprint, 2, func(context.Context) error {
			t.Fatalf("handler ran past PostgreSQL replay %q attempt limit", replayKey.AttemptGeneration)
			return nil
		})
		if !errors.Is(err, inbox.ErrAttemptsExhausted) || result.Attempt != 2 {
			t.Fatalf("exhausted replay %q = %#v, %v", replayKey.AttemptGeneration, result, err)
		}
	}
	assertExhausted(replayOne)
	assertExhausted(replayTwo)

	if err := store.ForgetAttempt(t.Context(), replayOne, fingerprint); err != nil {
		t.Fatalf("forget first replay generation: %v", err)
	}
	var identities int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM gomessenger_inbox
	    WHERE consumer_id = $1 AND source = $2 AND message_id = $3`, key.ConsumerID, key.Source,
		key.MessageID.String()).Scan(&identities); err != nil || identities != 1 {
		t.Fatalf("identity after generation cleanup = %d, %v", identities, err)
	}
	assertExhausted(replayTwo)

	key.AttemptGeneration = "gm-replay-three"
	if result, err := store.ProcessAttempt(
		t.Context(), key, fingerprint, 2, func(context.Context) error { return nil },
	); err != nil || result.Attempt != 1 {
		t.Fatalf("next replay generation = %#v, %v", result, err)
	}
}

func testPostgresCommitAndPrune(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "commit")
	fingerprint := postgresFingerprint("same")
	var calls atomic.Int32
	handler := func(ctx context.Context) error {
		calls.Add(1)
		return incrementPostgresBusiness(ctx, "commit")
	}
	result, err := store.Process(t.Context(), key, fingerprint, handler)
	if err != nil || result.Duplicate {
		t.Fatalf("first process = %#v, %v", result, err)
	}
	result, err = store.Process(t.Context(), key, fingerprint, handler)
	if err != nil || !result.Duplicate || calls.Load() != 1 {
		t.Fatalf("duplicate process = %#v, calls=%d, err=%v", result, calls.Load(), err)
	}
	if _, err := store.Process(t.Context(), key, postgresFingerprint("different"), handler); !errors.Is(
		err, inbox.ErrFingerprintConflict,
	) {
		t.Fatalf("fingerprint conflict = %v", err)
	}
	pruned, err := store.Prune(t.Context(), time.Now().Add(time.Hour), 10)
	if err != nil || pruned != 1 {
		t.Fatalf("prune = %d, %v", pruned, err)
	}
}

func testPostgresRollbackRetry(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "rollback")
	wantErr := errors.New("retry")
	_, err := store.Process(t.Context(), key, postgresFingerprint("rollback"), func(ctx context.Context) error {
		if err := incrementPostgresBusiness(ctx, "rollback"); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) || postgresBusinessValue(t, database, "rollback") != 0 {
		t.Fatalf("rollback error=%v value=%d", err, postgresBusinessValue(t, database, "rollback"))
	}
	result, err := store.Process(t.Context(), key, postgresFingerprint("rollback"), func(ctx context.Context) error {
		return incrementPostgresBusiness(ctx, "rollback")
	})
	if err != nil || result.Duplicate || postgresBusinessValue(t, database, "rollback") != 1 {
		t.Fatalf("retry result=%#v value=%d err=%v", result, postgresBusinessValue(t, database, "rollback"), err)
	}
}

func testPostgresConcurrentCommit(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "concurrent-commit")
	fingerprint := postgresFingerprint("concurrent-commit")
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	results := make(chan inbox.Result, 2)
	errorsChannel := make(chan error, 2)
	go func() {
		result, err := store.Process(t.Context(), key, fingerprint, func(ctx context.Context) error {
			calls.Add(1)
			close(started)
			<-release
			return incrementPostgresBusiness(ctx, "concurrent-commit")
		})
		results <- result
		errorsChannel <- err
	}()
	<-started
	go func() {
		result, err := store.Process(t.Context(), key, fingerprint, func(ctx context.Context) error {
			calls.Add(1)
			return incrementPostgresBusiness(ctx, "concurrent-commit")
		})
		results <- result
		errorsChannel <- err
	}()
	close(release)
	assertConcurrentResults(t, results, errorsChannel, true)
	if calls.Load() != 1 || postgresBusinessValue(t, database, "concurrent-commit") != 1 {
		t.Fatalf("calls=%d value=%d", calls.Load(), postgresBusinessValue(t, database, "concurrent-commit"))
	}
}

func testPostgresConcurrentRollback(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "concurrent-rollback")
	fingerprint := postgresFingerprint("concurrent-rollback")
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("first rollback")
	results := make(chan inbox.Result, 2)
	errorsChannel := make(chan error, 2)
	go func() {
		result, err := store.Process(t.Context(), key, fingerprint, func(context.Context) error {
			close(started)
			<-release
			return wantErr
		})
		results <- result
		errorsChannel <- err
	}()
	<-started
	go func() {
		result, err := store.Process(t.Context(), key, fingerprint, func(ctx context.Context) error {
			return incrementPostgresBusiness(ctx, "concurrent-rollback")
		})
		results <- result
		errorsChannel <- err
	}()
	close(release)
	assertConcurrentResults(t, results, errorsChannel, false)
	if postgresBusinessValue(t, database, "concurrent-rollback") != 1 {
		t.Fatalf("value=%d", postgresBusinessValue(t, database, "concurrent-rollback"))
	}
}

func newPostgresStore(t *testing.T, database *sql.DB) *inbox.Store {
	t.Helper()
	store, err := pgsql.New(database)
	if err != nil {
		t.Fatalf("new PostgreSQL inbox: %v", err)
	}
	return store
}

func resetPostgresFixtures(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.ExecContext(t.Context(), `TRUNCATE gomessenger_inbox_attempt_generations,
		gomessenger_inbox_attempts,
		gomessenger_inbox, gomessenger_inbox_test_business`); err != nil {
		t.Fatalf("reset PostgreSQL fixtures: %v", err)
	}
}

func postgresKey(t *testing.T, suffix string) inbox.Key {
	t.Helper()
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("parse message ID: %v", err)
	}
	return inbox.Key{ConsumerID: "postgres-" + suffix, Source: "urn:service:test", MessageID: id}
}

func postgresFingerprint(value string) inbox.Fingerprint {
	return inbox.Fingerprint(sha256.Sum256([]byte(value)))
}

func incrementPostgresBusiness(ctx context.Context, name string) error {
	tx, ok := inbox.SQLTxFromContext(ctx)
	if !ok {
		return errors.New("missing PostgreSQL inbox transaction")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox_test_business(name, value)
		VALUES ($1, 1)
		ON CONFLICT (name) DO UPDATE SET value = gomessenger_inbox_test_business.value + 1`, name)
	return err
}

func postgresBusinessValue(t *testing.T, database *sql.DB, name string) int64 {
	t.Helper()
	var value int64
	err := database.QueryRowContext(t.Context(),
		`SELECT value FROM gomessenger_inbox_test_business WHERE name = $1`, name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("read PostgreSQL business value: %v", err)
	}
	return value
}

func assertConcurrentResults(
	t *testing.T,
	results <-chan inbox.Result,
	errorsChannel <-chan error,
	wantDuplicate bool,
) {
	t.Helper()
	var duplicateCount int
	var errorsSeen []error
	for range 2 {
		if result := <-results; result.Duplicate {
			duplicateCount++
		}
		if err := <-errorsChannel; err != nil {
			errorsSeen = append(errorsSeen, err)
		}
	}
	if wantDuplicate {
		if duplicateCount != 1 || len(errorsSeen) != 0 {
			t.Fatalf("duplicates=%d errors=%v", duplicateCount, errorsSeen)
		}
		return
	}
	if duplicateCount != 0 || len(errorsSeen) != 1 {
		t.Fatalf("duplicates=%d errors=%v", duplicateCount, errorsSeen)
	}
}
