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
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE IF NOT EXISTS gomessenger_inbox_test_commit_parent (
		name TEXT PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create commit-failure parent fixture: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE IF NOT EXISTS gomessenger_inbox_test_commit_effect (
		name TEXT PRIMARY KEY,
		parent_name TEXT NOT NULL REFERENCES gomessenger_inbox_test_commit_parent(name)
			DEFERRABLE INITIALLY DEFERRED
	)`); err != nil {
		t.Fatalf("create commit-failure business fixture: %v", err)
	}

	t.Run("commit duplicate conflict and prune", func(t *testing.T) { testPostgresCommitAndPrune(t, database) })
	t.Run("handler rollback can retry", func(t *testing.T) { testPostgresRollbackRetry(t, database) })
	t.Run("attempt commit duplicate and conflict", func(t *testing.T) {
		testPostgresAttemptCommitAndConflict(t, database)
	})
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
	t.Run("concurrent identical attempt runs one handler", func(t *testing.T) {
		testPostgresAttemptConcurrentCommit(t, database)
	})
	t.Run("concurrent failed attempt lets waiter commit", func(t *testing.T) {
		testPostgresAttemptConcurrentRollback(t, database)
	})
	t.Run("attempt cancellation rolls back handler write", func(t *testing.T) {
		testPostgresAttemptCancellation(t, database)
	})
	t.Run("attempt finalization failures roll back handler write", func(t *testing.T) {
		testPostgresAttemptFinalizationFailures(t, database)
	})
	t.Run("batch mixed outcomes rollback and replay", func(t *testing.T) {
		testPostgresBatchOutcomes(t, database)
	})
	t.Run("custom schema and prefix", func(t *testing.T) {
		testPostgresCustomNamespace(t, database)
	})
}

func testPostgresBatchOutcomes(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	items := []inbox.BatchItem{
		postgresBatchItem(t, "018f4f2c-4a00-7000-8000-000000000071", "batch-success"),
		postgresBatchItem(t, "018f4f2c-4a00-7000-8000-000000000072", "batch-retry"),
		postgresBatchItem(t, "018f4f2c-4a00-7000-8000-000000000073", "batch-defer"),
		postgresBatchItem(t, "018f4f2c-4a00-7000-8000-000000000074", "batch-permanent"),
	}
	report, err := store.ProcessBatchAttempt(t.Context(), items, 2, func(
		ctx context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		if len(active) != 4 {
			t.Fatalf("active batch items = %d, want 4", len(active))
		}
		if err := incrementPostgresBusiness(ctx, "batch-success"); err != nil {
			return messenger.BatchResult{}, err
		}
		return messenger.BatchResult{Items: []messenger.BatchItemResult{
			{Key: postgresBatchResultKey(active[2]), Err: messenger.DeferAfter(errors.New("later"), 3*time.Second)},
			{Key: postgresBatchResultKey(active[0])},
			{Key: postgresBatchResultKey(active[3]), Err: messenger.Permanent(errors.New("bad"))},
			{Key: postgresBatchResultKey(active[1]), Err: messenger.RetryAfter(errors.New("busy"), 2*time.Second)},
		}}, nil
	})
	if err != nil || report.HandlerMessages != 4 || postgresBusinessValue(t, database, "batch-success") != 1 {
		t.Fatalf("mixed batch report=%#v value=%d error=%v", report,
			postgresBusinessValue(t, database, "batch-success"), err)
	}
	wantOutcomes := []inbox.BatchOutcome{inbox.BatchACK, inbox.BatchRetry, inbox.BatchDefer, inbox.BatchDLQ}
	wantAttempts := []uint64{1, 1, 0, 1}
	for index, outcome := range report.Items {
		if outcome.Outcome != wantOutcomes[index] || outcome.Attempt != wantAttempts[index] {
			t.Fatalf("mixed outcome %d = %#v", index, outcome)
		}
	}

	deferred := items[2]
	topErr := errors.New("whole batch retry")
	_, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{deferred}, 2, func(
		ctx context.Context,
		_ []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		if err := incrementPostgresBusiness(ctx, "batch-rollback"); err != nil {
			return messenger.BatchResult{}, err
		}
		return messenger.BatchResult{}, topErr
	})
	if !errors.Is(err, topErr) || postgresBusinessValue(t, database, "batch-rollback") != 0 {
		t.Fatalf("top-level batch error=%v value=%d", err,
			postgresBusinessValue(t, database, "batch-rollback"))
	}
	report, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{deferred}, 2, func(
		_ context.Context,
		active []inbox.BatchItem,
	) (messenger.BatchResult, error) {
		return messenger.BatchResult{Items: []messenger.BatchItemResult{{Key: postgresBatchResultKey(active[0])}}}, nil
	})
	if err != nil || report.Items[0].Attempt != 1 || report.Items[0].Outcome != inbox.BatchACK {
		t.Fatalf("batch retry after rollback = %#v, %v", report, err)
	}

	var replayCalls atomic.Int32
	report, err = store.ProcessBatchAttempt(t.Context(), []inbox.BatchItem{items[0], items[3]}, 2, func(
		context.Context,
		[]inbox.BatchItem,
	) (messenger.BatchResult, error) {
		replayCalls.Add(1)
		return messenger.BatchResult{}, nil
	})
	if err != nil || replayCalls.Load() != 0 || !report.Items[0].Duplicate ||
		report.Items[1].FailureKind != inbox.FailurePermanent {
		t.Fatalf("batch replay report=%#v calls=%d error=%v", report, replayCalls.Load(), err)
	}
}

func testPostgresCustomNamespace(t *testing.T, database *sql.DB) {
	t.Helper()
	const schema = "gomessenger_inbox_test_namespace"
	if _, err := database.ExecContext(t.Context(), `DROP SCHEMA IF EXISTS "gomessenger_inbox_test_namespace" CASCADE`); err != nil {
		t.Fatalf("drop prior custom schema: %v", err)
	}
	if err := pgsql.Migrate(
		t.Context(), database, pgsql.WithSchema(schema), pgsql.WithTablePrefix("site_"),
	); err == nil {
		t.Fatal("migration unexpectedly created a missing custom schema")
	}
	if _, err := database.ExecContext(t.Context(), `CREATE SCHEMA "gomessenger_inbox_test_namespace"`); err != nil {
		t.Fatalf("create custom schema: %v", err)
	}
	t.Cleanup(func() {
		_, err := database.ExecContext(
			context.Background(), `DROP SCHEMA IF EXISTS "gomessenger_inbox_test_namespace" CASCADE`,
		)
		if err != nil {
			t.Errorf("drop custom schema: %v", err)
		}
	})
	options := []pgsql.Option{pgsql.WithSchema(schema), pgsql.WithTablePrefix("site_")}
	if err := pgsql.Migrate(t.Context(), database, options...); err != nil {
		t.Fatalf("first custom migration: %v", err)
	}
	if err := pgsql.Migrate(t.Context(), database, options...); err != nil {
		t.Fatalf("idempotent custom migration: %v", err)
	}
	store, err := pgsql.New(database, options...)
	if err != nil {
		t.Fatalf("new custom store: %v", err)
	}

	var tableCount int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = $1 AND table_name IN ($2, $3, $4)`, schema,
		"site_inbox", "site_inbox_attempts", "site_inbox_attempt_generations").Scan(&tableCount); err != nil {
		t.Fatalf("inspect custom tables: %v", err)
	}
	if tableCount != 3 {
		t.Fatalf("custom table count = %d, want 3", tableCount)
	}
	var defaultTableCount int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = $1 AND table_name LIKE 'gomessenger_inbox%'`, schema).Scan(&defaultTableCount); err != nil {
		t.Fatalf("inspect default tables in custom schema: %v", err)
	}
	if defaultTableCount != 0 {
		t.Fatalf("default tables in custom schema = %d", defaultTableCount)
	}
	var indexCount int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = $1 AND indexname = $2`, schema, "site_inbox_completed_at_idx").Scan(&indexCount); err != nil {
		t.Fatalf("inspect custom index: %v", err)
	}
	if indexCount != 1 {
		t.Fatalf("custom completed-at index count = %d, want 1", indexCount)
	}

	key := postgresKey(t, "custom-namespace")
	fingerprint := postgresFingerprint("custom-namespace")
	var calls atomic.Int32
	handler := func(context.Context) error {
		calls.Add(1)
		return nil
	}
	if result, processErr := store.Process(t.Context(), key, fingerprint, handler); processErr != nil || result.Duplicate {
		t.Fatalf("custom process = %#v, %v", result, processErr)
	}
	if result, processErr := store.Process(t.Context(), key, fingerprint, handler); processErr != nil ||
		!result.Duplicate || calls.Load() != 1 {
		t.Fatalf("custom duplicate = %#v, calls=%d, error=%v", result, calls.Load(), processErr)
	}

	attemptKey := postgresKey(t, "custom-namespace-attempt")
	if result, processErr := store.ProcessAttempt(
		t.Context(), attemptKey, postgresFingerprint("custom-namespace-attempt"), 2,
		func(context.Context) error { return nil },
	); processErr != nil || result.Attempt != 1 {
		t.Fatalf("custom attempt = %#v, %v", result, processErr)
	}
	generationKey := postgresKey(t, "custom-namespace-attempt-generation")
	generationKey.AttemptGeneration = "gm-custom-replay"
	if result, processErr := store.ProcessAttempt(
		t.Context(), generationKey, postgresFingerprint("custom-namespace-attempt-generation"), 2,
		func(context.Context) error { return nil },
	); processErr != nil || result.Attempt != 1 {
		t.Fatalf("custom attempt generation = %#v, %v", result, processErr)
	}
	if pruned, pruneErr := store.Prune(t.Context(), time.Now().Add(time.Minute), 10); pruneErr != nil || pruned != 3 {
		t.Fatalf("custom prune = %d, %v", pruned, pruneErr)
	}
}

func testPostgresAttemptCommitAndConflict(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "attempt-commit")
	fingerprint := postgresFingerprint("attempt-commit")
	var calls atomic.Int32
	handler := func(ctx context.Context) error {
		calls.Add(1)
		return incrementPostgresBusiness(ctx, "attempt-commit")
	}
	result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, handler)
	if err != nil || result.Duplicate || result.Attempt != 1 {
		t.Fatalf("fresh attempt = %#v, %v", result, err)
	}
	result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 3, handler)
	if err != nil || !result.Duplicate || result.Attempt != 1 || calls.Load() != 1 {
		t.Fatalf("duplicate attempt = %#v, calls=%d, error=%v", result, calls.Load(), err)
	}
	_, err = store.ProcessAttempt(
		t.Context(), key, postgresFingerprint("attempt-conflict"), 3,
		func(context.Context) error {
			t.Fatal("handler ran for a fingerprint conflict")
			return nil
		},
	)
	if !errors.Is(err, inbox.ErrFingerprintConflict) || calls.Load() != 1 ||
		postgresBusinessValue(t, database, "attempt-commit") != 1 {
		t.Fatalf("attempt conflict calls=%d value=%d error=%v", calls.Load(),
			postgresBusinessValue(t, database, "attempt-commit"), err)
	}
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
		return messenger.Permanent(messenger.DeferAfter(cause, time.Second))
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

type postgresAttemptOutcome struct {
	result inbox.Result
	err    error
}

func testPostgresAttemptConcurrentCommit(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "attempt-concurrent-commit")
	fingerprint := postgresFingerprint("attempt-concurrent-commit")
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	outcomes := make(chan postgresAttemptOutcome, 2)
	go func() {
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			calls.Add(1)
			close(started)
			<-release
			return incrementPostgresBusiness(ctx, "attempt-concurrent-commit")
		})
		outcomes <- postgresAttemptOutcome{result: result, err: err}
	}()
	<-started
	go func() {
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			calls.Add(1)
			return incrementPostgresBusiness(ctx, "attempt-concurrent-commit")
		})
		outcomes <- postgresAttemptOutcome{result: result, err: err}
	}()
	close(release)
	duplicates := 0
	for range 2 {
		outcome := <-outcomes
		if outcome.err != nil || outcome.result.Attempt != 1 {
			t.Fatalf("concurrent attempt outcome = %#v, %v", outcome.result, outcome.err)
		}
		if outcome.result.Duplicate {
			duplicates++
		}
	}
	if duplicates != 1 || calls.Load() != 1 ||
		postgresBusinessValue(t, database, "attempt-concurrent-commit") != 1 {
		t.Fatalf("duplicates=%d calls=%d value=%d", duplicates, calls.Load(),
			postgresBusinessValue(t, database, "attempt-concurrent-commit"))
	}
}

func testPostgresAttemptConcurrentRollback(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "attempt-concurrent-rollback")
	fingerprint := postgresFingerprint("attempt-concurrent-rollback")
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("first attempt rollback")
	outcomes := make(chan postgresAttemptOutcome, 2)
	go func() {
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			if writeErr := incrementPostgresBusiness(ctx, "attempt-concurrent-rollback"); writeErr != nil {
				return writeErr
			}
			close(started)
			<-release
			return wantErr
		})
		outcomes <- postgresAttemptOutcome{result: result, err: err}
	}()
	<-started
	go func() {
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			return incrementPostgresBusiness(ctx, "attempt-concurrent-rollback")
		})
		outcomes <- postgresAttemptOutcome{result: result, err: err}
	}()
	close(release)
	failed := false
	succeeded := false
	for range 2 {
		outcome := <-outcomes
		switch {
		case errors.Is(outcome.err, wantErr):
			failed = outcome.result.Attempt == 1 && !outcome.result.Duplicate
		case outcome.err == nil:
			succeeded = outcome.result.Attempt == 2 && !outcome.result.Duplicate
		default:
			t.Fatalf("unexpected concurrent attempt error: %v", outcome.err)
		}
	}
	if !failed || !succeeded || postgresBusinessValue(t, database, "attempt-concurrent-rollback") != 1 {
		t.Fatalf("failed=%t succeeded=%t value=%d", failed, succeeded,
			postgresBusinessValue(t, database, "attempt-concurrent-rollback"))
	}
}

func testPostgresAttemptCancellation(t *testing.T, database *sql.DB) {
	t.Helper()
	resetPostgresFixtures(t, database)
	store := newPostgresStore(t, database)
	key := postgresKey(t, "attempt-cancellation")
	fingerprint := postgresFingerprint("attempt-cancellation")
	ctx, cancel := context.WithCancel(t.Context())
	result, err := store.ProcessAttempt(ctx, key, fingerprint, 3, func(handlerCtx context.Context) error {
		if writeErr := incrementPostgresBusiness(handlerCtx, "attempt-cancellation"); writeErr != nil {
			return writeErr
		}
		cancel()
		return nil
	})
	if err == nil || result.Attempt != 0 || postgresBusinessValue(t, database, "attempt-cancellation") != 0 {
		t.Fatalf("cancelled attempt = %#v, value=%d, error=%v", result,
			postgresBusinessValue(t, database, "attempt-cancellation"), err)
	}
	assertPostgresAttemptStateAbsent(t, database, key)
	result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(handlerCtx context.Context) error {
		return incrementPostgresBusiness(handlerCtx, "attempt-cancellation")
	})
	if err != nil || result.Attempt != 1 || result.Duplicate ||
		postgresBusinessValue(t, database, "attempt-cancellation") != 1 {
		t.Fatalf("retry after cancellation = %#v, value=%d, error=%v", result,
			postgresBusinessValue(t, database, "attempt-cancellation"), err)
	}
}

func testPostgresAttemptFinalizationFailures(t *testing.T, database *sql.DB) {
	t.Helper()
	t.Run("mark complete", func(t *testing.T) {
		resetPostgresFixtures(t, database)
		if _, err := database.ExecContext(t.Context(), `CREATE OR REPLACE FUNCTION
			gomessenger_inbox_test_reject_mark() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				RAISE EXCEPTION 'forced Inbox mark failure';
				RETURN NEW;
			END
			$$`); err != nil {
			t.Fatalf("create mark-failure function: %v", err)
		}
		if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER gomessenger_inbox_test_reject_mark
			BEFORE UPDATE OF completed_at ON gomessenger_inbox
			FOR EACH ROW WHEN (NEW.consumer_id = 'postgres-attempt-mark-failure')
			EXECUTE FUNCTION gomessenger_inbox_test_reject_mark()`); err != nil {
			t.Fatalf("create mark-failure trigger: %v", err)
		}
		t.Cleanup(func() {
			if _, err := database.ExecContext(context.Background(),
				`DROP TRIGGER IF EXISTS gomessenger_inbox_test_reject_mark ON gomessenger_inbox`); err != nil {
				t.Errorf("drop mark-failure trigger: %v", err)
			}
			if _, err := database.ExecContext(context.Background(),
				`DROP FUNCTION IF EXISTS gomessenger_inbox_test_reject_mark()`); err != nil {
				t.Errorf("drop mark-failure function: %v", err)
			}
		})

		key := postgresKey(t, "attempt-mark-failure")
		result, err := newPostgresStore(t, database).ProcessAttempt(
			t.Context(), key, postgresFingerprint("attempt-mark-failure"), 3,
			func(ctx context.Context) error { return incrementPostgresBusiness(ctx, "attempt-mark-failure") },
		)
		if err == nil || result.Attempt != 0 || postgresBusinessValue(t, database, "attempt-mark-failure") != 0 {
			t.Fatalf("mark failure = %#v, value=%d, error=%v", result,
				postgresBusinessValue(t, database, "attempt-mark-failure"), err)
		}
		assertPostgresAttemptStateAbsent(t, database, key)
	})

	t.Run("commit", func(t *testing.T) {
		resetPostgresFixtures(t, database)
		store := newPostgresStore(t, database)
		key := postgresKey(t, "attempt-commit-failure")
		fingerprint := postgresFingerprint("attempt-commit-failure")
		result, err := store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			tx, ok := inbox.SQLTxFromContext(ctx)
			if !ok {
				return errors.New("missing PostgreSQL inbox transaction")
			}
			_, execErr := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox_test_commit_effect(name, parent_name)
				VALUES ($1, $2)`, "attempt-commit-failure", "missing-parent")
			return execErr
		})
		if err == nil || result.Attempt != 0 || postgresCommitEffectCount(t, database) != 0 {
			t.Fatalf("commit failure = %#v, effects=%d, error=%v", result,
				postgresCommitEffectCount(t, database), err)
		}
		assertPostgresAttemptStateAbsent(t, database, key)

		if _, err := database.ExecContext(t.Context(),
			`INSERT INTO gomessenger_inbox_test_commit_parent(name) VALUES ($1)`, "missing-parent"); err != nil {
			t.Fatalf("insert commit-failure parent: %v", err)
		}
		result, err = store.ProcessAttempt(t.Context(), key, fingerprint, 3, func(ctx context.Context) error {
			tx, ok := inbox.SQLTxFromContext(ctx)
			if !ok {
				return errors.New("missing PostgreSQL inbox transaction")
			}
			_, execErr := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox_test_commit_effect(name, parent_name)
				VALUES ($1, $2)`, "attempt-commit-failure", "missing-parent")
			return execErr
		})
		if err != nil || result.Attempt != 1 || result.Duplicate || postgresCommitEffectCount(t, database) != 1 {
			t.Fatalf("retry after commit failure = %#v, effects=%d, error=%v", result,
				postgresCommitEffectCount(t, database), err)
		}
	})
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
		gomessenger_inbox,
		gomessenger_inbox_test_commit_effect,
		gomessenger_inbox_test_commit_parent,
		gomessenger_inbox_test_business`); err != nil {
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

func postgresBatchItem(t *testing.T, id, fingerprint string) inbox.BatchItem {
	t.Helper()
	messageID, err := messenger.ParseMessageID(id)
	if err != nil {
		t.Fatalf("parse batch message ID: %v", err)
	}
	return inbox.BatchItem{Key: inbox.Key{
		ConsumerID: "postgres-batch", Source: "urn:service:test", MessageID: messageID,
	}, Fingerprint: postgresFingerprint(fingerprint)}
}

func postgresBatchResultKey(item inbox.BatchItem) messenger.BatchItemKey {
	return messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
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

func postgresCommitEffectCount(t *testing.T, database *sql.DB) int64 {
	t.Helper()
	var count int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM gomessenger_inbox_test_commit_effect`).Scan(&count); err != nil {
		t.Fatalf("count PostgreSQL commit effects: %v", err)
	}
	return count
}

func assertPostgresAttemptStateAbsent(t *testing.T, database *sql.DB, key inbox.Key) {
	t.Helper()
	var identities int64
	var attempts int64
	if err := database.QueryRowContext(t.Context(), `SELECT
		(SELECT COUNT(*) FROM gomessenger_inbox
			WHERE consumer_id = $1 AND source = $2 AND message_id = $3),
		(SELECT COUNT(*) FROM gomessenger_inbox_attempts
			WHERE consumer_id = $1 AND source = $2 AND message_id = $3) +
		(SELECT COUNT(*) FROM gomessenger_inbox_attempt_generations
			WHERE consumer_id = $1 AND source = $2 AND message_id = $3)`,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&identities, &attempts); err != nil {
		t.Fatalf("inspect PostgreSQL attempt state: %v", err)
	}
	if identities != 0 || attempts != 0 {
		t.Fatalf("PostgreSQL attempt state remained: identities=%d attempts=%d", identities, attempts)
	}
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
