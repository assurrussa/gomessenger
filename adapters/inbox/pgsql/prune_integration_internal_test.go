package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

func TestPostgresInboxPruneAndBatchUseConsistentIdentityLockOrder(t *testing.T) {
	dsn := os.Getenv("GOMESSENGER_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GOMESSENGER_POSTGRES_DSN is not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	names, err := resolveNamespace(WithTablePrefix("prune_lock_order_"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, WithTablePrefix("prune_lock_order_")); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, table := range []string{names.terminal, names.attemptGenerations, names.attempts, names.inbox} {
			if _, err := db.ExecContext(context.Background(), "DROP TABLE "+table); err != nil {
				t.Error(err)
			}
		}
	}()
	now := time.Now().UTC()
	b := &backend{db: db, clock: func() time.Time { return now }, names: names, statements: newStatements(names)}
	items := seedPruneLockOrder(t, b)
	// Identity order is A, B, C; age order is B, A, C. Only A and B may be pruned.
	for index, age := range []time.Duration{2 * time.Hour, 3 * time.Hour, time.Hour} {
		if _, err := db.ExecContext(ctx, b.statements.markComplete, now.Add(-age),
			items[index].Key.ConsumerID, items[index].Key.Source, items[index].Key.MessageID.String()); err != nil {
			t.Fatal(err)
		}
	}
	batchTx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = batchTx.Rollback() }()
	var pid int
	if err := batchTx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	var completed sql.NullTime
	first := items[0].Key
	if err := batchTx.QueryRowContext(ctx, b.statements.lockIdentity,
		first.ConsumerID, first.Source, first.MessageID.String()).Scan(&stored, &completed); err != nil {
		t.Fatal(err)
	}
	pruned := make(chan error, 1)
	go func() {
		count, err := b.Prune(ctx, now, 2)
		if err == nil && count != 2 {
			err = errors.New("prune did not remove exactly the two oldest identities")
		}
		pruned <- err
	}()
	// Observe PostgreSQL's actual lock wait, rather than guessing when prune has started.
	waitForPruneLock(t, ctx, db, pid)
	// Bound the regression failure: the old prune holds B while waiting for A.
	if _, err := batchTx.ExecContext(ctx, "SET LOCAL lock_timeout = '500ms'"); err != nil {
		t.Fatal(err)
	}
	groups, err := prepareBatchGroups(items[:2])
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make([]inbox.BatchItemOutcome, 2)
	active, err := b.prepareBatchIdentities(ctx, batchTx, groups, outcomes, 2)
	if err != nil {
		t.Fatalf("batch could not lock B while prune waits for A: %v", err)
	}
	if len(active) != 0 || !outcomes[0].Duplicate || !outcomes[1].Duplicate {
		t.Fatalf("completed batch identities: active=%d, outcomes=%+v", len(active), outcomes)
	}
	if err := batchTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-pruned; err != nil {
		t.Fatal(err)
	}
	assertPruneLockOrderRemainder(t, ctx, db, names, items[2].Key.MessageID.String())
}

func assertPruneLockOrderRemainder(t *testing.T, ctx context.Context, db *sql.DB, names namespace, want string) {
	t.Helper()
	for _, table := range []string{names.inbox, names.attempts, names.attemptGenerations, names.terminal} {
		var count int
		var remaining string
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(message_id) FROM "+table).Scan(&count, &remaining); err != nil {
			t.Fatal(err)
		}
		if count != 1 || remaining != want {
			t.Fatalf("%s: count=%d, remaining=%s; want only C", table, count, remaining)
		}
	}
}

func waitForPruneLock(t *testing.T, ctx context.Context, db *sql.DB, pid int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := db.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("prune never waited for the first batch identity", ctx.Err())
		case <-ticker.C:
		}
	}
}

func seedPruneLockOrder(t *testing.T, b *backend) []inbox.BatchItem {
	t.Helper()
	items := make([]inbox.BatchItem, 0, 3)
	for _, rawID := range []string{
		"018f4f2c-4a00-7000-8000-000000000001",
		"018f4f2c-4a00-7000-8000-000000000002",
		"018f4f2c-4a00-7000-8000-000000000003",
	} {
		id, err := messenger.ParseMessageID(rawID)
		if err != nil {
			t.Fatal(err)
		}
		key := inbox.Key{ConsumerID: "lock-order", Source: testSource, MessageID: id}
		fingerprint := inbox.FingerprintEnvelope([]byte(rawID))
		if _, err := b.ProcessAttempt(t.Context(), key, fingerprint, 2,
			func(context.Context) error { return messenger.Permanent(errors.New("closed")) }); !messenger.IsPermanent(err) {
			t.Fatalf("seed terminal generation: %v", err)
		}
		key.AttemptGeneration = "replay"
		if _, err := b.ProcessAttempt(t.Context(), key, fingerprint, 2, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		items = append(items, inbox.BatchItem{Key: key, Fingerprint: fingerprint})
	}
	return items
}
