package atomicflow

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestHandleRequiresConcreteInboxTransaction(t *testing.T) {
	message, _, err := newInput()
	if err != nil {
		t.Fatal(err)
	}
	var nilTx *sql.Tx
	for name, ctx := range map[string]context.Context{
		"missing":       t.Context(),
		"nil context":   nil,
		"wrong backend": inbox.ContextWithSQLTx(t.Context(), rejectedSQLTx{}),
		"typed nil":     inbox.ContextWithSQLTx(t.Context(), nilTx),
	} {
		t.Run(name, func(t *testing.T) {
			receipt, err := Handle(ctx, message)
			if err == nil || !receipt.MessageID.IsZero() {
				t.Fatalf("receipt=%+v error=%v", receipt, err)
			}
		})
	}
}

func TestProjectedIdentityIsStableAndSourceScoped(t *testing.T) {
	message, _, err := newInput()
	if err != nil {
		t.Fatal(err)
	}
	first := projectedID(message.Metadata)
	message.Metadata.Time = message.Metadata.Time.Add(time.Hour)
	if repeated := projectedID(message.Metadata); repeated != first {
		t.Fatalf("retry clock changed output identity: %s != %s", repeated, first)
	}
	message.Metadata.Source += ":other"
	if projectedID(message.Metadata) == first {
		t.Fatal("different incoming source reused output identity")
	}
	if first.IsZero() || first == message.Metadata.ID {
		t.Fatal("outgoing identity must be nonzero and separate from the input")
	}
	if _, err := messenger.ParseMessageID(first.String()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAtomicInboxOutbox(t *testing.T) {
	db := openPostgres(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	report, err := Run(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	wantFailure := Snapshot{InboxRows: 1, Attempts: 1}
	wantCommit := Snapshot{BusinessRows: 1, OutboxJobs: 1, OutboxKeys: 1, InboxRows: 1, Completed: 1, Attempts: 2}
	if report.AfterFailure != wantFailure || report.AfterCommit != wantCommit ||
		report.AfterRedelivery != wantCommit || report.HandlerCalls != 2 {
		t.Fatalf("atomic transaction report=%+v", report)
	}
}

func TestPostgresAtomicInboxCommitFailure(t *testing.T) {
	db := openPostgres(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE atomic_parent (order_id TEXT PRIMARY KEY);
		ALTER TABLE atomic_order_projection ADD CONSTRAINT atomic_commit_check
		FOREIGN KEY (order_id) REFERENCES atomic_parent(order_id) DEFERRABLE INITIALLY DEFERRED`); err != nil {
		t.Fatal(err)
	}
	message, envelope, err := newInput()
	if err != nil {
		t.Fatal(err)
	}
	store, err := inboxpgsql.New(db)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := func(txCtx context.Context) error {
		calls++
		receipt, handleErr := Handle(txCtx, message)
		if handleErr != nil {
			return handleErr
		}
		if receipt.State != messenger.ReceiptStaged {
			t.Fatalf("pre-commit receipt=%+v", receipt)
		}
		return nil
	}
	result, err := store.ProcessAttempt(ctx, inputKey(message), inbox.FingerprintEnvelope(envelope), 3, handler)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || result.Duplicate || calls != 1 {
		t.Fatalf("deferred commit failure: result=%+v calls=%d error=%v", result, calls, err)
	}
	state, err := snapshot(ctx, db, message)
	if err != nil {
		t.Fatal(err)
	}
	if state != (Snapshot{}) {
		t.Fatalf("commit failure retained effects or attempt accounting: %+v", state)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO atomic_parent (order_id) VALUES ($1)", message.Payload.OrderID); err != nil {
		t.Fatal(err)
	}
	result, err = store.ProcessAttempt(ctx, inputKey(message), inbox.FingerprintEnvelope(envelope), 3, handler)
	if err != nil || result.Attempt != 1 || result.Duplicate || calls != 2 {
		t.Fatalf("retry after failed commit: result=%+v calls=%d error=%v", result, calls, err)
	}
	state, err = snapshot(ctx, db, message)
	if err != nil {
		t.Fatal(err)
	}
	want := Snapshot{BusinessRows: 1, OutboxJobs: 1, OutboxKeys: 1, InboxRows: 1, Completed: 1, Attempts: 1}
	if state != want {
		t.Fatalf("successful retry state=%+v", state)
	}
}

func TestPostgresAtomicInboxStagingFailure(t *testing.T) {
	db := openPostgres(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	// An SQL error from the official stager aborts PostgreSQL's current
	// transaction until the Inbox rolls back its handler savepoint.
	if _, err := db.ExecContext(ctx, `ALTER TABLE jobs ADD CONSTRAINT reject_atomic_relay
		CHECK (name <> 'gomessenger.relay')`); err != nil {
		t.Fatal(err)
	}
	message, envelope, err := newInput()
	if err != nil {
		t.Fatal(err)
	}
	store, err := inboxpgsql.New(db)
	if err != nil {
		t.Fatal(err)
	}
	handler := func(txCtx context.Context) error {
		_, handleErr := Handle(txCtx, message)
		return handleErr
	}
	result, err := store.ProcessAttempt(ctx, inputKey(message), inbox.FingerprintEnvelope(envelope), 3, handler)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || result.Attempt != 1 || result.Duplicate {
		t.Fatalf("staging failure: result=%+v error=%v", result, err)
	}
	state, err := snapshot(ctx, db, message)
	if err != nil {
		t.Fatal(err)
	}
	if state != (Snapshot{InboxRows: 1, Attempts: 1}) {
		t.Fatalf("staging failure retained business/event data: %+v", state)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE jobs DROP CONSTRAINT reject_atomic_relay"); err != nil {
		t.Fatal(err)
	}
	result, err = store.ProcessAttempt(ctx, inputKey(message), inbox.FingerprintEnvelope(envelope), 3, handler)
	if err != nil || result.Attempt != 2 || result.Duplicate {
		t.Fatalf("retry after staging failure: result=%+v error=%v", result, err)
	}
	state, err = snapshot(ctx, db, message)
	if err != nil {
		t.Fatal(err)
	}
	want := Snapshot{BusinessRows: 1, OutboxJobs: 1, OutboxKeys: 1, InboxRows: 1, Completed: 1, Attempts: 2}
	if state != want {
		t.Fatalf("staging failure retry state=%+v", state)
	}
}

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("GOMESSENGER_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GOMESSENGER_POSTGRES_DSN is not configured")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	id, err := messenger.UUIDv7Generator().New()
	if err != nil {
		t.Fatal(err)
	}
	schema := "gm_atomic_" + strings.ReplaceAll(id.String(), "-", "")
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	// A second connection acquired while the handler runs would deadlock
	// instead of silently escaping the transaction. Bound that failure.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		//nolint:gosec // The locally generated schema is quoted as a PostgreSQL identifier.
		if _, err := db.ExecContext(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop isolated test schema: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	//nolint:gosec // The locally generated schema is quoted as a PostgreSQL identifier.
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db
}

type rejectedSQLTx struct{}

func (rejectedSQLTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	panic("must reject non-*sql.Tx before business SQL")
}

func (rejectedSQLTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	panic("must reject non-*sql.Tx before query")
}

func (rejectedSQLTx) QueryRowContext(context.Context, string, ...any) *sql.Row {
	panic("must reject non-*sql.Tx before staging")
}
