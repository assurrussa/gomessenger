package sqlite

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

//go:embed migrations/*.sql
var migrations embed.FS

type backend struct {
	db         *sql.DB
	clock      func() time.Time
	names      namespace
	statements statements
}

type attemptDecision struct {
	attempt   uint64
	terminal  bool
	exhausted bool
	exists    bool
}

type attemptState struct {
	kind     string
	attempt  uint64
	terminal bool
}

// New constructs a SQLite inbox store. The caller owns db, migrations, and
// connection-pool sizing.
func New(db *sql.DB, options ...Option) (*inbox.Store, error) {
	if db == nil {
		return nil, errors.New("inbox/sqlite: nil database")
	}
	names, err := resolveNamespace(options...)
	if err != nil {
		return nil, err
	}
	return inbox.New(&backend{db: db, clock: time.Now, names: names, statements: newStatements(names)})
}

// Migrate applies the embedded additive inbox schema.
func Migrate(ctx context.Context, db *sql.DB, options ...Option) error {
	if db == nil {
		return errors.New("inbox/sqlite: nil database")
	}
	names, err := resolveNamespace(options...)
	if err != nil {
		return err
	}
	paths, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("inbox/sqlite: list migrations: %w", err)
	}
	for _, path := range paths {
		data, readErr := migrations.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("inbox/sqlite: read migration %s: %w", path, readErr)
		}
		if _, execErr := db.ExecContext(ctx, names.render(string(data))); execErr != nil {
			return fmt.Errorf("inbox/sqlite: apply migration %s: %w", path, execErr)
		}
	}
	return nil
}

func (b *backend) Process(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handler inbox.Handler,
) (result inbox.Result, err error) {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return result, fmt.Errorf("inbox/sqlite: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	inserted, err := tx.ExecContext(ctx, b.statements.insertIdentity,
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:], b.clock().UTC())
	if err != nil {
		return result, fmt.Errorf("inbox/sqlite: insert identity: %w", err)
	}
	rows, err := inserted.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("inbox/sqlite: inserted rows: %w", err)
	}
	if rows == 0 {
		duplicate, handled, err := b.handleExisting(ctx, tx, key, fingerprint)
		if handled || err != nil {
			return duplicate, err
		}
	}

	handlerContext := inbox.ContextWithSQLTx(ctx, tx)
	if err := handler(handlerContext); err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, b.statements.markComplete,
		b.clock().UTC(), key.ConsumerID, key.Source, key.MessageID.String()); err != nil {
		return result, fmt.Errorf("inbox/sqlite: mark complete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("inbox/sqlite: commit: %w", err)
	}
	return inbox.Result{}, nil
}

func (b *backend) ProcessAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	maxAttempts uint64,
	handler inbox.Handler,
) (result inbox.Result, err error) {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return result, fmt.Errorf("inbox/sqlite: begin attempt transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	duplicate, handled, err := b.prepareAttemptIdentity(ctx, tx, key, fingerprint)
	if handled || err != nil {
		return duplicate, err
	}

	decision, err := b.nextAttempt(ctx, tx, key, fingerprint, maxAttempts)
	if err != nil {
		return result, err
	}
	attempt := decision.attempt
	if decision.terminal || decision.exhausted {
		if err := b.recordTerminal(ctx, tx, key, fingerprint, decision.attempt, decision.terminal); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("inbox/sqlite: commit existing attempt state: %w", err)
		}
		if decision.terminal {
			return inbox.Result{Attempt: attempt}, messenger.Permanent(inbox.ErrAttemptTerminal)
		}
		return inbox.Result{Attempt: attempt}, inbox.ErrAttemptsExhausted
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT gomessenger_handler"); err != nil {
		return result, fmt.Errorf("inbox/sqlite: create handler savepoint: %w", err)
	}

	handlerContext := inbox.ContextWithSQLTx(ctx, tx)
	if handlerErr := handler(handlerContext); handlerErr != nil {
		return b.finishFailedAttempt(ctx, tx, key, fingerprint, decision, maxAttempts, handlerErr)
	}
	if err := b.consumeAttempt(ctx, tx, key, fingerprint, decision); err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, b.statements.markComplete,
		b.clock().UTC(), key.ConsumerID, key.Source, key.MessageID.String()); err != nil {
		return result, fmt.Errorf("inbox/sqlite: mark attempt complete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("inbox/sqlite: commit handler attempt: %w", err)
	}
	return inbox.Result{Attempt: attempt}, nil
}

func (b *backend) finishFailedAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	decision attemptDecision,
	maxAttempts uint64,
	handlerErr error,
) (inbox.Result, error) {
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT gomessenger_handler"); err != nil {
		return inbox.Result{}, fmt.Errorf("inbox/sqlite: rollback handler writes: %w", err)
	}
	if _, deferred := messenger.DeferDelay(handlerErr); deferred && !messenger.IsPermanent(handlerErr) {
		if err := b.terminalSQL().PreserveDeferred(ctx, tx, []inbox.BatchItemOutcome{{
			Key: key, Fingerprint: fingerprint, Outcome: inbox.BatchDefer, Attempt: decision.attempt - 1,
		}}, b.clock().UTC()); err != nil {
			return inbox.Result{}, err
		}
		if err := tx.Commit(); err != nil {
			return inbox.Result{}, fmt.Errorf("inbox/sqlite: commit deferred handler attempt: %w", err)
		}
		return inbox.Result{Attempt: decision.attempt - 1}, handlerErr
	}
	if err := b.consumeAttempt(ctx, tx, key, fingerprint, decision); err != nil {
		return inbox.Result{}, err
	}
	if err := b.markAttemptTerminal(ctx, tx, key, fingerprint, handlerErr); err != nil {
		return inbox.Result{}, err
	}
	if messenger.IsPermanent(handlerErr) || decision.attempt >= maxAttempts {
		if err := b.recordTerminal(ctx, tx, key, fingerprint, decision.attempt, messenger.IsPermanent(handlerErr)); err != nil {
			return inbox.Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return inbox.Result{}, fmt.Errorf("inbox/sqlite: commit failed handler attempt: %w", err)
	}
	return inbox.Result{Attempt: decision.attempt}, handlerErr
}

func (b *backend) markAttemptTerminal(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handlerErr error,
) error {
	if !messenger.IsPermanent(handlerErr) {
		return nil
	}
	attemptFingerprint := inbox.AttemptFingerprint(key, fingerprint)
	var err error
	if key.AttemptGeneration == "" {
		_, err = tx.ExecContext(ctx, b.statements.markTerminal, b.clock().UTC(),
			key.ConsumerID, key.Source, key.MessageID.String())
	} else {
		_, err = tx.ExecContext(ctx, b.statements.markTerminalGeneration, b.clock().UTC(),
			key.ConsumerID, key.Source, key.MessageID.String(), attemptFingerprint[:])
	}
	if err != nil {
		return fmt.Errorf("inbox/sqlite: mark handler attempt terminal: %w", err)
	}
	return nil
}

func (b *backend) prepareAttemptIdentity(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) (inbox.Result, bool, error) {
	inserted, err := tx.ExecContext(ctx, b.statements.insertIdentity,
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:], b.clock().UTC())
	if err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: insert attempt identity: %w", err)
	}
	rows, err := inserted.RowsAffected()
	if err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: attempt identity rows: %w", err)
	}
	if rows != 0 {
		return inbox.Result{}, false, nil
	}
	var stored []byte
	var completedAt sql.NullTime
	if err := tx.QueryRowContext(ctx, b.statements.readIdentity,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completedAt); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: read attempt identity: %w", err)
	}
	if !fingerprintsEqual(stored, fingerprint) {
		return inbox.Result{}, false, inbox.ErrFingerprintConflict
	}
	if !completedAt.Valid {
		return inbox.Result{}, false, nil
	}
	state, err := b.readAttempt(ctx, tx, key, inbox.AttemptFingerprint(key, fingerprint))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return inbox.Result{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: commit attempt duplicate: %w", err)
	}
	return inbox.Result{Duplicate: true, Attempt: state.attempt}, true, nil
}

func (b *backend) nextAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	maxAttempts uint64,
) (attemptDecision, error) {
	attemptFingerprint := inbox.AttemptFingerprint(key, fingerprint)
	state, err := b.readAttempt(ctx, tx, key, attemptFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return attemptDecision{attempt: 1}, nil
	}
	if err != nil {
		return attemptDecision{}, err
	}
	if state.terminal || state.kind == inbox.FailurePermanent {
		return attemptDecision{attempt: state.attempt, terminal: true, exists: true}, nil
	}
	if state.attempt >= maxAttempts || state.kind == inbox.FailureAttemptsExhausted {
		return attemptDecision{attempt: state.attempt, exhausted: true, exists: true}, nil
	}
	return attemptDecision{attempt: state.attempt + 1, exists: true}, nil
}

func (b *backend) consumeAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	decision attemptDecision,
) error {
	attemptFingerprint := inbox.AttemptFingerprint(key, fingerprint)
	if !decision.exists {
		if err := b.insertAttempt(ctx, tx, key, attemptFingerprint); err != nil {
			return fmt.Errorf("inbox/sqlite: insert handler attempt: %w", err)
		}
		return nil
	}
	if err := b.incrementAttempt(ctx, tx, key, attemptFingerprint); err != nil {
		return fmt.Errorf("inbox/sqlite: increment handler attempt: %w", err)
	}
	return nil
}

func (b *backend) insertAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	if key.AttemptGeneration == "" {
		_, err := tx.ExecContext(ctx, b.statements.insertAttempt, key.ConsumerID, key.Source,
			key.MessageID.String(), fingerprint[:], b.clock().UTC())
		return err
	}
	_, err := tx.ExecContext(ctx, b.statements.insertAttemptGeneration, key.ConsumerID, key.Source,
		key.MessageID.String(), fingerprint[:], b.clock().UTC())
	return err
}

func (b *backend) incrementAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	if key.AttemptGeneration == "" {
		_, err := tx.ExecContext(ctx, b.statements.incrementAttempt, b.clock().UTC(),
			key.ConsumerID, key.Source, key.MessageID.String())
		return err
	}
	_, err := tx.ExecContext(ctx, b.statements.incrementAttemptGeneration, b.clock().UTC(),
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:])
	return err
}

func (b *backend) readAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) (attemptState, error) {
	var attempt int64
	var terminal bool
	var kind string
	var err error
	if key.AttemptGeneration == "" {
		err = tx.QueryRowContext(ctx, b.statements.readAttempt,
			key.ConsumerID, key.Source, key.MessageID.String()).Scan(&attempt, &terminal, &kind)
	} else {
		err = tx.QueryRowContext(ctx, b.statements.readAttemptGeneration,
			key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:]).Scan(&attempt, &terminal, &kind)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return attemptState{}, sql.ErrNoRows
		}
		return attemptState{}, fmt.Errorf("inbox/sqlite: read handler attempt: %w", err)
	}
	if attempt < 0 {
		return attemptState{}, errors.New("inbox/sqlite: invalid stored handler attempt")
	}
	return attemptState{attempt: uint64(attempt), terminal: terminal, kind: kind}, nil
}

func (b *backend) ForgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("inbox/sqlite: begin forget attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var stored []byte
	var completedAt sql.NullTime
	err = tx.QueryRowContext(ctx, b.statements.readIdentity,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("inbox/sqlite: commit missing forgotten attempt: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inbox/sqlite: read forgotten attempt: %w", err)
	}
	if !fingerprintsEqual(stored, fingerprint) {
		return inbox.ErrFingerprintConflict
	}
	if completedAt.Valid {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("inbox/sqlite: commit completed forgotten attempt: %w", err)
		}
		return nil
	}
	if err := b.terminalSQL().Forget(ctx, tx, key, fingerprint); err != nil {
		return err
	}
	attemptFingerprint := inbox.AttemptFingerprint(key, fingerprint)
	if err := b.deleteAttempt(ctx, tx, key, attemptFingerprint); err != nil {
		return fmt.Errorf("inbox/sqlite: delete handler attempt: %w", err)
	}
	hasAttempts, err := b.hasAttempts(ctx, tx, key)
	if err != nil {
		return err
	}
	if hasAttempts {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("inbox/sqlite: commit forgotten attempt generation: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, b.statements.deleteIncompleteIdentity,
		key.ConsumerID, key.Source, key.MessageID.String()); err != nil {
		return fmt.Errorf("inbox/sqlite: delete incomplete identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("inbox/sqlite: commit forgotten attempt: %w", err)
	}
	return nil
}

func (b *backend) deleteAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	if key.AttemptGeneration == "" {
		_, err := tx.ExecContext(ctx, b.statements.deleteAttempt,
			key.ConsumerID, key.Source, key.MessageID.String())
		return err
	}
	_, err := tx.ExecContext(ctx, b.statements.deleteAttemptGeneration,
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:])
	return err
}

func (b *backend) hasAttempts(ctx context.Context, tx *sql.Tx, key inbox.Key) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, b.statements.hasAttempts,
		key.ConsumerID, key.Source, key.MessageID.String(),
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("inbox/sqlite: inspect remaining handler attempts: %w", err)
	}
	return exists, nil
}

func (b *backend) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, fmt.Errorf("inbox/sqlite: begin prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := b.terminalSQL().PruneCompleted(ctx, tx, before, limit); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, b.statements.pruneAttemptGenerations, before, limit); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: prune attempt generations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, b.statements.pruneAttempts, before, limit); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: prune attempts: %w", err)
	}
	result, err := tx.ExecContext(ctx, b.statements.pruneInbox, before, limit)
	if err != nil {
		return 0, fmt.Errorf("inbox/sqlite: prune: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inbox/sqlite: pruned rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: commit prune: %w", err)
	}
	return rows, nil
}

func (b *backend) handleExisting(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) (inbox.Result, bool, error) {
	var stored []byte
	var completedAt sql.NullTime
	if err := tx.QueryRowContext(ctx, b.statements.readIdentity,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completedAt); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: read identity: %w", err)
	}
	if !fingerprintsEqual(stored, fingerprint) {
		return inbox.Result{}, false, inbox.ErrFingerprintConflict
	}
	if completedAt.Valid {
		if err := tx.Commit(); err != nil {
			return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: commit duplicate: %w", err)
		}
		return inbox.Result{Duplicate: true}, true, nil
	}
	var maxTerminal int
	var count int
	if err := tx.QueryRowContext(ctx, b.statements.inspectAttempts,
		key.ConsumerID, key.Source, key.MessageID.String(),
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&maxTerminal, &count); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: inspect attempts: %w", err)
	}
	if count > 0 {
		if maxTerminal > 0 {
			return inbox.Result{}, false, messenger.Permanent(inbox.ErrAttemptTerminal)
		}
		return inbox.Result{}, false, inbox.ErrAttemptConflict
	}
	return inbox.Result{}, false, nil
}

func fingerprintsEqual(stored []byte, fingerprint inbox.Fingerprint) bool {
	return len(stored) == len(fingerprint) && subtle.ConstantTimeCompare(stored, fingerprint[:]) == 1
}
