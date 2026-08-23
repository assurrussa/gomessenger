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
	db    *sql.DB
	clock func() time.Time
}

type attemptDecision struct {
	attempt   uint64
	terminal  bool
	exhausted bool
}

type attemptState struct {
	attempt  uint64
	terminal bool
}

// New constructs a SQLite inbox store. The caller owns db and migrations.
func New(db *sql.DB) (*inbox.Store, error) {
	if db == nil {
		return nil, errors.New("inbox/sqlite: nil database")
	}
	return inbox.New(&backend{db: db, clock: time.Now})
}

// Migrate applies the embedded additive inbox schema.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("inbox/sqlite: nil database")
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
		if _, execErr := db.ExecContext(ctx, string(data)); execErr != nil {
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

	inserted, err := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox
        (consumer_id, source, message_id, fingerprint, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING`,
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
	if _, err := tx.ExecContext(ctx, `UPDATE gomessenger_inbox SET completed_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
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
		return b.finishFailedAttempt(ctx, tx, key, fingerprint, attempt, handlerErr)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT gomessenger_handler"); err != nil {
		return result, fmt.Errorf("inbox/sqlite: release handler savepoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gomessenger_inbox SET completed_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
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
	attempt uint64,
	handlerErr error,
) (inbox.Result, error) {
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT gomessenger_handler"); err != nil {
		return inbox.Result{}, fmt.Errorf("inbox/sqlite: rollback handler writes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT gomessenger_handler"); err != nil {
		return inbox.Result{}, fmt.Errorf("inbox/sqlite: release failed handler savepoint: %w", err)
	}
	if err := b.markAttemptTerminal(ctx, tx, key, fingerprint, handlerErr); err != nil {
		return inbox.Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return inbox.Result{}, fmt.Errorf("inbox/sqlite: commit failed handler attempt: %w", err)
	}
	return inbox.Result{Attempt: attempt}, handlerErr
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
		_, err = tx.ExecContext(ctx, `UPDATE gomessenger_inbox_attempts
	        SET terminal = 1, updated_at = ?
	        WHERE consumer_id = ? AND source = ? AND message_id = ?`, b.clock().UTC(),
			key.ConsumerID, key.Source, key.MessageID.String())
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE gomessenger_inbox_attempt_generations
	        SET terminal = 1, updated_at = ?
	        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`, b.clock().UTC(),
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
	inserted, err := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox
        (consumer_id, source, message_id, fingerprint, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING`,
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
	if err := tx.QueryRowContext(ctx, `SELECT fingerprint, completed_at
        FROM gomessenger_inbox
        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completedAt); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: read attempt identity: %w", err)
	}
	if !fingerprintsEqual(stored, fingerprint) {
		return inbox.Result{}, false, inbox.ErrFingerprintConflict
	}
	if !completedAt.Valid {
		return inbox.Result{}, false, nil
	}
	state, err := readAttempt(ctx, tx, key, inbox.AttemptFingerprint(key, fingerprint))
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
	state, err := readAttempt(ctx, tx, key, attemptFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		if insertErr := b.insertAttempt(ctx, tx, key, attemptFingerprint); insertErr != nil {
			return attemptDecision{}, fmt.Errorf("inbox/sqlite: insert handler attempt: %w", insertErr)
		}
		return attemptDecision{attempt: 1}, nil
	}
	if err != nil {
		return attemptDecision{}, err
	}
	if state.terminal {
		return attemptDecision{attempt: state.attempt, terminal: true}, nil
	}
	if state.attempt >= maxAttempts {
		return attemptDecision{attempt: state.attempt, exhausted: true}, nil
	}
	if err := b.incrementAttempt(ctx, tx, key, attemptFingerprint); err != nil {
		return attemptDecision{}, fmt.Errorf("inbox/sqlite: increment handler attempt: %w", err)
	}
	return attemptDecision{attempt: state.attempt + 1}, nil
}

func (b *backend) insertAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	if key.AttemptGeneration == "" {
		_, err := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox_attempts
	        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
	        VALUES (?, ?, ?, ?, 1, ?)`, key.ConsumerID, key.Source,
			key.MessageID.String(), fingerprint[:], b.clock().UTC())
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO gomessenger_inbox_attempt_generations
	    (consumer_id, source, message_id, fingerprint, attempts, updated_at)
	    VALUES (?, ?, ?, ?, 1, ?)`, key.ConsumerID, key.Source,
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
		_, err := tx.ExecContext(ctx, `UPDATE gomessenger_inbox_attempts
	        SET attempts = attempts + 1, updated_at = ?
	        WHERE consumer_id = ? AND source = ? AND message_id = ?`, b.clock().UTC(),
			key.ConsumerID, key.Source, key.MessageID.String())
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE gomessenger_inbox_attempt_generations
	    SET attempts = attempts + 1, updated_at = ?
	    WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`, b.clock().UTC(),
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:])
	return err
}

func readAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) (attemptState, error) {
	var attempt int64
	var terminal bool
	var err error
	if key.AttemptGeneration == "" {
		err = tx.QueryRowContext(ctx, `SELECT attempts, terminal
	        FROM gomessenger_inbox_attempts
	        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
			key.ConsumerID, key.Source, key.MessageID.String()).Scan(&attempt, &terminal)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT attempts, terminal
	        FROM gomessenger_inbox_attempt_generations
	        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`,
			key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:]).Scan(&attempt, &terminal)
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
	return attemptState{attempt: uint64(attempt), terminal: terminal}, nil
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
	err = tx.QueryRowContext(ctx, `SELECT fingerprint, completed_at
        FROM gomessenger_inbox
        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
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
	attemptFingerprint := inbox.AttemptFingerprint(key, fingerprint)
	if err := deleteAttempt(ctx, tx, key, attemptFingerprint); err != nil {
		return fmt.Errorf("inbox/sqlite: delete handler attempt: %w", err)
	}
	hasAttempts, err := hasAttempts(ctx, tx, key)
	if err != nil {
		return err
	}
	if hasAttempts {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("inbox/sqlite: commit forgotten attempt generation: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND completed_at IS NULL`,
		key.ConsumerID, key.Source, key.MessageID.String()); err != nil {
		return fmt.Errorf("inbox/sqlite: delete incomplete identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("inbox/sqlite: commit forgotten attempt: %w", err)
	}
	return nil
}

func deleteAttempt(
	ctx context.Context,
	tx *sql.Tx,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	if key.AttemptGeneration == "" {
		_, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox_attempts
	        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
			key.ConsumerID, key.Source, key.MessageID.String())
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox_attempt_generations
	    WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`,
		key.ConsumerID, key.Source, key.MessageID.String(), fingerprint[:])
	return err
}

func hasAttempts(ctx context.Context, tx *sql.Tx, key inbox.Key) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT
	    EXISTS(SELECT 1 FROM gomessenger_inbox_attempts
	        WHERE consumer_id = ? AND source = ? AND message_id = ?)
	    OR EXISTS(SELECT 1 FROM gomessenger_inbox_attempt_generations
	        WHERE consumer_id = ? AND source = ? AND message_id = ?)`,
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox_attempt_generations
	    WHERE (consumer_id, source, message_id) IN (
	        SELECT consumer_id, source, message_id FROM gomessenger_inbox
	        WHERE completed_at < ?
	        ORDER BY completed_at, consumer_id, source, message_id
	        LIMIT ?
	    )`, before, limit); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: prune attempt generations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox_attempts
        WHERE (consumer_id, source, message_id) IN (
            SELECT consumer_id, source, message_id FROM gomessenger_inbox
            WHERE completed_at < ?
            ORDER BY completed_at, consumer_id, source, message_id
            LIMIT ?
        )`, before, limit); err != nil {
		return 0, fmt.Errorf("inbox/sqlite: prune attempts: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM gomessenger_inbox
        WHERE rowid IN (
            SELECT rowid FROM gomessenger_inbox
            WHERE completed_at < ?
            ORDER BY completed_at, consumer_id, source, message_id
            LIMIT ?
        )`, before, limit)
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
	if err := tx.QueryRowContext(ctx, `SELECT fingerprint, completed_at
        FROM gomessenger_inbox
        WHERE consumer_id = ? AND source = ? AND message_id = ?`,
		key.ConsumerID, key.Source, key.MessageID.String()).Scan(&stored, &completedAt); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: read identity: %w", err)
	}
	if !fingerprintsEqual(stored, fingerprint) {
		return inbox.Result{}, false, inbox.ErrFingerprintConflict
	}
	if !completedAt.Valid {
		return inbox.Result{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return inbox.Result{}, false, fmt.Errorf("inbox/sqlite: commit duplicate: %w", err)
	}
	return inbox.Result{Duplicate: true}, true, nil
}

func fingerprintsEqual(stored []byte, fingerprint inbox.Fingerprint) bool {
	return len(stored) == len(fingerprint) && subtle.ConstantTimeCompare(stored, fingerprint[:]) == 1
}
