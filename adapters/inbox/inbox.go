package inbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

var (
	// ErrFingerprintConflict reports identity reuse with different content.
	ErrFingerprintConflict = errors.New("inbox: message identity has a different fingerprint")
	// ErrInvalidKey reports an incomplete idempotency key.
	ErrInvalidKey = errors.New("inbox: invalid key")
	// ErrAttemptsExhausted reports that the durable handler limit was reached.
	ErrAttemptsExhausted = errors.New("inbox: handler attempts exhausted")
	// ErrAttemptTerminal reports a previously committed permanent handler outcome.
	ErrAttemptTerminal = errors.New("inbox: handler attempt is terminal")
	// ErrAttemptTrackingUnsupported reports a backend without durable attempt support.
	ErrAttemptTrackingUnsupported = errors.New("inbox: durable attempt tracking is unsupported")
)

// Key is the durable consumer-scoped message identity.
type Key struct {
	ConsumerID string
	Source     string
	MessageID  messenger.MessageID
	// AttemptGeneration selects one bounded ProcessAttempt cycle without
	// changing the logical identity used by Process or completed deduplication.
	AttemptGeneration string
}

// Fingerprint is SHA-256 over the canonical native envelope.
type Fingerprint [sha256.Size]byte

// Result describes whether the handler ran or an earlier commit was reused.
type Result struct {
	Duplicate bool
	Attempt   uint64
}

// Handler performs business writes inside the inbox transaction context.
type Handler func(context.Context) error

// Backend owns the atomic backend-specific transaction algorithm.
type Backend interface {
	Process(ctx context.Context, key Key, fingerprint Fingerprint, handler Handler) (Result, error)
	Prune(ctx context.Context, before time.Time, limit int) (int64, error)
}

// AttemptBackend extends Backend with durable handler-attempt accounting.
// Implementations must retain Permanent handler outcomes and return a
// Permanent-wrapped ErrAttemptTerminal without invoking that handler again.
// A non-empty Key.AttemptGeneration starts a fresh bounded attempt cycle while
// preserving the logical inbox identity and completed-duplicate suppression.
// Distinct non-empty generations retain independent counters until forgotten.
type AttemptBackend interface {
	ProcessAttempt(
		ctx context.Context,
		key Key,
		fingerprint Fingerprint,
		maxAttempts uint64,
		handler Handler,
	) (Result, error)
	ForgetAttempt(ctx context.Context, key Key, fingerprint Fingerprint) error
}

// Store validates common inputs and delegates atomic work to one backend.
type Store struct{ backend Backend }

// New constructs an inbox store around a PostgreSQL or SQLite backend.
func New(backend Backend) (*Store, error) {
	if backend == nil {
		return nil, errors.New("inbox: nil backend")
	}
	return &Store{backend: backend}, nil
}

// Process executes handler once for one identity and fingerprint.
func (s *Store) Process(
	ctx context.Context,
	key Key,
	fingerprint Fingerprint,
	handler Handler,
) (Result, error) {
	if err := ValidateKey(key); err != nil {
		return Result{}, err
	}
	if handler == nil {
		return Result{}, errors.New("inbox: nil handler")
	}
	key.AttemptGeneration = ""
	return s.backend.Process(ctx, key, fingerprint, handler)
}

// SupportsAttempts reports whether ProcessAttempt has durable backend support.
func (s *Store) SupportsAttempts() bool {
	_, ok := s.backend.(AttemptBackend)
	return ok
}

// ProcessAttempt executes one bounded handler attempt and durably records a
// failed invocation while rolling back its business writes. A permanent
// handler error remains terminal across later calls in the same attempt
// generation until ForgetAttempt.
func (s *Store) ProcessAttempt(
	ctx context.Context,
	key Key,
	fingerprint Fingerprint,
	maxAttempts uint64,
	handler Handler,
) (Result, error) {
	if err := ValidateKey(key); err != nil {
		return Result{}, err
	}
	if maxAttempts == 0 {
		return Result{}, errors.New("inbox: invalid handler attempt limit")
	}
	if handler == nil {
		return Result{}, errors.New("inbox: nil handler")
	}
	backend, ok := s.backend.(AttemptBackend)
	if !ok {
		return Result{}, ErrAttemptTrackingUnsupported
	}
	return backend.ProcessAttempt(ctx, key, fingerprint, maxAttempts, handler)
}

// ForgetAttempt removes an incomplete attempt record after the source message
// has been durably handed off and terminated.
func (s *Store) ForgetAttempt(ctx context.Context, key Key, fingerprint Fingerprint) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	backend, ok := s.backend.(AttemptBackend)
	if !ok {
		return ErrAttemptTrackingUnsupported
	}
	return backend.ForgetAttempt(ctx, key, fingerprint)
}

// Prune removes at most limit completed identities older than before.
func (s *Store) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	if before.IsZero() || limit <= 0 || limit > 10_000 {
		return 0, errors.New("inbox: invalid prune bounds")
	}
	return s.backend.Prune(ctx, before.UTC(), limit)
}

// ValidateKey checks the stable inbox identity fields.
func ValidateKey(key Key) error {
	if strings.TrimSpace(key.ConsumerID) == "" || strings.TrimSpace(key.Source) == "" || key.MessageID.IsZero() ||
		strings.TrimSpace(key.AttemptGeneration) != key.AttemptGeneration || len(key.AttemptGeneration) > 128 {
		return fmt.Errorf("%w: consumer=%q source=%q", ErrInvalidKey, key.ConsumerID, key.Source)
	}
	return nil
}

// AttemptFingerprint derives the durable attempt-cycle fingerprint for key.
// The empty generation preserves the canonical envelope fingerprint used by
// records created before attempt generations were introduced.
func AttemptFingerprint(key Key, fingerprint Fingerprint) Fingerprint {
	if key.AttemptGeneration == "" {
		return fingerprint
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("gomessenger/inbox/attempt-generation/v1\x00"))
	_, _ = hash.Write(fingerprint[:])
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(key.AttemptGeneration))
	var derived Fingerprint
	copy(derived[:], hash.Sum(nil))
	return derived
}

// FingerprintEnvelope calculates the canonical envelope fingerprint.
func FingerprintEnvelope(data []byte) Fingerprint {
	return Fingerprint(messenger.EnvelopeFingerprint(data))
}

// SQLTx is the standard transaction surface exposed to host repositories.
type SQLTx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type sqlTxContextKey struct{}

// ContextWithSQLTx attaches a backend transaction for host repository adapters.
func ContextWithSQLTx(ctx context.Context, tx SQLTx) context.Context {
	return context.WithValue(ctx, sqlTxContextKey{}, tx)
}

// SQLTxFromContext returns the active inbox transaction.
func SQLTxFromContext(ctx context.Context) (SQLTx, bool) {
	tx, ok := ctx.Value(sqlTxContextKey{}).(SQLTx)
	return tx, ok
}
