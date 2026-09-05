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
	// ErrAttemptConflict reports an incomplete message identity tracked by ProcessAttempt.
	ErrAttemptConflict = errors.New("inbox: message identity is tracked by attempt processing")
	// ErrAttemptTrackingUnsupported reports a backend without durable attempt support.
	ErrAttemptTrackingUnsupported = errors.New("inbox: durable attempt tracking is unsupported")
	// ErrBatchAttemptTrackingUnsupported reports a backend without the atomic
	// partial-outcome batch capability.
	ErrBatchAttemptTrackingUnsupported = errors.New("inbox: durable batch attempt tracking is unsupported")
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

// BatchItem is one broker delivery admitted to an atomic batch attempt. Items
// with the same logical identity and fingerprint may be coalesced before the
// handler is invoked.
type BatchItem struct {
	Key         Key
	Fingerprint Fingerprint
}

// BatchHandler receives only unique active logical items in broker order. Its
// context contains the SQL transaction used for Inbox and business writes.
type BatchHandler func(context.Context, []BatchItem) (messenger.BatchResult, error)

// BatchOutcome identifies the broker action selected by a committed Inbox
// transaction.
type BatchOutcome string

const (
	// BatchACK acknowledges a successful or already-completed delivery.
	BatchACK BatchOutcome = "ack"
	// BatchRetry schedules a retry that consumed an attempt.
	BatchRetry BatchOutcome = "retry"
	// BatchDefer schedules a retry without consuming an attempt.
	BatchDefer BatchOutcome = "defer"
	// BatchDLQ performs a terminal dead-letter handoff.
	BatchDLQ BatchOutcome = "dlq"
)

const (
	// FailureIdentityConflict is a terminal identity/fingerprint mismatch.
	FailureIdentityConflict = "identity_conflict"
	// FailureAttemptsExhausted is a terminal bounded-attempt outcome.
	FailureAttemptsExhausted = "attempts_exhausted"
	// FailurePermanent is an explicitly permanent handler outcome.
	FailurePermanent = "permanent"
)

// BatchItemOutcome is the committed decision for one input BatchItem.
type BatchItemOutcome struct {
	Key         Key
	Fingerprint Fingerprint
	Outcome     BatchOutcome
	Attempt     uint64
	Duplicate   bool
	Delay       time.Duration
	FailureKind string
	Err         error
}

// BatchProcessResult preserves input order and records the number of unique
// active messages passed to the handler.
type BatchProcessResult struct {
	Items           []BatchItemOutcome
	HandlerMessages int
}

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

// BatchAttemptBackend extends AttemptBackend with one shared transaction for partial
// batch outcomes. A top-level handler error rolls the transaction back and is
// returned directly without consuming any item attempts.
type BatchAttemptBackend interface {
	AttemptBackend

	ProcessBatchAttempt(
		ctx context.Context,
		items []BatchItem,
		maxAttempts uint64,
		handler BatchHandler,
	) (BatchProcessResult, error)
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

// SupportsBatchAttempts reports whether ProcessBatchAttempt has atomic backend
// support.
func (s *Store) SupportsBatchAttempts() bool {
	_, ok := s.backend.(BatchAttemptBackend)
	return ok
}

// ProcessBatchAttempt runs one shared Inbox/business transaction and returns
// per-delivery outcomes only after commit.
func (s *Store) ProcessBatchAttempt(
	ctx context.Context,
	items []BatchItem,
	maxAttempts uint64,
	handler BatchHandler,
) (BatchProcessResult, error) {
	if ctx == nil {
		return BatchProcessResult{}, errors.New("inbox: nil batch context")
	}
	if len(items) == 0 {
		return BatchProcessResult{}, errors.New("inbox: empty batch")
	}
	if maxAttempts == 0 {
		return BatchProcessResult{}, errors.New("inbox: invalid batch handler attempt limit")
	}
	if handler == nil {
		return BatchProcessResult{}, errors.New("inbox: nil batch handler")
	}
	consumerID := items[0].Key.ConsumerID
	generations := make(map[messenger.BatchItemKey]string, len(items))
	for index, item := range items {
		if err := ValidateKey(item.Key); err != nil {
			return BatchProcessResult{}, fmt.Errorf("inbox: batch item %d: %w", index, err)
		}
		if item.Key.ConsumerID != consumerID {
			return BatchProcessResult{}, fmt.Errorf("%w: mixed consumer IDs", ErrInvalidKey)
		}
		key := messenger.BatchItemKey{
			Source:    item.Key.Source,
			MessageID: item.Key.MessageID,
		}
		if gen, exists := generations[key]; exists && gen != item.Key.AttemptGeneration {
			return BatchProcessResult{}, fmt.Errorf(
				"%w: mixed attempt generations for %s/%s",
				messenger.ErrInvalidBatchResult,
				key.Source,
				key.MessageID,
			)
		}
		generations[key] = item.Key.AttemptGeneration
	}
	backend, ok := s.backend.(BatchAttemptBackend)
	if !ok {
		return BatchProcessResult{}, ErrBatchAttemptTrackingUnsupported
	}
	return backend.ProcessBatchAttempt(ctx, items, maxAttempts, handler)
}

// ProcessAttempt executes one bounded handler attempt and durably records a
// failed invocation while rolling back its business writes. Permanent and
// exhausted generations remain closed across restarts and limit changes until
// explicit retention or destructive reset removes their protection.
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

// ForgetAttempt explicitly resets an incomplete generation, including its terminal protection.
// It must not be called automatically after broker acknowledgement.
//
// Deprecated: replay with a new AttemptGeneration; use PruneTerminalAttempts for retention.
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
