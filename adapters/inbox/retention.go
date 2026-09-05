package inbox

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrTerminalRetentionUnsupported reports a backend without terminal retention support.
	ErrTerminalRetentionUnsupported = errors.New("inbox: terminal retention is unsupported")
	// ErrTerminalStateMissing reports a handoff without a persisted terminal generation.
	ErrTerminalStateMissing = errors.New("inbox: terminal generation is missing")
)

// TerminalRetentionBackend optionally records confirmed terminal handoffs and
// prunes old generations. It must never infer handoff confirmation from age.
// Implementations serialize confirmation and pruning with identity processing.
type TerminalRetentionBackend interface {
	ConfirmTerminalHandoff(ctx context.Context, key Key, fingerprint Fingerprint) error
	PruneTerminalAttempts(ctx context.Context, before time.Time, limit int) (int64, error)
}

// SupportsTerminalRetention reports whether the backend supports safe retention.
func (s *Store) SupportsTerminalRetention() bool {
	_, ok := s.backend.(TerminalRetentionBackend)
	return ok
}

// ConfirmTerminalHandoff records a broker-confirmed DLQ and source ACK/offset
// handoff. Call only after both boundaries succeeded. A failure retains state.
func (s *Store) ConfirmTerminalHandoff(ctx context.Context, key Key, fingerprint Fingerprint) error {
	if ctx == nil {
		return errors.New("inbox: nil confirmation context")
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	backend, ok := s.backend.(TerminalRetentionBackend)
	if !ok {
		return ErrTerminalRetentionUnsupported
	}
	return backend.ConfirmTerminalHandoff(ctx, key, fingerprint)
}

// PruneTerminalAttempts removes at most limit terminal generations whose last
// confirmed handoff precedes before. The host must choose a cutoff beyond its
// broker retention and in-flight delivery horizon. Removed generations lose
// their protection. No background pruning or default TTL is installed.
func (s *Store) PruneTerminalAttempts(ctx context.Context, before time.Time, limit int) (int64, error) {
	if ctx == nil || before.IsZero() || limit <= 0 || limit > 10_000 {
		return 0, errors.New("inbox: invalid terminal prune bounds")
	}
	backend, ok := s.backend.(TerminalRetentionBackend)
	if !ok {
		return 0, ErrTerminalRetentionUnsupported
	}
	return backend.PruneTerminalAttempts(ctx, before.UTC(), limit)
}
