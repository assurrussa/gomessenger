package messenger

import (
	"errors"
	"fmt"
	"time"
)

type permanentError struct{ cause error }

func (e *permanentError) Error() string { return e.cause.Error() }
func (e *permanentError) Unwrap() error { return e.cause }

// Permanent marks an error as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{cause: err}
}

type retryAfterError struct {
	cause error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return e.cause.Error() }
func (e *retryAfterError) Unwrap() error { return e.cause }

// RetryAfter asks a durable transport to retry after an exact positive delay.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	if delay <= 0 {
		return fmt.Errorf("%w: retry delay must be positive: %w", ErrInvalidMessage, err)
	}
	return &retryAfterError{cause: err, delay: delay}
}

// IsPermanent reports whether err contains a Permanent marker.
func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}

// RetryDelay returns an explicitly requested retry delay.
func RetryDelay(err error) (time.Duration, bool) {
	var target *retryAfterError
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.delay, true
}
