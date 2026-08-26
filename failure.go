package messenger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
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

// HandlerPanicError reports a recovered handler or middleware panic without
// exposing the recovered value or stack through ordinary error channels.
type HandlerPanicError struct {
	HandlerID string
}

func (e *HandlerPanicError) Error() string {
	if e == nil || e.HandlerID == "" {
		return "messenger: handler panicked"
	}
	return fmt.Sprintf("messenger: handler %s panicked", e.HandlerID)
}

// PanicReport contains sensitive diagnostics for an explicitly configured
// PanicReporter. Value and Stack must not be written to untrusted logs or DLQ
// records without host-side redaction.
type PanicReport struct {
	HandlerID string
	Value     any
	Stack     []byte
}

// PanicReporter receives sensitive recovered-panic diagnostics. The default is
// to drop these details and return only HandlerPanicError to application code.
type PanicReporter interface {
	ReportPanic(ctx context.Context, report PanicReport)
}

// PanicReporterFunc adapts a function to PanicReporter.
type PanicReporterFunc func(context.Context, PanicReport)

// ReportPanic implements PanicReporter.
func (f PanicReporterFunc) ReportPanic(ctx context.Context, report PanicReport) {
	if f != nil {
		f(ctx, report)
	}
}

// ReportHandlerPanic sends sensitive details to the optional reporter and
// returns a safe error suitable for retries, observations, logs, and DLQ data.
func ReportHandlerPanic(
	ctx context.Context,
	reporter PanicReporter,
	handlerID string,
	recovered any,
	stack []byte,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !nilInterface(reporter) {
		report := PanicReport{
			HandlerID: handlerID,
			Value:     recovered,
			Stack:     append([]byte(nil), stack...),
		}
		func() {
			defer func() { _ = recover() }()
			reporter.ReportPanic(ctx, report)
		}()
	}
	return &HandlerPanicError{HandlerID: handlerID}
}

// HandlerCompletionError prevents a handler that returns nil after its context
// deadline from committing its transaction. Handler deadlines remain
// cooperative: a handler must still observe ctx.Done to stop promptly.
func HandlerCompletionError(ctx context.Context, handlerErr error) error {
	if handlerErr != nil {
		return handlerErr
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil handler context", ErrInvalidMessage)
	}
	return ctx.Err()
}

// FailureSanitizer converts an error to text safe for operational channels such
// as default logs, traces, and dead-letter records.
type FailureSanitizer interface {
	SanitizeFailure(err error) string
}

// FailureSanitizerFunc adapts a function to FailureSanitizer.
type FailureSanitizerFunc func(error) string

// SanitizeFailure implements FailureSanitizer.
func (f FailureSanitizerFunc) SanitizeFailure(err error) string {
	if f == nil {
		return ""
	}
	return f(err)
}

type defaultFailureSanitizer struct{}

func (defaultFailureSanitizer) SanitizeFailure(err error) string {
	if err == nil {
		return ""
	}
	var panicErr *HandlerPanicError
	switch {
	case errors.As(err, &panicErr):
		return panicErr.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, ErrEnvelopeTooLarge):
		return ErrEnvelopeTooLarge.Error()
	case errors.Is(err, ErrDescriptorConflict):
		return ErrDescriptorConflict.Error()
	case errors.Is(err, ErrUnsupportedCapability):
		return ErrUnsupportedCapability.Error()
	case errors.Is(err, ErrMessageExpired):
		return ErrMessageExpired.Error()
	case errors.Is(err, ErrMessageNotReady):
		return ErrMessageNotReady.Error()
	case errors.Is(err, ErrInvalidMessage):
		return ErrInvalidMessage.Error()
	default:
		return "messenger: operation failed"
	}
}

// DefaultFailureSanitizer returns the conservative built-in sanitizer. Hosts
// may opt in to richer text with an explicit FailureSanitizer implementation.
func DefaultFailureSanitizer() FailureSanitizer { return defaultFailureSanitizer{} }

// SanitizeFailure returns safe failure text. A nil or typed-nil sanitizer uses
// DefaultFailureSanitizer.
func SanitizeFailure(sanitizer FailureSanitizer, err error) string {
	if err == nil {
		return ""
	}
	if nilInterface(sanitizer) {
		sanitizer = DefaultFailureSanitizer()
	}
	text := strings.TrimSpace(sanitizer.SanitizeFailure(err))
	if text == "" {
		return "messenger: operation failed"
	}
	return strings.ToValidUTF8(text, "�")
}

type sanitizedFailureError struct {
	cause   error
	message string
}

func (e *sanitizedFailureError) Error() string { return e.message }
func (e *sanitizedFailureError) Unwrap() error { return e.cause }

// SanitizeError preserves errors.Is/errors.As through Unwrap while exposing
// only sanitized text through Error.
func SanitizeError(sanitizer FailureSanitizer, err error) error {
	if err == nil {
		return nil
	}
	return &sanitizedFailureError{cause: err, message: SanitizeFailure(sanitizer, err)}
}

// BoundedFailureText returns sanitized, valid UTF-8 text no longer than limit
// bytes. It never splits a UTF-8 code point.
func BoundedFailureText(sanitizer FailureSanitizer, err error, limit int) string {
	if limit <= 0 {
		return ""
	}
	text := SanitizeFailure(sanitizer, err)
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	if end == 0 {
		return ""
	}
	return text[:end]
}
