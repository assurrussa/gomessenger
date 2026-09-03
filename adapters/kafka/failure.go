package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	messenger "github.com/assurrussa/gomessenger"
)

// PanicReporter receives sensitive recovered-panic diagnostics. Its method
// uses only standard-library types so one implementation can also satisfy the
// root messenger.PanicReporter contract in a clean, independently versioned module.
type PanicReporter interface {
	ReportPanic(ctx context.Context, handlerID string, recovered any, stack []byte)
}

// FailureSanitizer converts errors to text safe for operational observations
// and logs. In batch consumers, durable DLQ wire text strictly uses the
// conservative built-in sanitizer to protect rebalance boundaries.
type FailureSanitizer interface {
	SanitizeFailure(err error) string
}

type handlerPanicError struct{ handlerID string }

func (err *handlerPanicError) Error() string { return handlerPanicText(err.HandlerPanicID()) }

func (err *handlerPanicError) HandlerPanicID() string {
	if err == nil {
		return ""
	}
	return err.handlerID
}

func handlerPanicText(handlerID string) string {
	if handlerID == "" {
		return "messenger/kafka: handler panicked"
	}
	return fmt.Sprintf("messenger/kafka: handler %s panicked", handlerID)
}

func reportHandlerPanic(
	ctx context.Context,
	reporter PanicReporter,
	handlerID string,
	recovered any,
	stack []byte,
) error {
	if ctx != nil && !nilValue(reporter) {
		func() {
			defer func() { _ = recover() }()
			reporter.ReportPanic(ctx, handlerID, recovered, append([]byte(nil), stack...))
		}()
	}
	return &handlerPanicError{handlerID: handlerID}
}

type defaultFailureSanitizer struct{}

func (defaultFailureSanitizer) SanitizeFailure(err error) string {
	if err == nil {
		return ""
	}
	var panicErr interface {
		error
		HandlerPanicID() string
	}
	switch {
	case errors.As(err, &panicErr):
		return handlerPanicText(panicErr.HandlerPanicID())
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, messenger.ErrEnvelopeTooLarge):
		return messenger.ErrEnvelopeTooLarge.Error()
	case errors.Is(err, messenger.ErrDescriptorConflict):
		return messenger.ErrDescriptorConflict.Error()
	case errors.Is(err, ErrMessageExpired):
		return ErrMessageExpired.Error()
	case errors.Is(err, ErrMessageNotReady):
		return ErrMessageNotReady.Error()
	case errors.Is(err, messenger.ErrInvalidMessage):
		return messenger.ErrInvalidMessage.Error()
	default:
		return operationFailureText
	}
}

func handlerCompletionError(ctx context.Context, handlerErr error) error {
	if handlerErr != nil {
		return handlerErr
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil handler context", messenger.ErrInvalidMessage)
	}
	return ctx.Err()
}

func sanitizeFailure(sanitizer FailureSanitizer, err error) string {
	if err == nil {
		return ""
	}
	if nilValue(sanitizer) {
		sanitizer = defaultFailureSanitizer{}
	}
	text := strings.TrimSpace(callFailureSanitizer(sanitizer, err))
	if text == "" {
		return operationFailureText
	}
	return strings.ToValidUTF8(text, "�")
}

func callFailureSanitizer(sanitizer FailureSanitizer, err error) (text string) {
	defer func() {
		if recover() != nil {
			text = ""
		}
	}()
	return sanitizer.SanitizeFailure(err)
}

type sanitizedFailureError struct {
	cause   error
	message string
}

func (err *sanitizedFailureError) Error() string { return err.message }
func (err *sanitizedFailureError) Unwrap() error { return err.cause }

func sanitizeError(sanitizer FailureSanitizer, err error) error {
	if err == nil {
		return nil
	}
	return &sanitizedFailureError{cause: err, message: sanitizeFailure(sanitizer, err)}
}
