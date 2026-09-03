package messenger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

const (
	// DefaultBatchMaxMessages is the zero-value BatchConfig message limit.
	DefaultBatchMaxMessages = 100
	// DefaultBatchMaxBytes is the zero-value BatchConfig canonical byte limit.
	DefaultBatchMaxBytes = 4 << 20
	// DefaultBatchMaxWait is the zero-value BatchConfig fill deadline.
	DefaultBatchMaxWait = 25 * time.Millisecond
)

// BatchItemKey is the consumer-independent logical identity returned by a
// BatchHandler. Consumer identity remains an Inbox and transport concern.
type BatchItemKey struct {
	Source    string
	MessageID MessageID
}

// BatchItemResult classifies one logical message from a BatchHandler input.
// A nil Err marks success.
type BatchItemResult struct {
	Key BatchItemKey
	Err error
}

// BatchResult contains exactly one result for every logical message passed to
// a BatchHandler. Item order is irrelevant because results are keyed.
type BatchResult struct {
	Items []BatchItemResult
}

// BatchResultBuilder simplifies building a complete and valid BatchResult for
// a batch of messages. It initializes with every message in the batch marked
// as succeeded (nil error) and ensures that all input items are preserved in
// original order.
type BatchResultBuilder[T any] struct {
	keys          []BatchItemKey
	results       map[BatchItemKey]error
	validationErr error
}

// NewBatchResultBuilder initializes a builder for the supplied batch of messages.
// All items default to success (nil error).
func NewBatchResultBuilder[T any](messages []Message[T]) *BatchResultBuilder[T] {
	keys := make([]BatchItemKey, len(messages))
	results := make(map[BatchItemKey]error, len(messages))
	for index, message := range messages {
		key := BatchItemKey{
			Source:    message.Metadata.Source,
			MessageID: message.Metadata.ID,
		}
		keys[index] = key
		results[key] = nil
	}
	return &BatchResultBuilder[T]{
		keys:    keys,
		results: results,
	}
}

// OK marks the message as successfully processed.
func (b *BatchResultBuilder[T]) OK(message Message[T]) *BatchResultBuilder[T] {
	return b.OKKey(BatchItemKey{
		Source:    message.Metadata.Source,
		MessageID: message.Metadata.ID,
	})
}

// Fail marks the message as failed with err.
func (b *BatchResultBuilder[T]) Fail(message Message[T], err error) *BatchResultBuilder[T] {
	return b.FailKey(BatchItemKey{
		Source:    message.Metadata.Source,
		MessageID: message.Metadata.ID,
	}, err)
}

// OKKey marks the message identified by key as successfully processed.
func (b *BatchResultBuilder[T]) OKKey(key BatchItemKey) *BatchResultBuilder[T] {
	return b.FailKey(key, nil)
}

// FailKey marks the message identified by key as failed with err.
func (b *BatchResultBuilder[T]) FailKey(key BatchItemKey, err error) *BatchResultBuilder[T] {
	if b == nil || b.results == nil {
		return b
	}
	if _, exists := b.results[key]; !exists {
		if b.validationErr == nil {
			b.validationErr = fmt.Errorf("%w: unknown key %s/%s",
				ErrInvalidBatchResult, key.Source, key.MessageID)
		}
		return b
	}
	b.results[key] = err
	return b
}

// HasErrors reports whether any message in the batch has a non-nil error.
func (b *BatchResultBuilder[T]) HasErrors() bool {
	if b == nil {
		return false
	}
	if b.validationErr != nil {
		return true
	}
	for _, err := range b.results {
		if err != nil {
			return true
		}
	}
	return false
}

// ErrorKey returns the classified error for key and reports whether the key was
// present in the batch.
//
//nolint:revive // Comma-ok pattern returns the stored value (which is of type error) followed by ok.
func (b *BatchResultBuilder[T]) ErrorKey(key BatchItemKey) (error, bool) {
	if b == nil || b.results == nil {
		return nil, false
	}
	err, ok := b.results[key]
	return err, ok
}

// Error returns the classified error for the message and reports whether the
// message was present in the batch.
//
//nolint:revive // Comma-ok pattern returns the stored value (which is of type error) followed by ok.
func (b *BatchResultBuilder[T]) Error(message Message[T]) (error, bool) {
	return b.ErrorKey(BatchItemKey{
		Source:    message.Metadata.Source,
		MessageID: message.Metadata.ID,
	})
}

// Build constructs the populated BatchResult containing one item result for every
// message in the original batch in input order, or returns an error if an unknown
// key was passed to the builder.
func (b *BatchResultBuilder[T]) Build() (BatchResult, error) {
	if b == nil {
		return BatchResult{}, nil
	}
	if b.validationErr != nil {
		return BatchResult{}, b.validationErr
	}
	if len(b.keys) == 0 {
		return BatchResult{}, nil
	}
	items := make([]BatchItemResult, len(b.keys))
	for index, key := range b.keys {
		items[index] = BatchItemResult{
			Key: key,
			Err: b.results[key],
		}
	}
	return BatchResult{Items: items}, nil
}

// BatchHandler processes one broker-ordered batch of unique active messages.
// Implementations must classify the complete batch before performing business
// SQL and may write only for the successful subset.
type BatchHandler[T any] func(context.Context, []Message[T]) (BatchResult, error)

// BatchHandlerFunc is the transport-neutral terminal shape wrapped by batch
// middleware.
type BatchHandlerFunc func(context.Context) (BatchResult, error)

// BatchMiddleware wraps one batch invocation. Metadata is supplied as a
// defensive copy in broker order. The first registered middleware is the
// outermost wrapper.
type BatchMiddleware func(
	ctx context.Context,
	metadata []Metadata,
	handlerID string,
	next BatchHandlerFunc,
) (BatchResult, error)

// BatchHandlerMiddleware wraps a typed batch handler.
type BatchHandlerMiddleware[T any] func(BatchHandler[T]) BatchHandler[T]

// BatchConfig bounds one consumer batch. Its zero value resolves to 100
// messages, 4 MiB of canonical envelope bytes, and 25 milliseconds.
type BatchConfig struct {
	MaxMessages int
	MaxBytes    int
	MaxWait     time.Duration
	Middlewares []BatchMiddleware
}

// Normalize applies zero-value defaults and validates process-wide bounds for
// the supplied positive batch concurrency.
func (c BatchConfig) Normalize(concurrency int) (BatchConfig, error) {
	if concurrency <= 0 {
		return BatchConfig{}, fmt.Errorf("%w: batch concurrency must be positive", ErrInvalidMessage)
	}
	if c.MaxMessages < 0 || c.MaxBytes < 0 || c.MaxWait < 0 {
		return BatchConfig{}, fmt.Errorf("%w: negative batch limit", ErrInvalidMessage)
	}
	if c.MaxMessages == 0 {
		c.MaxMessages = DefaultBatchMaxMessages
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = DefaultBatchMaxBytes
	}
	if c.MaxWait == 0 {
		c.MaxWait = DefaultBatchMaxWait
	}
	for index, middleware := range c.Middlewares {
		if middleware == nil {
			return BatchConfig{}, fmt.Errorf("%w: nil batch middleware at index %d", ErrInvalidMessage, index)
		}
	}
	maxInt := int(^uint(0) >> 1)
	effectiveBytes := max(c.MaxBytes, DefaultMaxEnvelopeBytes)
	if c.MaxMessages > maxInt/concurrency {
		return BatchConfig{}, fmt.Errorf("%w: batch delivery bound overflows int", ErrInvalidMessage)
	}
	if effectiveBytes > maxInt/concurrency {
		return BatchConfig{}, fmt.Errorf("%w: batch byte bound overflows int", ErrInvalidMessage)
	}
	c.Middlewares = slices.Clone(c.Middlewares)
	return c, nil
}

// ChainBatchHandler applies typed batch middleware with the first item
// outermost. It returns nil when the chain is invalid.
func ChainBatchHandler[T any](
	handler BatchHandler[T],
	middlewares ...BatchHandlerMiddleware[T],
) BatchHandler[T] {
	if handler == nil {
		return nil
	}
	for index := len(middlewares) - 1; index >= 0; index-- {
		if middlewares[index] == nil {
			return nil
		}
		handler = middlewares[index](handler)
		if handler == nil {
			return nil
		}
	}
	return handler
}

type deferAfterError struct {
	cause error
	delay time.Duration
}

func (e *deferAfterError) Error() string { return e.cause.Error() }
func (e *deferAfterError) Unwrap() error { return e.cause }

// DeferAfter asks a durable consumer to retry after an exact positive delay
// without consuming a handler attempt.
func DeferAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	if delay <= 0 {
		return fmt.Errorf("%w: defer delay must be positive: %w", ErrInvalidMessage, err)
	}
	return &deferAfterError{cause: err, delay: delay}
}

// DeferDelay returns the exact no-attempt delay carried by err.
func DeferDelay(err error) (time.Duration, bool) {
	var target *deferAfterError
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.delay, true
}
