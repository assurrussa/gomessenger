// Package batchruntime centralizes the transport-neutral batch handler
// contract shared by durable adapters.
package batchruntime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync/atomic"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

// FailureKind is the ordered durable classification of a handler error.
type FailureKind uint8

const (
	FailureSuccess FailureKind = iota
	FailurePermanent
	FailureDefer
	FailureRetryAfter
	FailureOrdinary
)

// Classify applies marker precedence: Permanent, DeferAfter, RetryAfter, then
// an ordinary consuming failure.
func Classify(err error) (FailureKind, time.Duration) {
	if err == nil {
		return FailureSuccess, 0
	}
	if messenger.IsPermanent(err) {
		return FailurePermanent, 0
	}
	if delay, ok := messenger.DeferDelay(err); ok {
		return FailureDefer, delay
	}
	if delay, ok := messenger.RetryDelay(err); ok {
		return FailureRetryAfter, delay
	}
	return FailureOrdinary, 0
}

// ValidateResult returns item errors in expected key order after proving exact
// identity coverage.
func ValidateResult(expected []messenger.BatchItemKey, result messenger.BatchResult) ([]error, error) {
	if len(result.Items) != len(expected) {
		return nil, fmt.Errorf("%w: got %d items, want %d", messenger.ErrInvalidBatchResult,
			len(result.Items), len(expected))
	}
	expectedSet := make(map[messenger.BatchItemKey]struct{}, len(expected))
	for _, key := range expected {
		if key.Source == "" || key.MessageID.IsZero() {
			return nil, fmt.Errorf("%w: incomplete expected key", messenger.ErrInvalidBatchResult)
		}
		if _, exists := expectedSet[key]; exists {
			return nil, fmt.Errorf("%w: duplicate expected key %s/%s",
				messenger.ErrInvalidBatchResult, key.Source, key.MessageID)
		}
		expectedSet[key] = struct{}{}
	}
	byKey := make(map[messenger.BatchItemKey]error, len(result.Items))
	for _, item := range result.Items {
		if _, exists := expectedSet[item.Key]; !exists {
			return nil, fmt.Errorf("%w: unknown key %s/%s",
				messenger.ErrInvalidBatchResult, item.Key.Source, item.Key.MessageID)
		}
		if _, exists := byKey[item.Key]; exists {
			return nil, fmt.Errorf("%w: duplicate key %s/%s",
				messenger.ErrInvalidBatchResult, item.Key.Source, item.Key.MessageID)
		}
		byKey[item.Key] = item.Err
	}
	ordered := make([]error, len(expected))
	for index, key := range expected {
		itemErr, exists := byKey[key]
		if !exists {
			return nil, fmt.Errorf("%w: missing key %s/%s",
				messenger.ErrInvalidBatchResult, key.Source, key.MessageID)
		}
		ordered[index] = itemErr
	}
	return ordered, nil
}

// Invoke runs batch middleware and the typed handler with panic recovery,
// next-once enforcement, and completion-context rechecking.
func Invoke[T any](
	ctx context.Context,
	messages []messenger.Message[T],
	handlerID string,
	handler messenger.BatchHandler[T],
	middlewares []messenger.BatchMiddleware,
	reporter messenger.PanicReporter,
) (result messenger.BatchResult, err error) {
	if ctx == nil || handler == nil || handlerID == "" {
		return messenger.BatchResult{}, fmt.Errorf("%w: incomplete batch invocation", messenger.ErrInvalidBatchResult)
	}
	metadata := make([]messenger.Metadata, len(messages))
	for index, message := range messages {
		metadata[index] = message.Metadata
		metadata[index].Headers = maps.Clone(message.Metadata.Headers)
	}

	var terminalCompletionErr error
	var terminalCtxErr func() error
	next := messenger.BatchHandlerFunc(func(nextCtx context.Context) (messenger.BatchResult, error) {
		if nextCtx == nil {
			return messenger.BatchResult{}, fmt.Errorf("%w: middleware supplied a nil context",
				messenger.ErrInvalidBatchResult)
		}
		terminalCtxErr = nextCtx.Err
		result, handlerErr := handler(nextCtx, messages)
		terminalCompletionErr = messenger.HandlerCompletionError(nextCtx, handlerErr)
		if terminalCompletionErr != nil && handlerErr == nil {
			result = messenger.BatchResult{}
		}
		return result, terminalCompletionErr
	})
	for index := len(middlewares) - 1; index >= 0; index-- {
		middleware := middlewares[index]
		if middleware == nil {
			return messenger.BatchResult{}, fmt.Errorf("%w: nil batch middleware at index %d",
				messenger.ErrInvalidBatchResult, index)
		}
		wrapped := next
		next = func(nextCtx context.Context) (messenger.BatchResult, error) {
			var called atomic.Bool
			guarded := messenger.BatchHandlerFunc(func(callCtx context.Context) (messenger.BatchResult, error) {
				if !called.CompareAndSwap(false, true) {
					return messenger.BatchResult{}, fmt.Errorf("%w: middleware called next more than once",
						messenger.ErrInvalidBatchResult)
				}
				return wrapped(callCtx)
			})
			return middleware(nextCtx, cloneMetadata(metadata), handlerID, guarded)
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = messenger.ReportHandlerPanic(ctx, reporter, handlerID, recovered, debug.Stack())
			result = messenger.BatchResult{}
		}
	}()
	result, err = next(ctx)
	if terminalCtxErr != nil {
		if lateCtxErr := terminalCtxErr(); lateCtxErr != nil {
			result = messenger.BatchResult{}
			if err == nil {
				err = lateCtxErr
			}
		}
	}
	if terminalCompletionErr != nil {
		result = messenger.BatchResult{}
		if err == nil {
			err = terminalCompletionErr
		}
	}
	if err != nil && len(result.Items) != 0 {
		return messenger.BatchResult{}, fmt.Errorf("%w: non-empty result with top-level error: %w",
			messenger.ErrInvalidBatchResult, err)
	}
	return result, err
}

func cloneMetadata(metadata []messenger.Metadata) []messenger.Metadata {
	cloned := make([]messenger.Metadata, len(metadata))
	for index := range metadata {
		cloned[index] = metadata[index]
		cloned[index].Headers = maps.Clone(metadata[index].Headers)
	}
	return cloned
}

// IsFailClosed reports top-level errors that represent a handler contract
// defect rather than a transient batch failure.
func IsFailClosed(err error) bool {
	return errors.Is(err, messenger.ErrInvalidBatchResult) || messenger.IsPermanent(err)
}
