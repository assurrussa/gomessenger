package kafka

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	logAttrAbortAttempted = "abort_attempted"
	logAttrAbortError     = "abort_error"
	logAttrAction         = "action"
	logAttrChangeCount    = "change_count"
	logAttrConflictCount  = "conflict_count"
	logAttrConsumerID     = "consumer_id"
	logAttrError          = "error"
	logAttrFieldsCount    = "fields_count"
	logAttrMessageID      = "message_id"
	logAttrNotBefore      = "not_before"
	logAttrOperation      = "operation"
	logAttrPartition      = "partition"
	logAttrRoute          = "route"
	logAttrTopic          = "topic"
	logAttrTopicCount     = "topic_count"
	logAttrTransport      = "transport"
)

func applyObservabilityDefaults(config *HandlerConfig) error {
	if nilValue(config.Logger) {
		config.Logger = messenger.AdaptSlog(nil)
	}
	if nilValue(config.Propagator) {
		config.Propagator = messenger.NoopContextPropagator()
	}
	for _, observer := range config.Observers {
		if nilValue(observer) {
			return fmt.Errorf("%w: nil observer", ErrInvalidConfig)
		}
	}
	for _, middleware := range config.Middlewares {
		if middleware == nil {
			return fmt.Errorf("%w: nil middleware", ErrInvalidConfig)
		}
	}
	// This outer middleware converts a late nil return into ctx.Err before the
	// Inbox callback can commit business writes and its completion marker.
	config.Middlewares = append([]messenger.Middleware{handlerCompletionMiddleware}, config.Middlewares...)
	return nil
}

func handlerCompletionMiddleware(
	ctx context.Context,
	_ messenger.Metadata,
	_ string,
	next messenger.HandlerFunc,
) error {
	err := next(ctx)
	if err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil handler context", messenger.ErrInvalidMessage)
	}
	return ctx.Err()
}

func notifyObservers(ctx context.Context, config HandlerConfig, observation messenger.Observation) {
	for _, observer := range config.Observers {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					logInfrastructure(ctx, config.Logger, messenger.LogError, "messenger observer panicked",
						messenger.LogAttr{Key: "operation", Value: observation.Operation},
						messenger.LogAttr{Key: logAttrConsumerID, Value: config.ConsumerID},
						messenger.LogAttr{Key: "observer_panic", Value: true})
				}
			}()
			observer.Observe(ctx, observation)
		}()
	}
}

func logInfrastructure(ctx context.Context, logger messenger.Logger, level messenger.LogLevel, message string,
	attrs ...messenger.LogAttr,
) {
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Log(ctx, level, message, attrs...)
}

func (t *Transport) logInfrastructure(
	ctx context.Context,
	level messenger.LogLevel,
	message string,
	attrs ...messenger.LogAttr,
) {
	if t == nil || ctx == nil {
		return
	}
	transportAttrs := make([]messenger.LogAttr, 0, len(attrs)+1)
	transportAttrs = append(transportAttrs, messenger.LogAttr{Key: logAttrTransport, Value: t.config.Name})
	transportAttrs = append(transportAttrs, attrs...)
	logInfrastructure(ctx, t.config.Logger, level, message, transportAttrs...)
}

func (t *Transport) logFailure(
	ctx context.Context,
	level messenger.LogLevel,
	message string,
	operation string,
	err error,
	attrs ...messenger.LogAttr,
) {
	attrs = append(attrs,
		messenger.LogAttr{Key: logAttrOperation, Value: operation},
		messenger.LogAttr{Key: logAttrError, Value: safeOperationalError(err)},
	)
	t.logInfrastructure(ctx, level, message, attrs...)
}

func (t *Transport) logTransactionFailure(
	ctx context.Context,
	operation string,
	err error,
	abortErr error,
	attrs ...messenger.LogAttr,
) {
	attrs = append(attrs, messenger.LogAttr{Key: logAttrAbortAttempted, Value: true})
	if abortErr != nil {
		attrs = append(attrs, messenger.LogAttr{Key: logAttrAbortError, Value: safeOperationalError(abortErr)})
	}
	t.logFailure(ctx, messenger.LogError, "Kafka transaction failed", operation, err, attrs...)
}

func safeOperationalError(err error) error {
	if err == nil {
		return nil
	}
	return operationalError{cause: err}
}

type operationalError struct{ cause error }

func (operationalError) Error() string { return "messenger/kafka: operation failed" }
func (e operationalError) Unwrap() error { return e.cause }

func invokeMiddlewares(
	ctx context.Context,
	metadata messenger.Metadata,
	handlerID string,
	handler messenger.HandlerFunc,
	middlewares []messenger.Middleware,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("messenger/kafka: handler %s panicked", handlerID)
		}
	}()
	if len(middlewares) == 0 {
		return handler(ctx)
	}
	var invoke func(int, context.Context) error
	invoke = func(index int, current context.Context) error {
		if current == nil {
			return fmt.Errorf("%w: middleware supplied a nil context", messenger.ErrInvalidMessage)
		}
		if index == len(middlewares) {
			return handler(current)
		}
		var called atomic.Bool
		next := func(nextContext context.Context) error {
			if !called.CompareAndSwap(false, true) {
				return fmt.Errorf("%w: middleware called next more than once", messenger.ErrInvalidMessage)
			}
			return invoke(index+1, nextContext)
		}
		cloned := metadata
		cloned.Headers = maps.Clone(metadata.Headers)
		return middlewares[index](current, cloned, handlerID, next)
	}
	return invoke(0, ctx)
}
