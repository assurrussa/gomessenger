package nats

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"runtime/debug"
	"sync/atomic"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	logAttrConsumerID     = "consumer_id"
	logAttrMessageID      = "message_id"
	logAttrAttempt        = "attempt"
	logAttrError          = "error"
	operationBrokerAck    = messenger.Operation("broker_ack")
	operationDLQHandoff   = messenger.Operation("dlq_handoff")
	operationRetryHandoff = messenger.Operation("retry_handoff")
	operationFailureText  = "messenger/nats: operation failed"
)

func applyObservabilityDefaults(config *HandlerConfig) error {
	if nilValue(config.Logger) {
		config.Logger = messenger.AdaptSlog(nil)
	}
	if nilValue(config.Propagator) {
		config.Propagator = messenger.NoopContextPropagator()
	}
	if config.PanicReporter != nil && nilValue(config.PanicReporter) {
		return fmt.Errorf("%w: nil panic reporter", ErrInvalidConfig)
	}
	if nilValue(config.FailureSanitizer) {
		config.FailureSanitizer = defaultFailureSanitizer{}
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
	return nil
}

func notifyObservers(ctx context.Context, config HandlerConfig, observation messenger.Observation) {
	for _, observer := range config.Observers {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					logInfrastructure(ctx, config.Logger, messenger.LogError, "messenger observer panicked",
						messenger.LogAttr{Key: "operation", Value: observation.Operation},
						messenger.LogAttr{Key: logAttrConsumerID, Value: config.ConsumerID},
						messenger.LogAttr{Key: "observer_panic", Value: true},
					)
				}
			}()
			observer.Observe(ctx, observation)
		}()
	}
}

func logInfrastructure(
	ctx context.Context,
	logger messenger.Logger,
	level messenger.LogLevel,
	message string,
	attrs ...messenger.LogAttr,
) {
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Log(ctx, level, message, attrs...)
}

func extractDeliveryContext(
	parent context.Context,
	config HandlerConfig,
	headers map[string]string,
) context.Context {
	if nilValue(config.Propagator) {
		return parent
	}
	extracted := config.Propagator.Extract(parent, headers)
	if extracted != nil {
		return extracted
	}
	logInfrastructure(parent, config.Logger, messenger.LogError, "context propagator returned nil",
		messenger.LogAttr{Key: logAttrConsumerID, Value: config.ConsumerID},
	)
	return parent
}

func invokeMiddlewares(
	ctx context.Context,
	metadata messenger.Metadata,
	handlerID string,
	handler messenger.HandlerFunc,
	middlewares []messenger.Middleware,
	panicReporter PanicReporter,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = reportHandlerPanic(ctx, panicReporter, handlerID, recovered, debug.Stack())
		}
	}()
	var handlerContextErr func() error
	var invoke func(int, context.Context) error
	invoke = func(index int, current context.Context) error {
		if current == nil {
			return fmt.Errorf("%w: middleware supplied a nil context", messenger.ErrInvalidMessage)
		}
		if index == len(middlewares) {
			handlerContextErr = current.Err
			return handlerCompletionError(current, handler(current))
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
	err = invoke(0, ctx)
	if err != nil || handlerContextErr == nil {
		return err
	}
	// A middleware may swallow the handler's error. Recheck the exact context
	// used by the handler before the Inbox callback is allowed to commit.
	return handlerContextErr()
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
