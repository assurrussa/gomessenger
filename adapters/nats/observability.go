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
	logAttrConsumerID = "consumer_id"
	logAttrMessageID  = "message_id"
	logAttrAttempt    = "attempt"
	logAttrError      = "error"
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
	return messenger.HandlerCompletionError(ctx, next(ctx))
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

func invokeMiddlewares(
	ctx context.Context,
	metadata messenger.Metadata,
	handlerID string,
	handler messenger.HandlerFunc,
	middlewares []messenger.Middleware,
	panicReporters ...messenger.PanicReporter,
) (err error) {
	var panicReporter messenger.PanicReporter
	if len(panicReporters) > 0 && !nilValue(panicReporters[0]) {
		panicReporter = panicReporters[0]
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = messenger.ReportHandlerPanic(ctx, panicReporter, handlerID, recovered, debug.Stack())
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
