package messenger

import "context"

// Handler processes one typed message.
type Handler[T any] func(context.Context, Message[T]) error

// PayloadHandler processes only the payload and ignores message metadata.
type PayloadHandler[T any] func(context.Context, T) error

// QueryHandler processes one typed query and returns its typed result.
type QueryHandler[Q, R any] func(context.Context, Message[Q]) (R, error)

// QueryPayloadHandler processes only a query payload and returns its typed result.
type QueryPayloadHandler[Q, R any] func(context.Context, Q) (R, error)

// HandlerFunc is the transport-neutral terminal handler shape used by global
// middleware.
type HandlerFunc func(context.Context) error

// Middleware wraps one local query/command/event or durable handler. The first registered
// middleware is the outermost wrapper and may short-circuit by not calling
// next.
type Middleware func(
	ctx context.Context,
	metadata Metadata,
	handlerID string,
	next HandlerFunc,
) error

// HandlerMiddleware wraps a typed handler.
type HandlerMiddleware[T any] func(Handler[T]) Handler[T]

// QueryHandlerMiddleware wraps a typed query handler and may return a cached
// or synthetic result without invoking the wrapped handler.
type QueryHandlerMiddleware[Q, R any] func(QueryHandler[Q, R]) QueryHandler[Q, R]

// ChainHandler applies typed middleware with the first item outermost. It
// returns nil when handler, a middleware, or a middleware result is nil so the
// receiving Builder or durable consumer can reject the invalid chain.
func ChainHandler[T any](handler Handler[T], middlewares ...HandlerMiddleware[T]) Handler[T] {
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

// ChainQueryHandler applies typed query middleware with the first item
// outermost. It returns nil when the chain is invalid.
func ChainQueryHandler[Q, R any](
	handler QueryHandler[Q, R],
	middlewares ...QueryHandlerMiddleware[Q, R],
) QueryHandler[Q, R] {
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

// HandlePayload adapts a payload-only handler to the primary Handler contract.
func HandlePayload[T any](handler PayloadHandler[T]) Handler[T] {
	if handler == nil {
		return nil
	}
	return func(ctx context.Context, message Message[T]) error {
		return handler(ctx, message.Payload)
	}
}

// HandleQueryPayload adapts a payload-only query handler to QueryHandler.
func HandleQueryPayload[Q, R any](handler QueryPayloadHandler[Q, R]) QueryHandler[Q, R] {
	if handler == nil {
		return nil
	}
	return func(ctx context.Context, message Message[Q]) (R, error) {
		return handler(ctx, message.Payload)
	}
}

// Sender is the ordinary generic DI interface for a bound command descriptor.
type Sender[T any] interface {
	Send(ctx context.Context, payload T) (Receipt, error)
	SendMessage(ctx context.Context, outgoing Outgoing[T]) (Receipt, error)
}

// Publisher is the ordinary generic DI interface for a bound event descriptor.
type Publisher[T any] interface {
	Publish(ctx context.Context, payload T) (Receipt, error)
	PublishMessage(ctx context.Context, outgoing Outgoing[T]) (Receipt, error)
}

// Querier is the ordinary generic DI interface for a bound query descriptor.
type Querier[Q, R any] interface {
	Query(ctx context.Context, payload Q) (R, error)
}
