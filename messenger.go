package messenger

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type commandBinding struct {
	descriptor    descriptorRegistration
	handlerID     string
	handler       func(any) (funcContextHandler, error)
	route         Route
	observer      Observer
	clock         func() time.Time
	middleware    []Middleware
	panicReporter PanicReporter
}

type queryBinding struct {
	descriptor    descriptorRegistration
	resultType    reflect.Type
	handlerID     string
	handler       func(any) (funcContextQueryHandler, error)
	route         LocalQueryRoute
	observer      Observer
	clock         func() time.Time
	middleware    []Middleware
	panicReporter PanicReporter
}

type boundEventSubscriber struct {
	id      string
	handler func(any) (funcContextHandler, error)
}

type eventBinding struct {
	descriptor    descriptorRegistration
	subscribers   []boundEventSubscriber
	route         Route
	observer      Observer
	clock         func() time.Time
	middleware    []Middleware
	panicReporter PanicReporter
}

// Messenger sends commands, executes local queries, and publishes events
// through immutable descriptor bindings.
type Messenger struct {
	source      string
	idGenerator IDGenerator
	clock       func() time.Time
	logger      Logger
	observer    Observer
	propagator  ContextPropagator
	commands    map[descriptorKey]commandBinding
	events      map[descriptorKey]eventBinding
	queries     map[descriptorKey]queryBinding
	manifest    Manifest
}

// Send sends a command with generated metadata.
func (m *Messenger) Send[T any](ctx context.Context, descriptor Command[T], payload T) (Receipt, error) {
	return m.SendMessage(ctx, descriptor, Outgoing[T]{Payload: payload})
}

// SendMessage sends a command with explicit optional metadata.
func (m *Messenger) SendMessage[T any](
	ctx context.Context,
	descriptor Command[T],
	outgoing Outgoing[T],
) (Receipt, error) {
	if ctx == nil {
		return Receipt{}, fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	binding, ok := m.commands[keyFor(descriptor.info)]
	if !ok || !sameDescriptor(binding.descriptor, descriptor.info, reflect.TypeFor[T]()) {
		return Receipt{}, fmt.Errorf("%w: command %s v%d", ErrDescriptorConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.route == nil {
		return Receipt{}, fmt.Errorf("%w: command %s v%d", ErrRouteNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	metadata, err := m.newMetadata(ctx, descriptor.info, outgoing.Metadata)
	if err != nil {
		return Receipt{}, err
	}
	delivery := binding.delivery(metadata, outgoing.Payload)
	return m.deliver(ctx, binding.route, delivery)
}

// Publish publishes an event with generated metadata.
func (m *Messenger) Publish[T any](ctx context.Context, descriptor Event[T], payload T) (Receipt, error) {
	return m.PublishMessage(ctx, descriptor, Outgoing[T]{Payload: payload})
}

// PublishMessage publishes an event with explicit optional metadata.
func (m *Messenger) PublishMessage[T any](
	ctx context.Context,
	descriptor Event[T],
	outgoing Outgoing[T],
) (Receipt, error) {
	if ctx == nil {
		return Receipt{}, fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	binding, ok := m.events[keyFor(descriptor.info)]
	if !ok || !sameDescriptor(binding.descriptor, descriptor.info, reflect.TypeFor[T]()) {
		return Receipt{}, fmt.Errorf("%w: event %s v%d", ErrDescriptorConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.route == nil {
		return Receipt{}, fmt.Errorf("%w: event %s v%d", ErrRouteNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	metadata, err := m.newMetadata(ctx, descriptor.info, outgoing.Metadata)
	if err != nil {
		return Receipt{}, err
	}
	delivery := binding.delivery(metadata, outgoing.Payload)
	return m.deliver(ctx, binding.route, delivery)
}

// Query executes a typed local request/reply call through its configured route.
func (m *Messenger) Query[Q, R any](
	ctx context.Context,
	descriptor Query[Q, R],
	payload Q,
) (R, error) {
	var zero R
	if ctx == nil {
		return zero, fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	binding, ok := m.queries[keyFor(descriptor.info)]
	if !ok || !sameDescriptor(binding.descriptor, descriptor.info, reflect.TypeFor[Q]()) ||
		binding.resultType != reflect.TypeFor[R]() || descriptor.resultType != reflect.TypeFor[R]() {
		return zero, fmt.Errorf("%w: query %s v%d", ErrDescriptorConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.handler == nil {
		return zero, fmt.Errorf("%w: query %s v%d", ErrHandlerNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.route == nil {
		return zero, fmt.Errorf("%w: query %s v%d", ErrRouteNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	metadata, err := m.newQueryMetadata(ctx, descriptor.info)
	if err != nil {
		return zero, err
	}
	started := m.clock().UTC()
	result, err := binding.route.query(ctx, binding.call(metadata, payload))
	if err == nil {
		zero, err = queryResultAs[R](result)
	}
	if m.observer != nil {
		observe(ctx, m.observer, Observation{
			Operation:     OperationQuery,
			MessageID:     metadata.ID,
			Kind:          metadata.Kind,
			Name:          metadata.Name,
			SchemaVersion: metadata.SchemaVersion,
			Route:         binding.route.Name(),
			HandlerID:     binding.handlerID,
			StartedAt:     started,
			Duration:      m.clock().UTC().Sub(started),
			Err:           err,
		})
	}
	return zero, err
}

// BindSender returns a narrow DI facade bound to one command descriptor.
func BindSender[T any](messenger *Messenger, descriptor Command[T]) Sender[T] {
	return boundSender[T]{messenger: messenger, descriptor: descriptor}
}

// BindPublisher returns a narrow DI facade bound to one event descriptor.
func BindPublisher[T any](messenger *Messenger, descriptor Event[T]) Publisher[T] {
	return boundPublisher[T]{messenger: messenger, descriptor: descriptor}
}

// BindBatchSender returns a narrow atomic batch facade bound to one command.
func BindBatchSender[T any](messenger *Messenger, descriptor Command[T]) BatchSender[T] {
	return boundBatchSender[T]{messenger: messenger, descriptor: descriptor}
}

// BindBatchPublisher returns a narrow atomic batch facade bound to one event.
func BindBatchPublisher[T any](messenger *Messenger, descriptor Event[T]) BatchPublisher[T] {
	return boundBatchPublisher[T]{messenger: messenger, descriptor: descriptor}
}

// BindQuerier returns a narrow DI facade bound to one query descriptor.
func BindQuerier[Q, R any](messenger *Messenger, descriptor Query[Q, R]) Querier[Q, R] {
	return boundQuerier[Q, R]{messenger: messenger, descriptor: descriptor}
}

type boundSender[T any] struct {
	messenger  *Messenger
	descriptor Command[T]
}

func (s boundSender[T]) Send(ctx context.Context, payload T) (Receipt, error) {
	return s.messenger.Send(ctx, s.descriptor, payload)
}

func (s boundSender[T]) SendMessage(ctx context.Context, outgoing Outgoing[T]) (Receipt, error) {
	return s.messenger.SendMessage(ctx, s.descriptor, outgoing)
}

type boundPublisher[T any] struct {
	messenger  *Messenger
	descriptor Event[T]
}

type boundBatchSender[T any] struct {
	messenger  *Messenger
	descriptor Command[T]
}

type boundBatchPublisher[T any] struct {
	messenger  *Messenger
	descriptor Event[T]
}

type boundQuerier[Q, R any] struct {
	messenger  *Messenger
	descriptor Query[Q, R]
}

func (p boundPublisher[T]) Publish(ctx context.Context, payload T) (Receipt, error) {
	return p.messenger.Publish(ctx, p.descriptor, payload)
}

func (p boundPublisher[T]) PublishMessage(ctx context.Context, outgoing Outgoing[T]) (Receipt, error) {
	return p.messenger.PublishMessage(ctx, p.descriptor, outgoing)
}

func (s boundBatchSender[T]) SendBatch(ctx context.Context, payloads []T) ([]Receipt, error) {
	return s.messenger.SendBatch(ctx, s.descriptor, payloads)
}

func (s boundBatchSender[T]) SendMessageBatch(ctx context.Context, outgoing []Outgoing[T]) ([]Receipt, error) {
	return s.messenger.SendMessageBatch(ctx, s.descriptor, outgoing)
}

func (p boundBatchPublisher[T]) PublishBatch(ctx context.Context, payloads []T) ([]Receipt, error) {
	return p.messenger.PublishBatch(ctx, p.descriptor, payloads)
}

func (p boundBatchPublisher[T]) PublishMessageBatch(ctx context.Context, outgoing []Outgoing[T]) ([]Receipt, error) {
	return p.messenger.PublishMessageBatch(ctx, p.descriptor, outgoing)
}

func (q boundQuerier[Q, R]) Query(ctx context.Context, payload Q) (R, error) {
	return q.messenger.Query(ctx, q.descriptor, payload)
}

func (m *Messenger) deliver(ctx context.Context, route Route, delivery Delivery) (Receipt, error) {
	metadata := delivery.Metadata()
	started := m.clock().UTC()
	receipt, err := route.Deliver(ctx, delivery)
	if err == nil {
		err = normalizeReceipt(&receipt, metadata, route, m.clock)
	}
	if m.observer != nil {
		observe(ctx, m.observer, Observation{
			Operation:     OperationDeliver,
			MessageID:     metadata.ID,
			Kind:          metadata.Kind,
			Name:          metadata.Name,
			SchemaVersion: metadata.SchemaVersion,
			Route:         route.Name(),
			State:         receipt.State,
			StartedAt:     started,
			Duration:      m.clock().UTC().Sub(started),
			Err:           err,
		})
	}
	return receipt, err
}

// normalizeReceipt fills defaults and validates one route receipt against the
// delivery metadata and route identity.
func normalizeReceipt(receipt *Receipt, metadata Metadata, route Route, clock func() time.Time) error {
	if receipt.MessageID.IsZero() {
		receipt.MessageID = metadata.ID
	}
	if receipt.MessageID != metadata.ID {
		return fmt.Errorf("%w: route changed message identity", ErrInvalidMessage)
	}
	if receipt.Route == "" {
		receipt.Route = route.Name()
	}
	if receipt.Route != route.Name() || receipt.State == "" {
		return fmt.Errorf("%w: invalid receipt from route %s", ErrInvalidMessage, route.Name())
	}
	// Messenger reports the normalized boundary using its configured clock.
	// Routes that expose direct publishing surfaces still return their own
	// post-confirmation timestamp when called without Messenger.
	receipt.At = clock().UTC()
	return nil
}

func (m *Messenger) newMetadata(
	ctx context.Context,
	info DescriptorInfo,
	outgoing OutgoingMetadata,
) (Metadata, error) {
	now := outgoing.Time
	if now.IsZero() {
		now = m.clock()
	}
	now = now.UTC()
	id := outgoing.ID
	if id.IsZero() {
		generated, err := m.idGenerator.New()
		if err != nil {
			return Metadata{}, err
		}
		id = generated
	}
	correlationID := outgoing.CorrelationID
	causationID := outgoing.CausationID
	if parent, ok := MetadataFromContext(ctx); ok {
		if correlationID.IsZero() {
			correlationID = parent.CorrelationID
			if correlationID.IsZero() {
				correlationID = parent.ID
			}
		}
		if causationID.IsZero() {
			causationID = parent.ID
		}
	}
	if correlationID.IsZero() {
		correlationID = id
	}
	headers := maps.Clone(outgoing.Headers)
	if _, noop := m.propagator.(noopContextPropagator); !noop {
		if headers == nil {
			headers = make(map[string]string)
		}
		m.propagator.Inject(ctx, headers)
	}
	if len(headers) == 0 {
		headers = nil
	}
	metadata := Metadata{
		ID:            id,
		Kind:          info.Kind,
		Name:          info.Name,
		SchemaVersion: info.SchemaVersion,
		Source:        m.source,
		Subject:       outgoing.Subject,
		Time:          now,
		CorrelationID: correlationID,
		CausationID:   causationID,
		Key:           outgoing.Key,
		ContentType:   info.ContentType,
		Schema:        info.Schema,
		Headers:       headers,
		NotBefore:     utcOrZero(outgoing.NotBefore),
		ExpiresAt:     utcOrZero(outgoing.ExpiresAt),
	}
	if err := validateMetadata(metadata); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

func (m *Messenger) newQueryMetadata(ctx context.Context, info DescriptorInfo) (Metadata, error) {
	return m.newMetadata(ctx, info, OutgoingMetadata{})
}

func validateMetadata(metadata Metadata) error {
	if metadata.ID.IsZero() || metadata.CorrelationID.IsZero() || metadata.Time.IsZero() ||
		!metadata.Kind.valid() || metadata.Name == "" || metadata.SchemaVersion <= 0 ||
		metadata.Source == "" || metadata.ContentType == "" {
		return fmt.Errorf("%w: incomplete metadata", ErrInvalidMessage)
	}
	if !metadata.NotBefore.IsZero() && !metadata.ExpiresAt.IsZero() &&
		!metadata.ExpiresAt.After(metadata.NotBefore) {
		return fmt.Errorf("%w: expiresAt must follow notBefore", ErrInvalidMessage)
	}
	return validateHeaders(metadata.Headers)
}

func makeCommandBinding(
	registration *commandRegistration,
	observer Observer,
	clock func() time.Time,
	middleware []Middleware,
	panicReporter PanicReporter,
) commandBinding {
	return commandBinding{
		descriptor:    registration.descriptor,
		handlerID:     registration.handlerID,
		handler:       registration.handler,
		route:         registration.route,
		observer:      observer,
		clock:         clock,
		middleware:    append([]Middleware(nil), middleware...),
		panicReporter: panicReporter,
	}
}

func makeQueryBinding(
	registration *queryRegistration,
	observer Observer,
	clock func() time.Time,
	middleware []Middleware,
	panicReporter PanicReporter,
) queryBinding {
	return queryBinding{
		descriptor:    registration.descriptor,
		resultType:    registration.resultType,
		handlerID:     registration.handlerID,
		handler:       registration.handler,
		route:         registration.route,
		observer:      observer,
		clock:         clock,
		middleware:    append([]Middleware(nil), middleware...),
		panicReporter: panicReporter,
	}
}

func (b queryBinding) call(metadata Metadata, payload any) localQueryCall {
	return localQueryCall{
		metadata: metadata,
		invoke: func(ctx context.Context) (localQueryResult, error) {
			handler, err := b.handler(payload)
			if err != nil {
				return localQueryResult{}, err
			}
			started := b.clock().UTC()
			var result localQueryResult
			err = invokeMiddleware(ctx, metadata, b.handlerID, func(current context.Context) error {
				var handlerErr error
				result, handlerErr = handler(current)
				return handlerErr
			}, b.middleware, b.panicReporter)
			if err == nil && !result.present {
				err = ErrQueryResultMissing
			}
			if b.observer != nil {
				observe(ctx, b.observer, Observation{
					Operation:     OperationHandle,
					MessageID:     metadata.ID,
					Kind:          metadata.Kind,
					Name:          metadata.Name,
					SchemaVersion: metadata.SchemaVersion,
					Route:         b.route.Name(),
					HandlerID:     b.handlerID,
					StartedAt:     started,
					Duration:      b.clock().UTC().Sub(started),
					Err:           err,
				})
			}
			return result, err
		},
	}
}

func queryResultAs[R any](result localQueryResult) (R, error) {
	var zero R
	expected := reflect.TypeFor[R]()
	if !result.present {
		return zero, ErrQueryResultMissing
	}
	if result.expectedOutputType != expected {
		return zero, fmt.Errorf("%w: query result type", ErrDescriptorConflict)
	}
	if result.value == nil {
		return zero, nil
	}
	typed, ok := result.value.(R)
	if !ok {
		return zero, fmt.Errorf("%w: query result value", ErrDescriptorConflict)
	}
	return typed, nil
}

func (b commandBinding) delivery(metadata Metadata, payload any) Delivery {
	return &delivery{
		onExpire: expiryObserver(b.observer, metadata, b.route.Name()),
		metadata: metadata,
		encode: func() ([]byte, DataEncoding, error) {
			return b.descriptor.encode(payload)
		},
		handlers: boolInt(b.handler != nil),
		invoke: func(ctx context.Context) error {
			if b.handler == nil {
				return fmt.Errorf("%w: command %s v%d", ErrHandlerNotFound,
					metadata.Name, metadata.SchemaVersion)
			}
			handler, err := b.handler(payload)
			if err != nil {
				return err
			}
			started := b.clock().UTC()
			err = invokeMiddleware(ctx, metadata, b.handlerID, handler, b.middleware, b.panicReporter)
			if b.observer != nil {
				observe(ctx, b.observer, Observation{
					Operation:     OperationHandle,
					MessageID:     metadata.ID,
					Kind:          metadata.Kind,
					Name:          metadata.Name,
					SchemaVersion: metadata.SchemaVersion,
					HandlerID:     b.handlerID,
					StartedAt:     started,
					Duration:      b.clock().UTC().Sub(started),
					Err:           err,
				})
			}
			return err
		},
	}
}

func makeEventBinding(
	registration *eventRegistration,
	observer Observer,
	clock func() time.Time,
	middleware []Middleware,
	panicReporter PanicReporter,
) eventBinding {
	subscribers := make([]boundEventSubscriber, len(registration.subscribers))
	for index, subscriber := range registration.subscribers {
		subscribers[index] = boundEventSubscriber(subscriber)
	}
	return eventBinding{
		descriptor:    registration.descriptor,
		subscribers:   subscribers,
		route:         registration.route,
		observer:      observer,
		clock:         clock,
		middleware:    append([]Middleware(nil), middleware...),
		panicReporter: panicReporter,
	}
}

func (b eventBinding) delivery(metadata Metadata, payload any) Delivery {
	return &delivery{
		onExpire: expiryObserver(b.observer, metadata, b.route.Name()),
		metadata: metadata,
		encode: func() ([]byte, DataEncoding, error) {
			return b.descriptor.encode(payload)
		},
		handlers: len(b.subscribers),
		invoke: func(ctx context.Context) error {
			var handlerErrors []error
			for _, subscriber := range b.subscribers {
				handler, err := subscriber.handler(payload)
				if err != nil {
					handlerErrors = append(handlerErrors, err)
					continue
				}
				started := b.clock().UTC()
				err = invokeMiddleware(ctx, metadata, subscriber.id, handler, b.middleware, b.panicReporter)
				if b.observer != nil {
					observe(ctx, b.observer, Observation{
						Operation:     OperationHandle,
						MessageID:     metadata.ID,
						Kind:          metadata.Kind,
						Name:          metadata.Name,
						SchemaVersion: metadata.SchemaVersion,
						HandlerID:     subscriber.id,
						StartedAt:     started,
						Duration:      b.clock().UTC().Sub(started),
						Err:           err,
					})
				}
				if err != nil {
					handlerErrors = append(handlerErrors, err)
				}
			}
			return errors.Join(handlerErrors...)
		},
	}
}

func invokeMiddleware(
	ctx context.Context,
	metadata Metadata,
	handlerID string,
	handler HandlerFunc,
	middlewares []Middleware,
	panicReporter PanicReporter,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = ReportHandlerPanic(ctx, panicReporter, handlerID, recovered, debug.Stack())
		}
	}()
	if len(middlewares) == 0 {
		return handler(ctx)
	}
	var invoke func(int, context.Context) error
	invoke = func(index int, current context.Context) error {
		if current == nil {
			return fmt.Errorf("%w: middleware supplied a nil context", ErrInvalidMessage)
		}
		if index == len(middlewares) {
			return handler(current)
		}
		var called atomic.Bool
		next := func(nextContext context.Context) error {
			if !called.CompareAndSwap(false, true) {
				return fmt.Errorf("%w: middleware called next more than once", ErrInvalidMessage)
			}
			return invoke(index+1, nextContext)
		}
		return middlewares[index](current, cloneMetadata(metadata), handlerID, next)
	}
	return invoke(0, ctx)
}

func utcOrZero(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
