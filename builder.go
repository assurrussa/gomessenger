package messenger

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// Option configures a Builder.
type Option func(*Builder)

// WithSource sets the required stable producer identity.
func WithSource(source string) Option {
	return func(builder *Builder) { builder.source = source }
}

// WithIDGenerator overrides UUIDv7 generation, primarily for deterministic tests.
func WithIDGenerator(generator IDGenerator) Option {
	return func(builder *Builder) {
		if nilInterface(generator) {
			builder.addError(fmt.Errorf("%w: nil ID generator", ErrInvalidMessage))
			return
		}
		builder.idGenerator = generator
	}
}

// WithClock overrides the UTC wall clock used for messages and receipts.
func WithClock(clock func() time.Time) Option {
	return func(builder *Builder) {
		if clock == nil {
			builder.addError(fmt.Errorf("%w: nil clock", ErrInvalidMessage))
			return
		}
		builder.clock = clock
	}
}

// WithLogger sets the core structured logger. The default logger is a no-op.
func WithLogger(logger Logger) Option {
	return func(builder *Builder) {
		if nilInterface(logger) {
			builder.addError(fmt.Errorf("%w: nil logger", ErrInvalidMessage))
			return
		}
		builder.logger = logger
	}
}

// WithObserver appends a lifecycle observer.
func WithObserver(observer Observer) Option {
	return func(builder *Builder) {
		if nilInterface(observer) {
			builder.addError(fmt.Errorf("%w: nil observer", ErrInvalidMessage))
			return
		}
		builder.observers = append(builder.observers, observer)
	}
}

// WithContextPropagator sets distributed-context injection for outgoing
// metadata. The default propagator is a no-op.
func WithContextPropagator(propagator ContextPropagator) Option {
	return func(builder *Builder) {
		if nilInterface(propagator) {
			builder.addError(fmt.Errorf("%w: nil context propagator", ErrInvalidMessage))
			return
		}
		builder.propagator = propagator
	}
}

// WithPanicReporter enables explicit handling of sensitive recovered-panic
// values and stacks. Without it, only a sanitized HandlerPanicError is emitted.
func WithPanicReporter(reporter PanicReporter) Option {
	return func(builder *Builder) {
		if nilInterface(reporter) {
			builder.addError(fmt.Errorf("%w: nil panic reporter", ErrInvalidMessage))
			return
		}
		builder.panicReporter = reporter
	}
}

// WithRuntimeShutdownTimeout sets the internal bound used when Run owns service
// shutdown after cancellation or an unexpected service return.
func WithRuntimeShutdownTimeout(timeout time.Duration) Option {
	return func(builder *Builder) {
		if timeout <= 0 {
			builder.addError(fmt.Errorf("%w: runtime shutdown timeout must be positive", ErrInvalidMessage))
			return
		}
		builder.runtimeShutdownTimeout = timeout
	}
}

type descriptorRegistration struct {
	info        DescriptorInfo
	payloadType reflect.Type
	encode      func(any) ([]byte, DataEncoding, error)
}

type commandRegistration struct {
	descriptor descriptorRegistration
	handlerID  string
	handler    func(any) (funcContextHandler, error)
	route      Route
}

type queryRegistration struct {
	descriptor descriptorRegistration
	resultType reflect.Type
	handlerID  string
	handler    func(any) (funcContextQueryHandler, error)
	route      LocalQueryRoute
}

type eventSubscriber struct {
	id      string
	handler func(any) (funcContextHandler, error)
}

type eventRegistration struct {
	descriptor  descriptorRegistration
	subscribers []eventSubscriber
	route       Route
}

type funcContextHandler = HandlerFunc

type funcContextQueryHandler func(context.Context) (localQueryResult, error)

type namedService struct {
	id      string
	service Service
	valueID uintptr
}

// Builder declares immutable descriptors, handlers, routes, and managed services.
// It is not safe for concurrent mutation.
type Builder struct {
	source                 string
	idGenerator            IDGenerator
	clock                  func() time.Time
	logger                 Logger
	observers              []Observer
	propagator             ContextPropagator
	panicReporter          PanicReporter
	runtimeShutdownTimeout time.Duration
	middlewares            []Middleware

	commands map[descriptorKey]*commandRegistration
	events   map[descriptorKey]*eventRegistration
	queries  map[descriptorKey]*queryRegistration
	services map[string]namedService
	errors   []error
}

// NewBuilder constructs an empty messenger builder.
func NewBuilder(options ...Option) *Builder {
	builder := &Builder{
		idGenerator:            UUIDv7Generator(),
		clock:                  time.Now,
		logger:                 noopLogger{},
		propagator:             noopContextPropagator{},
		runtimeShutdownTimeout: defaultRuntimeShutdownTimeout,
		commands:               make(map[descriptorKey]*commandRegistration),
		events:                 make(map[descriptorKey]*eventRegistration),
		queries:                make(map[descriptorKey]*queryRegistration),
		services:               make(map[string]namedService),
	}
	for _, option := range options {
		if option == nil {
			builder.addError(fmt.Errorf("%w: nil builder option", ErrInvalidMessage))
			continue
		}
		option(builder)
	}
	return builder
}

// UseMiddleware appends global handler middleware. Validation errors are
// returned by Build.
func (b *Builder) UseMiddleware(middlewares ...Middleware) {
	for _, middleware := range middlewares {
		if middleware == nil {
			b.addError(fmt.Errorf("%w: nil middleware", ErrInvalidMessage))
			continue
		}
		b.middlewares = append(b.middlewares, middleware)
	}
}

// HandleCommand registers the one local handler for a command descriptor.
// Validation errors are returned by Build.
func (b *Builder) HandleCommand[T any](descriptor Command[T], handlerID string, handler Handler[T]) {
	registration := b.ensureCommand(descriptor)
	if registration == nil {
		return
	}
	if !validStableID(handlerID) || handler == nil {
		b.addError(fmt.Errorf("%w: invalid handler %q for %s", ErrHandlerConflict, handlerID, descriptor.info.Name))
		return
	}
	if registration.handler != nil {
		b.addError(fmt.Errorf("%w: command %s v%d", ErrHandlerConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion))
		return
	}
	registration.handlerID = handlerID
	registration.handler = func(payload any) (funcContextHandler, error) {
		typed, ok := payload.(T)
		if !ok {
			return nil, fmt.Errorf("%w: command payload type for %s", ErrDescriptorConflict, descriptor.info.Name)
		}
		return func(ctx context.Context) error {
			metadata, _ := MetadataFromContext(ctx)
			return handler(ctx, Message[T]{Metadata: metadata, Payload: typed})
		}, nil
	}
}

// HandleCommandFunc registers a payload-only command handler.
func (b *Builder) HandleCommandFunc[T any](
	descriptor Command[T],
	handlerID string,
	handler PayloadHandler[T],
) {
	b.HandleCommand(descriptor, handlerID, HandlePayload(handler))
}

// HandleQuery registers the one local handler for a query descriptor.
// Validation errors are returned by Build.
func (b *Builder) HandleQuery[Q, R any](
	descriptor Query[Q, R],
	handlerID string,
	handler QueryHandler[Q, R],
) {
	registration := b.ensureQuery(descriptor)
	if registration == nil {
		return
	}
	if !validStableID(handlerID) || handler == nil {
		b.addError(fmt.Errorf("%w: invalid query handler %q for %s",
			ErrHandlerConflict, handlerID, descriptor.info.Name))
		return
	}
	if registration.handler != nil {
		b.addError(fmt.Errorf("%w: query %s v%d", ErrHandlerConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion))
		return
	}
	registration.handlerID = handlerID
	registration.handler = func(payload any) (funcContextQueryHandler, error) {
		var typed Q
		if payload == nil {
			// A nil interface loses its dynamic type when it is erased to any.
			if reflect.TypeFor[Q]().Kind() != reflect.Interface {
				return nil, fmt.Errorf("%w: query payload type for %s", ErrDescriptorConflict, descriptor.info.Name)
			}
		} else {
			var ok bool
			typed, ok = payload.(Q)
			if !ok {
				return nil, fmt.Errorf("%w: query payload type for %s", ErrDescriptorConflict, descriptor.info.Name)
			}
		}
		return func(ctx context.Context) (localQueryResult, error) {
			metadata, _ := MetadataFromContext(ctx)
			result, err := handler(ctx, Message[Q]{Metadata: metadata, Payload: typed})
			if err != nil {
				return localQueryResult{expectedOutputType: descriptor.resultType}, err
			}
			return localQueryResult{
				value:              result,
				expectedOutputType: descriptor.resultType,
				present:            true,
			}, nil
		}, nil
	}
}

// HandleQueryFunc registers a payload-only local query handler.
func (b *Builder) HandleQueryFunc[Q, R any](
	descriptor Query[Q, R],
	handlerID string,
	handler QueryPayloadHandler[Q, R],
) {
	b.HandleQuery(descriptor, handlerID, HandleQueryPayload(handler))
}

// Subscribe appends a named local event subscription.
// Validation errors are returned by Build.
func (b *Builder) Subscribe[T any](descriptor Event[T], subscriptionID string, handler Handler[T]) {
	registration := b.ensureEvent(descriptor)
	if registration == nil {
		return
	}
	if !validStableID(subscriptionID) || handler == nil {
		b.addError(fmt.Errorf("%w: invalid subscription %q for %s",
			ErrHandlerConflict, subscriptionID, descriptor.info.Name))
		return
	}
	for _, subscriber := range registration.subscribers {
		if subscriber.id == subscriptionID {
			b.addError(fmt.Errorf("%w: duplicate subscription %q for %s",
				ErrHandlerConflict, subscriptionID, descriptor.info.Name))
			return
		}
	}
	registration.subscribers = append(registration.subscribers, eventSubscriber{
		id: subscriptionID,
		handler: func(payload any) (funcContextHandler, error) {
			typed, ok := payload.(T)
			if !ok {
				return nil, fmt.Errorf("%w: event payload type for %s", ErrDescriptorConflict, descriptor.info.Name)
			}
			return func(ctx context.Context) error {
				metadata, _ := MetadataFromContext(ctx)
				return handler(ctx, Message[T]{Metadata: metadata, Payload: typed})
			}, nil
		},
	})
}

// SubscribeFunc appends a payload-only event subscription.
func (b *Builder) SubscribeFunc[T any](
	descriptor Event[T],
	subscriptionID string,
	handler PayloadHandler[T],
) {
	b.Subscribe(descriptor, subscriptionID, HandlePayload(handler))
}

// RouteCommand sets the command's one primary outbound route.
func (b *Builder) RouteCommand[T any](descriptor Command[T], route Route) {
	registration := b.ensureCommand(descriptor)
	if registration == nil || !b.validateRoute(descriptor.info, route) {
		return
	}
	if registration.route != nil {
		b.addError(fmt.Errorf("%w: command %s v%d", ErrRouteConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion))
		return
	}
	registration.route = route
	b.addRouteService(route)
}

// RouteEvent sets the event's one primary outbound route.
func (b *Builder) RouteEvent[T any](descriptor Event[T], route Route) {
	registration := b.ensureEvent(descriptor)
	if registration == nil || !b.validateRoute(descriptor.info, route) {
		return
	}
	if registration.route != nil {
		b.addError(fmt.Errorf("%w: event %s v%d", ErrRouteConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion))
		return
	}
	registration.route = route
	b.addRouteService(route)
}

// RouteQuery sets the query's required local request/reply route.
func (b *Builder) RouteQuery[Q, R any](descriptor Query[Q, R], route LocalQueryRoute) {
	registration := b.ensureQuery(descriptor)
	if registration == nil || !b.validateQueryRoute(descriptor.info, route) {
		return
	}
	if registration.route != nil {
		b.addError(fmt.Errorf("%w: query %s v%d", ErrRouteConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion))
		return
	}
	registration.route = route
	b.addQueryRouteService(route)
}

// Use adds a named managed consumer or worker service to the returned Runtime.
func (b *Builder) Use(serviceID string, service Service) {
	b.addService(serviceID, service)
}

// Build validates and freezes the declared topology.
func (b *Builder) Build() (*Messenger, *Runtime, error) {
	if err := validateSource(b.source); err != nil {
		b.addError(err)
	}
	for _, command := range b.commands {
		if command.route != nil {
			if _, ok := command.route.(localRoute); ok && command.handler == nil {
				b.addError(fmt.Errorf("%w: local command %s v%d", ErrHandlerNotFound,
					command.descriptor.info.Name, command.descriptor.info.SchemaVersion))
			}
		}
	}
	for _, query := range b.queries {
		if query.handler == nil {
			b.addError(fmt.Errorf("%w: query %s v%d", ErrHandlerNotFound,
				query.descriptor.info.Name, query.descriptor.info.SchemaVersion))
		}
		if query.route == nil {
			b.addError(fmt.Errorf("%w: query %s v%d", ErrRouteNotFound,
				query.descriptor.info.Name, query.descriptor.info.SchemaVersion))
		}
	}
	if err := errors.Join(b.errors...); err != nil {
		return nil, nil, err
	}

	observer := newObserverSet(b.logger, b.observers)
	commands := make(map[descriptorKey]commandBinding, len(b.commands))
	for key, registration := range b.commands {
		commands[key] = makeCommandBinding(registration, observer, b.clock, b.middlewares, b.panicReporter)
	}
	events := make(map[descriptorKey]eventBinding, len(b.events))
	for key, registration := range b.events {
		events[key] = makeEventBinding(registration, observer, b.clock, b.middlewares, b.panicReporter)
	}
	queries := make(map[descriptorKey]queryBinding, len(b.queries))
	for key, registration := range b.queries {
		queries[key] = makeQueryBinding(registration, observer, b.clock, b.middlewares, b.panicReporter)
	}
	services := make([]namedService, 0, len(b.services))
	for _, service := range b.services {
		services = append(services, service)
	}
	messenger := &Messenger{
		source:      b.source,
		idGenerator: b.idGenerator,
		clock:       b.clock,
		logger:      b.logger,
		observer:    observer,
		propagator:  b.propagator,
		commands:    commands,
		events:      events,
		queries:     queries,
	}
	messenger.manifest = buildManifest(b.source, commands, events, queries, services)
	return messenger, newRuntime(
		services,
		b.logger,
		observer,
		b.panicReporter,
		b.runtimeShutdownTimeout,
	), nil
}

func (b *Builder) ensureCommand[T any](descriptor Command[T]) *commandRegistration {
	registration, ok := b.commands[keyFor(descriptor.info)]
	if ok {
		if !sameDescriptor(registration.descriptor, descriptor.info, reflect.TypeFor[T]()) {
			b.addError(fmt.Errorf("%w: command %s v%d", ErrDescriptorConflict,
				descriptor.info.Name, descriptor.info.SchemaVersion))
			return nil
		}
		return registration
	}
	registration = &commandRegistration{descriptor: eraseDescriptor(descriptor.descriptor)}
	b.commands[keyFor(descriptor.info)] = registration
	return registration
}

func (b *Builder) ensureEvent[T any](descriptor Event[T]) *eventRegistration {
	registration, ok := b.events[keyFor(descriptor.info)]
	if ok {
		if !sameDescriptor(registration.descriptor, descriptor.info, reflect.TypeFor[T]()) {
			b.addError(fmt.Errorf("%w: event %s v%d", ErrDescriptorConflict,
				descriptor.info.Name, descriptor.info.SchemaVersion))
			return nil
		}
		return registration
	}
	registration = &eventRegistration{descriptor: eraseDescriptor(descriptor.descriptor)}
	b.events[keyFor(descriptor.info)] = registration
	return registration
}

func (b *Builder) ensureQuery[Q, R any](descriptor Query[Q, R]) *queryRegistration {
	registration, ok := b.queries[keyFor(descriptor.info)]
	if ok {
		if !sameDescriptor(registration.descriptor, descriptor.info, reflect.TypeFor[Q]()) ||
			registration.resultType != descriptor.resultType || descriptor.resultType != reflect.TypeFor[R]() {
			b.addError(fmt.Errorf("%w: query %s v%d", ErrDescriptorConflict,
				descriptor.info.Name, descriptor.info.SchemaVersion))
			return nil
		}
		return registration
	}
	registration = &queryRegistration{
		descriptor: eraseDescriptor(descriptor.descriptor),
		resultType: descriptor.resultType,
	}
	b.queries[keyFor(descriptor.info)] = registration
	return registration
}

func eraseDescriptor[T any](descriptor descriptor[T]) descriptorRegistration {
	return descriptorRegistration{
		info:        descriptor.info,
		payloadType: reflect.TypeFor[T](),
		encode: func(payload any) ([]byte, DataEncoding, error) {
			typed, ok := payload.(T)
			if !ok {
				return nil, 0, fmt.Errorf("%w: payload for %s", ErrDescriptorConflict, descriptor.info.Name)
			}
			data, err := descriptor.codec.Encode(typed)
			return data, descriptor.codec.Encoding(), err
		},
	}
}

func sameDescriptor(existing descriptorRegistration, info DescriptorInfo, payloadType reflect.Type) bool {
	return existing.info == info && existing.payloadType == payloadType
}

func (b *Builder) validateRoute(info DescriptorInfo, route Route) bool {
	if route == nil {
		b.addError(fmt.Errorf("%w: nil route for %s", ErrRouteNotFound, info.Name))
		return false
	}
	value := reflect.ValueOf(route)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		b.addError(fmt.Errorf("%w: nil route for %s", ErrRouteNotFound, info.Name))
		return false
	}
	if !validStableID(route.Name()) {
		b.addError(fmt.Errorf("%w: invalid route name %q", ErrRouteNotFound, route.Name()))
		return false
	}
	return true
}

func (b *Builder) validateQueryRoute(info DescriptorInfo, route LocalQueryRoute) bool {
	if route == nil {
		b.addError(fmt.Errorf("%w: nil query route for %s", ErrRouteNotFound, info.Name))
		return false
	}
	value := reflect.ValueOf(route)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		b.addError(fmt.Errorf("%w: nil query route for %s", ErrRouteNotFound, info.Name))
		return false
	}
	if !validStableID(route.Name()) {
		b.addError(fmt.Errorf("%w: invalid query route name %q", ErrRouteNotFound, route.Name()))
		return false
	}
	return true
}

func (b *Builder) addRouteService(route Route) {
	provider, ok := route.(ServiceProvider)
	if !ok {
		return
	}
	serviceID, service := provider.ManagedService()
	b.addService(serviceID, service)
}

func (b *Builder) addQueryRouteService(route LocalQueryRoute) {
	provider, ok := route.(ServiceProvider)
	if !ok {
		return
	}
	serviceID, service := provider.ManagedService()
	b.addService(serviceID, service)
}

func (b *Builder) addService(serviceID string, service Service) {
	if !validStableID(serviceID) || service == nil {
		b.addError(fmt.Errorf("%w: invalid service %q", ErrServiceConflict, serviceID))
		return
	}
	value := reflect.ValueOf(service)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		b.addError(fmt.Errorf("%w: nil service %q", ErrServiceConflict, serviceID))
		return
	}
	var valueID uintptr
	if value.Kind() == reflect.Pointer {
		valueID = value.Pointer()
	}
	if existing, ok := b.services[serviceID]; ok {
		if valueID != 0 && existing.valueID == valueID {
			return
		}
		b.addError(fmt.Errorf("%w: %s", ErrServiceConflict, serviceID))
		return
	}
	b.services[serviceID] = namedService{id: serviceID, service: service, valueID: valueID}
}

func (b *Builder) addError(err error) { b.errors = append(b.errors, err) }

func nilInterface(value any) bool {
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

func validateSource(source string) error {
	if source == "" || strings.TrimSpace(source) != source {
		return fmt.Errorf("%w: source is required", ErrInvalidMessage)
	}
	if _, err := url.Parse(source); err != nil {
		return fmt.Errorf("%w: invalid source %q: %w", ErrInvalidMessage, source, err)
	}
	return nil
}

func validStableID(value string) bool {
	if value == "" || len(value) > maxDescriptorNameLength || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._:-/", character) {
			continue
		}
		return false
	}
	return true
}
