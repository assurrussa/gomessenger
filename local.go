package messenger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/assurrussa/gobus"
	gobusasync "github.com/assurrussa/gobus/async"
)

type localCall struct{ delivery Delivery }

type localCallExecutor struct{}

func (localCallExecutor) Execute(ctx context.Context, call localCall) error {
	return call.delivery.Invoke(ctx)
}

// LocalSyncRoute executes handlers synchronously through a private GoBus instance.
type LocalSyncRoute struct{ bus *gobus.Bus }

// NewLocalSyncRoute constructs a local synchronous route.
func NewLocalSyncRoute() *LocalSyncRoute {
	bus := gobus.New()
	bus.Register[localCall](localCallExecutor{})
	return &LocalSyncRoute{bus: bus}
}

// Name implements Route.
func (*LocalSyncRoute) Name() string { return "local.sync" }

// Deliver implements Route.
func (r *LocalSyncRoute) Deliver(ctx context.Context, delivery Delivery) (Receipt, error) {
	if ctx == nil || delivery == nil {
		return Receipt{}, fmt.Errorf("%w: local sync delivery", ErrInvalidMessage)
	}
	metadata := delivery.Metadata()
	if metadata.Kind == KindEvent && delivery.HandlerCount() == 0 {
		return Receipt{MessageID: metadata.ID, Route: r.Name(), State: ReceiptNoop, At: time.Now().UTC()}, nil
	}
	if err := r.bus.Dispatch(ctx, localCall{delivery: delivery}); err != nil {
		return Receipt{}, err
	}
	return Receipt{MessageID: metadata.ID, Route: r.Name(), State: ReceiptCompleted, At: time.Now().UTC()}, nil
}

func (*LocalSyncRoute) requiresLocalHandler() {}

// LocalAsyncConfig bounds local asynchronous admission and execution.
type LocalAsyncConfig struct {
	Capacity int
	Workers  int
	// DetachExecution is retained for source compatibility. Accepted jobs
	// always detach execution from the caller's cancellation and deadline.
	//
	// Deprecated: caller context controls admission only.
	DetachExecution bool
}

// LocalAsyncRoute admits handler calls to a bounded GoBus async runtime.
type LocalAsyncRoute struct {
	name    string
	runtime *gobusasync.Runtime

	runMu       sync.Mutex
	running     bool
	draining    bool
	closed      bool
	everStarted bool
	drainOnce   sync.Once
	drainDone   chan struct{}
	drainStop   context.CancelFunc
	drainErr    error
}

// NewLocalAsyncRoute constructs a named bounded local asynchronous route.
func NewLocalAsyncRoute(name string, config LocalAsyncConfig) (*LocalAsyncRoute, error) {
	if !validStableID(name) {
		return nil, fmt.Errorf("%w: invalid async route name %q", ErrRouteNotFound, name)
	}
	bus := gobus.New()
	bus.Register[localCall](localCallExecutor{})
	runtime, err := gobusasync.New(bus, gobusasync.QueueConfig{
		Capacity: config.Capacity,
		Workers:  config.Workers,
	})
	if err != nil {
		return nil, fmt.Errorf("messenger: create local async route: %w", err)
	}
	return &LocalAsyncRoute{
		name:      name,
		runtime:   runtime,
		drainDone: make(chan struct{}),
	}, nil
}

// Name implements Route.
func (r *LocalAsyncRoute) Name() string { return r.name }

// Deliver implements Route and reports admission, not handler completion.
func (r *LocalAsyncRoute) Deliver(ctx context.Context, delivery Delivery) (Receipt, error) {
	if ctx == nil || delivery == nil {
		return Receipt{}, fmt.Errorf("%w: local async delivery", ErrInvalidMessage)
	}
	r.runMu.Lock()
	running := r.running && !r.draining && !r.closed
	r.runMu.Unlock()
	if !running {
		return Receipt{}, ErrRuntimeNotRunning
	}
	if _, err := r.runtime.Submit(ctx, localCall{delivery: delivery},
		gobusasync.WithExecutionContext(context.WithoutCancel(ctx))); err != nil {
		return Receipt{}, fmt.Errorf("messenger: async admission: %w", err)
	}
	metadata := delivery.Metadata()
	return Receipt{MessageID: metadata.ID, Route: r.Name(), State: ReceiptAccepted, At: time.Now().UTC()}, nil
}

func (*LocalAsyncRoute) requiresLocalHandler() {}

// ManagedService exposes this route to Builder runtime aggregation.
func (r *LocalAsyncRoute) ManagedService() (string, Service) { return r.name, r }

// Run starts queue workers and blocks until cancellation or draining.
func (r *LocalAsyncRoute) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.runMu.Lock()
	if r.running {
		r.runMu.Unlock()
		return ErrRuntimeRunning
	}
	if r.draining && !r.everStarted {
		r.everStarted = true
		drainDone := r.drainDone
		r.runMu.Unlock()
		<-drainDone
		r.runMu.Lock()
		r.closed = true
		drainErr := r.drainErr
		r.runMu.Unlock()
		return drainErr
	}
	if r.closed {
		r.runMu.Unlock()
		return ErrRuntimeClosed
	}
	if err := r.runtime.Start(); err != nil {
		r.runMu.Unlock()
		return fmt.Errorf("messenger: start local async route: %w", err)
	}
	r.running = true
	r.everStarted = true
	r.runMu.Unlock()

	select {
	case <-ctx.Done():
		r.beginDrain(context.WithoutCancel(ctx))
		r.forceDrain()
		<-r.drainDone
		return ctx.Err()
	case <-r.drainDone:
		return r.drainErr
	}
}

func (r *LocalAsyncRoute) forceDrain() {
	r.runMu.Lock()
	stop := r.drainStop
	r.runMu.Unlock()
	if stop != nil {
		stop()
	}
}

// Readiness verifies that this route is accepting work.
func (r *LocalAsyncRoute) Readiness(context.Context) error {
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if !r.running || r.draining {
		return ErrRuntimeNotRunning
	}
	stats := r.runtime.Stats()
	if stats.State != gobusasync.StateRunning {
		return fmt.Errorf("%w: local async state %s", ErrRuntimeNotRunning, stats.State)
	}
	return nil
}

// BeginDrain rejects new work and drains accepted jobs.
func (r *LocalAsyncRoute) BeginDrain() {
	r.beginDrain(context.Background())
}

// beginDrain starts the drain with an explicit parent context so callers can
// propagate cancellation from their own context.
func (r *LocalAsyncRoute) beginDrain(parent context.Context) {
	r.drainOnce.Do(func() {
		drainContext, stop := context.WithCancel(parent)
		r.runMu.Lock()
		r.draining = true
		r.drainStop = stop
		r.runMu.Unlock()
		go func() {
			err := r.runtime.Shutdown(drainContext)
			r.runMu.Lock()
			r.drainErr = err
			r.running = false
			if r.everStarted {
				r.closed = true
			}
			r.runMu.Unlock()
			close(r.drainDone)
		}()
	})
}

// Shutdown drains or force-cancels the route within ctx.
func (r *LocalAsyncRoute) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.beginDrain(context.WithoutCancel(ctx))
	select {
	case <-r.drainDone:
		r.runMu.Lock()
		r.closed = true
		drainErr := r.drainErr
		r.runMu.Unlock()
		return drainErr
	case <-ctx.Done():
		r.forceDrain()
		return ctx.Err()
	}
}
