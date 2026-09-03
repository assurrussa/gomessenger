package messenger

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

const defaultRuntimeShutdownTimeout = 30 * time.Second

// Service is a host-supervised managed consumer or worker lifecycle.
// BeginDrain must be non-blocking, stop admission, and cause a running Run call
// to return without requiring Shutdown to be invoked first. Shutdown waits for
// or force-cancels remaining work within its context.
type Service interface {
	Run(ctx context.Context) error
	Readiness(ctx context.Context) error
	BeginDrain()
	Shutdown(ctx context.Context) error
}

// LivenessChecker optionally separates process liveness from readiness and
// transient broker or topology failures.
type LivenessChecker interface {
	Liveness(ctx context.Context) error
}

// DeepHealthChecker optionally performs expensive topology and infrastructure
// validation outside the normal readiness probe path.
type DeepHealthChecker interface {
	DeepHealth(ctx context.Context) error
}

// ServiceProvider lets a route contribute one managed service to Builder.
type ServiceProvider interface {
	ManagedService() (serviceID string, service Service)
}

type runtimeState uint8

const (
	runtimeNew runtimeState = iota
	runtimeRunning
	runtimeDraining
	runtimeClosed
)

// Runtime supervises the services declared on one immutable Builder.
// It never restarts a service automatically.
type Runtime struct {
	services        []namedService
	logger          Logger
	observer        Observer
	panicReporter   PanicReporter
	shutdownTimeout time.Duration

	mu        sync.Mutex
	state     runtimeState
	runCancel context.CancelFunc
	// shutdownStarted is set only when Shutdown owns a never-started
	// runtime. A started Run remains the lifecycle owner and closes done.
	shutdownStarted bool
	shutdownErr     error
	drainErr        error
	done            chan struct{}
	drain           chan struct{}
	drainOnce       sync.Once
	closeOnce       sync.Once
}

func newRuntime(
	services []namedService,
	logger Logger,
	observer Observer,
	panicReporter PanicReporter,
	shutdownTimeout time.Duration,
) *Runtime {
	sort.Slice(services, func(i, j int) bool { return services[i].id < services[j].id })
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultRuntimeShutdownTimeout
	}
	return &Runtime{
		services:        services,
		logger:          logger,
		observer:        observer,
		panicReporter:   panicReporter,
		shutdownTimeout: shutdownTimeout,
		done:            make(chan struct{}),
		drain:           make(chan struct{}),
	}
}

// Run starts every service and blocks until cancellation, draining, or the
// first unexpected service return.
func (r *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.mu.Lock()
	if r.state == runtimeRunning || r.state == runtimeDraining {
		r.mu.Unlock()
		return ErrRuntimeRunning
	}
	if r.state == runtimeClosed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	runContext, cancel := context.WithCancel(ctx)
	r.runCancel = cancel
	r.state = runtimeRunning
	r.mu.Unlock()

	type serviceResult struct {
		id        string
		startedAt time.Time
		err       error
	}
	results := make(chan serviceResult, max(1, len(r.services)))
	var workers sync.WaitGroup
	for _, configuredService := range r.services {
		workers.Add(1)
		go func(service namedService) {
			defer workers.Done()
			startedAt := time.Now().UTC()
			results <- serviceResult{
				id:        service.id,
				startedAt: startedAt,
				err:       r.runManagedService(runContext, service),
			}
		}(configuredService)
	}

	var runErr error
	gracefulDrain := false
	if len(r.services) == 0 {
		select {
		case <-runContext.Done():
		case <-r.drain:
			gracefulDrain = true
		}
	} else {
		select {
		case <-runContext.Done():
		case <-r.drain:
			gracefulDrain = true
		case result := <-results:
			r.mu.Lock()
			draining := r.state == runtimeDraining
			r.mu.Unlock()
			switch {
			case draining:
				gracefulDrain = true
				if result.err != nil && !isExpectedContextDone(runContext, result.err) {
					runErr = fmt.Errorf("messenger: service %s during drain: %w", result.id, result.err)
				}
			case result.err != nil && !isExpectedContextDone(runContext, result.err):
				runErr = fmt.Errorf("messenger: service %s: %w", result.id, result.err)
			case runContext.Err() == nil:
				runErr = fmt.Errorf("messenger: service %s stopped unexpectedly", result.id)
			}
			if runErr != nil {
				r.reportServiceFailure(ctx, result.id, result.startedAt, runErr)
			}
		}
	}

	drainErr := r.beginDrain(ctx)
	if drainErr != nil {
		runErr = errors.Join(runErr, drainErr)
	}
	if !gracefulDrain || drainErr != nil {
		cancel()
	}
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()

	shutdownContext, shutdownCancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		r.shutdownTimeout,
	)
	defer shutdownCancel()

	select {
	case <-workersDone:
	case <-shutdownContext.Done():
		cancel()
		runErr = errors.Join(runErr, fmt.Errorf(
			"messenger: services did not stop within shutdown timeout: %w",
			shutdownContext.Err(),
		))
	}
	for len(results) > 0 {
		result := <-results
		if result.err != nil && !isExpectedContextDone(runContext, result.err) {
			prefix := "messenger: service %s: %w"
			if gracefulDrain {
				prefix = "messenger: service %s during drain: %w"
			}
			runErr = errors.Join(runErr,
				fmt.Errorf(prefix, result.id, result.err))
		}
	}
	shutdownErr := r.shutdownServices(shutdownContext)
	cancel()
	combinedErr := errors.Join(runErr, shutdownErr)
	r.mu.Lock()
	r.shutdownErr = combinedErr
	r.mu.Unlock()
	r.markClosed()
	return combinedErr
}

func isExpectedContextDone(runContext context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if ctxErr := runContext.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return true
	}
	return false
}

func (r *Runtime) runManagedService(ctx context.Context, service namedService) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = ReportHandlerPanic(
				ctx,
				r.panicReporter,
				"service."+service.id,
				recovered,
				debug.Stack(),
			)
		}
	}()
	return service.service.Run(ctx)
}

func (r *Runtime) reportServiceFailure(ctx context.Context, serviceID string, startedAt time.Time, err error) {
	if r.observer != nil {
		observe(ctx, r.observer, Observation{
			Operation: OperationService,
			ServiceID: serviceID,
			StartedAt: startedAt,
			Duration:  time.Since(startedAt),
			Err:       err,
		})
	}
	safeLog(ctx, r.logger, LogError, "messenger service stopped",
		LogAttr{Key: logAttrServiceIDKey, Value: serviceID},
		LogAttr{Key: logAttrErrorKey, Value: SanitizeError(DefaultFailureSanitizer(), err)},
	)
}

// Readiness checks runtime admission state and each service's lightweight
// readiness contract. Expensive topology validation belongs in DeepHealth.
func (r *Runtime) Readiness(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	if state != runtimeRunning {
		return ErrRuntimeNotRunning
	}
	return r.checkServices(ctx, "readiness", func(ctx context.Context, service namedService) error {
		return service.service.Readiness(ctx)
	})
}

// Liveness checks that Runtime has not terminated and invokes optional service
// liveness checks without requiring readiness or topology access.
func (r *Runtime) Liveness(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	switch state {
	case runtimeRunning, runtimeDraining:
	case runtimeClosed:
		return ErrRuntimeClosed
	case runtimeNew:
		return ErrRuntimeNotRunning
	default:
		return ErrRuntimeNotRunning
	}
	return r.checkServices(ctx, "liveness", func(ctx context.Context, service namedService) error {
		checker, ok := service.service.(LivenessChecker)
		if !ok {
			return nil
		}
		return checker.Liveness(ctx)
	})
}

// DeepHealth performs explicit, potentially expensive service health and
// topology checks. It is intended for diagnostics or a low-frequency probe.
func (r *Runtime) DeepHealth(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	if state != runtimeRunning {
		return ErrRuntimeNotRunning
	}
	return r.checkServices(ctx, "deep health", func(ctx context.Context, service namedService) error {
		if checker, ok := service.service.(DeepHealthChecker); ok {
			return checker.DeepHealth(ctx)
		}
		return service.service.Readiness(ctx)
	})
}

func (r *Runtime) checkServices(
	ctx context.Context,
	operation string,
	check func(context.Context, namedService) error,
) error {
	checkErrors := make([]error, len(r.services))
	var workers sync.WaitGroup
	for index, configuredService := range r.services {
		workers.Add(1)
		go func(index int, service namedService) {
			defer workers.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					checkErrors[index] = ReportHandlerPanic(
						ctx,
						r.panicReporter,
						"service."+service.id+"."+operation,
						recovered,
						debug.Stack(),
					)
				}
			}()
			if err := check(ctx, service); err != nil {
				checkErrors[index] = fmt.Errorf("messenger: service %s %s: %w", service.id, operation, err)
			}
		}(index, configuredService)
	}
	workers.Wait()
	return errors.Join(checkErrors...)
}

// BeginDrain marks the runtime unready and asks every service to stop admission.
func (r *Runtime) BeginDrain() {
	_ = r.beginDrain(context.Background())
}

func (r *Runtime) beginDrain(ctx context.Context) error {
	r.drainOnce.Do(func() {
		r.mu.Lock()
		if r.state != runtimeClosed {
			r.state = runtimeDraining
		}
		r.mu.Unlock()
		drainErrors := make([]error, len(r.services))
		for index, configuredService := range r.services {
			drainErrors[index] = r.beginDrainService(ctx, configuredService)
		}
		drainErr := errors.Join(drainErrors...)
		r.mu.Lock()
		r.drainErr = drainErr
		cancel := r.runCancel
		r.mu.Unlock()
		if drainErr != nil && cancel != nil {
			cancel()
		}
		close(r.drain)
	})
	r.mu.Lock()
	err := r.drainErr
	r.mu.Unlock()
	return err
}

func (r *Runtime) beginDrainService(ctx context.Context, service namedService) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = ReportHandlerPanic(
				ctx,
				r.panicReporter,
				"service."+service.id+".begin_drain",
				recovered,
				debug.Stack(),
			)
			safeLog(ctx, r.logger, LogError, "messenger service drain panicked",
				LogAttr{Key: logAttrServiceIDKey, Value: service.id},
				LogAttr{Key: logAttrErrorKey, Value: err},
			)
		}
	}()
	service.service.BeginDrain()
	return nil
}

// Shutdown drains all services and waits for a concurrent Run to finish.
// Coordinate it outside handlers executing on this Runtime.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	r.mu.Lock()
	if r.state == runtimeClosed {
		err := r.shutdownErr
		r.mu.Unlock()
		return err
	}
	cancel := r.runCancel
	waitForOwnedShutdown := cancel == nil && r.shutdownStarted
	if cancel == nil && !waitForOwnedShutdown {
		// Claim shutdown before releasing the mutex so Run cannot take lifecycle
		// ownership in the gap before BeginDrain.
		r.shutdownStarted = true
		r.state = runtimeDraining
	}
	r.mu.Unlock()
	if waitForOwnedShutdown {
		select {
		case <-r.done:
			r.mu.Lock()
			err := r.shutdownErr
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	drainErr := r.beginDrain(ctx)
	if cancel != nil {
		select {
		case <-r.done:
			r.mu.Lock()
			err := r.shutdownErr
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		}
	}
	err := errors.Join(drainErr, r.shutdownServices(ctx))
	r.mu.Lock()
	r.shutdownErr = err
	r.mu.Unlock()
	r.markClosed()
	return err
}

type shutdownServiceResult struct {
	index int
	err   error
}

func (r *Runtime) shutdownServices(ctx context.Context) error {
	results := make(chan shutdownServiceResult, len(r.services))
	for index, configuredService := range r.services {
		go func(index int, service namedService) {
			var err error
			defer func() {
				if recovered := recover(); recovered != nil {
					err = ReportHandlerPanic(
						ctx,
						r.panicReporter,
						"service."+service.id+".shutdown",
						recovered,
						debug.Stack(),
					)
				}
				results <- shutdownServiceResult{
					index: index,
					err:   err,
				}
			}()
			if shutdownErr := service.service.Shutdown(ctx); shutdownErr != nil {
				err = fmt.Errorf("messenger: shutdown service %s: %w", service.id, shutdownErr)
			}
		}(index, configuredService)
	}

	shutdownErrors := make([]error, len(r.services))
	remaining := len(r.services)
	for remaining > 0 {
		select {
		case res := <-results:
			shutdownErrors[res.index] = res.err
			remaining--
		case <-ctx.Done():
			return errors.Join(
				errors.Join(shutdownErrors...),
				fmt.Errorf(
					"messenger: service shutdown exceeded deadline: %w",
					ctx.Err(),
				),
			)
		}
	}
	return errors.Join(shutdownErrors...)
}

func (r *Runtime) markClosed() {
	r.mu.Lock()
	r.state = runtimeClosed
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.done) })
}
