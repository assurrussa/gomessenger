package messenger_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const testRuntimeServiceID = "worker"

type controlledService struct {
	started       chan struct{}
	finish        chan struct{}
	drainCalled   chan struct{}
	shutdownErr   error
	runErr        error
	cancelled     atomic.Bool
	readyErr      error
	livenessErr   error
	deepHealthErr error
	drainOnce     sync.Once
	shutdownCall  atomic.Int32
}

type blockingShutdownService struct {
	started chan<- string
	release <-chan struct{}
	id      string
	err     error
}

type singleOwnerShutdownService struct {
	drainStarted    chan struct{}
	releaseDrain    chan struct{}
	shutdownStarted chan struct{}
	releaseShutdown chan struct{}
	shutdownCalls   atomic.Int32
}

func (s *blockingShutdownService) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *blockingShutdownService) Readiness(context.Context) error { return nil }

func (s *blockingShutdownService) BeginDrain() {}

func (s *blockingShutdownService) Shutdown(context.Context) error {
	s.started <- s.id
	<-s.release
	return s.err
}

func (*singleOwnerShutdownService) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*singleOwnerShutdownService) Readiness(context.Context) error { return nil }

func (s *singleOwnerShutdownService) BeginDrain() {
	close(s.drainStarted)
	<-s.releaseDrain
}

func (s *singleOwnerShutdownService) Shutdown(context.Context) error {
	s.shutdownCalls.Add(1)
	s.shutdownStarted <- struct{}{}
	<-s.releaseShutdown
	return nil
}

func newControlledService() *controlledService {
	return &controlledService{
		started: make(chan struct{}), finish: make(chan struct{}), drainCalled: make(chan struct{}),
	}
}

func (s *controlledService) Run(ctx context.Context) error {
	close(s.started)
	select {
	case <-s.finish:
		return s.runErr
	case <-ctx.Done():
		s.cancelled.Store(true)
		return ctx.Err()
	}
}

func (s *controlledService) Readiness(context.Context) error { return s.readyErr }

func (s *controlledService) Liveness(context.Context) error { return s.livenessErr }

func (s *controlledService) DeepHealth(context.Context) error { return s.deepHealthErr }

func (s *controlledService) BeginDrain() {
	s.drainOnce.Do(func() { close(s.drainCalled) })
}

func (s *controlledService) Shutdown(context.Context) error {
	s.shutdownCall.Add(1)
	return s.shutdownErr
}

func TestRuntimeShutdownDrainsBeforeCancellingActiveService(t *testing.T) {
	service := newControlledService()
	runtime := runtimeWithServices(t, map[string]messenger.Service{testRuntimeServiceID: service})
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	<-service.started

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		shutdownDone <- runtime.Shutdown(ctx)
	}()
	<-service.drainCalled
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before accepted work finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if service.cancelled.Load() {
		t.Fatal("active service context was cancelled during graceful drain")
	}
	close(service.finish)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if service.cancelled.Load() || service.shutdownCall.Load() != 1 {
		t.Fatalf("cancelled=%v shutdown calls=%d", service.cancelled.Load(), service.shutdownCall.Load())
	}
}

func TestRuntimeShutdownDeadlineForceCancelsService(t *testing.T) {
	service := newControlledService()
	runtime := runtimeWithServices(t, map[string]messenger.Service{testRuntimeServiceID: service})
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	<-service.started
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := runtime.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if !service.cancelled.Load() {
		t.Fatal("deadline did not force cancellation")
	}
}

func TestRuntimeConcurrentPreRunShutdownHasSingleOwner(t *testing.T) {
	service := &singleOwnerShutdownService{
		drainStarted:    make(chan struct{}),
		releaseDrain:    make(chan struct{}),
		shutdownStarted: make(chan struct{}, 2),
		releaseShutdown: make(chan struct{}),
	}
	runtime := runtimeWithServices(t, map[string]messenger.Service{testRuntimeServiceID: service})
	firstDone := make(chan error, 1)
	go func() { firstDone <- runtime.Shutdown(t.Context()) }()
	<-service.drainStarted

	waiterContext, cancelWaiter := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancelWaiter()
	secondDone := make(chan error, 1)
	go func() { secondDone <- runtime.Shutdown(waiterContext) }()
	select {
	case secondErr := <-secondDone:
		if !errors.Is(secondErr, context.DeadlineExceeded) {
			t.Fatalf("concurrent shutdown waiter error = %v", secondErr)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent shutdown waiter blocked in BeginDrain")
	}

	close(service.releaseDrain)
	<-service.shutdownStarted
	close(service.releaseShutdown)
	firstErr := <-firstDone
	if firstErr != nil || service.shutdownCalls.Load() != 1 {
		t.Fatalf("shutdown error=%v service calls=%d", firstErr, service.shutdownCalls.Load())
	}
}

func TestRuntimeReportsUnexpectedServiceStopAndReadinessErrors(t *testing.T) {
	failed := newControlledService()
	failure := errors.New("consumer failed")
	failed.runErr = failure
	failed.readyErr = errors.New("not ready")
	peer := newControlledService()
	observer := &recordingObserver{}
	logger := &testLogger{}
	runtime := runtimeWithServices(
		t,
		map[string]messenger.Service{"failed": failed, "peer": peer},
		messenger.WithObserver(observer),
		messenger.WithLogger(logger),
	)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	<-failed.started
	<-peer.started
	if err := runtime.Readiness(t.Context()); err == nil {
		t.Fatal("readiness error was not aggregated")
	}
	close(failed.finish)
	if err := <-runDone; !errors.Is(err, failure) {
		t.Fatalf("run error = %v", err)
	}
	if !peer.cancelled.Load() {
		t.Fatal("peer was not cancelled after unexpected service stop")
	}
	if len(observer.observations) != 1 || observer.observations[0].Operation != messenger.OperationService ||
		observer.observations[0].ServiceID != "failed" || !errors.Is(observer.observations[0].Err, failure) {
		t.Fatalf("service observations = %#v", observer.observations)
	}
	logs := logger.snapshot()
	if len(logs) != 1 || logs[0].text != "messenger service stopped" || logs[0].level != messenger.LogError {
		t.Fatalf("service logs = %#v", logs)
	}
	if err := runtime.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("closed readiness = %v", err)
	}
}

func TestRuntimeSeparatesReadinessLivenessAndDeepHealth(t *testing.T) {
	service := newControlledService()
	service.readyErr = errors.New("not accepting work")
	service.livenessErr = errors.New("worker loop stopped")
	service.deepHealthErr = errors.New("topology drift")
	runtime := runtimeWithServices(t, map[string]messenger.Service{testRuntimeServiceID: service})
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	<-service.started

	if err := runtime.Readiness(t.Context()); !errors.Is(err, service.readyErr) {
		t.Fatalf("readiness = %v", err)
	}
	if err := runtime.Liveness(t.Context()); !errors.Is(err, service.livenessErr) {
		t.Fatalf("liveness = %v", err)
	}
	if err := runtime.DeepHealth(t.Context()); !errors.Is(err, service.deepHealthErr) {
		t.Fatalf("deep health = %v", err)
	}

	runtime.BeginDrain()
	close(service.finish)
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

type panickingRunService struct{}

func (panickingRunService) Run(context.Context) error { panic("sensitive service panic") }

func (panickingRunService) Readiness(context.Context) error { return nil }

func (panickingRunService) BeginDrain() {}

func (panickingRunService) Shutdown(context.Context) error { return nil }

type panickingDrainService struct {
	started       chan struct{}
	cancelled     chan struct{}
	shutdownCalls atomic.Int32
}

func (s *panickingDrainService) Run(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	close(s.cancelled)
	return ctx.Err()
}

func (*panickingDrainService) Readiness(context.Context) error { return nil }

func (*panickingDrainService) BeginDrain() { panic("sensitive drain panic") }

func (s *panickingDrainService) Shutdown(context.Context) error {
	s.shutdownCalls.Add(1)
	return nil
}

func assertRuntimeDrainPanicError(t *testing.T, err error) {
	t.Helper()
	var panicErr messenger.HandlerPanicError
	if !errors.As(err, &panicErr) ||
		panicErr.HandlerPanicID() != "service."+testRuntimeServiceID+".begin_drain" ||
		strings.Contains(err.Error(), "sensitive drain panic") {
		t.Fatalf("runtime drain panic error = %#v, %v", panicErr, err)
	}
}

func TestRuntimeReportsServicePanicsWithoutExposingDetails(t *testing.T) {
	var report messenger.PanicReport
	runtime := runtimeWithServices(
		t,
		map[string]messenger.Service{testRuntimeServiceID: panickingRunService{}},
		messenger.WithPanicReporter(messenger.PanicReporterFunc(func(
			_ context.Context,
			received messenger.PanicReport,
		) {
			report = received
		})),
	)
	err := runtime.Run(t.Context())
	var panicErr messenger.HandlerPanicError
	if !errors.As(err, &panicErr) || panicErr.HandlerPanicID() != "service."+testRuntimeServiceID ||
		strings.Contains(err.Error(), "sensitive service panic") {
		t.Fatalf("runtime panic error = %#v, %v", panicErr, err)
	}
	if report.HandlerID != "service."+testRuntimeServiceID ||
		report.Value != "sensitive service panic" || len(report.Stack) == 0 {
		t.Fatalf("runtime panic report = %#v", report)
	}
}

func TestRuntimeBeginDrainPanicForceCancelsRun(t *testing.T) {
	service := &panickingDrainService{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	reports := make(chan messenger.PanicReport, 1)
	runtime := runtimeWithServices(
		t,
		map[string]messenger.Service{testRuntimeServiceID: service},
		messenger.WithPanicReporter(messenger.PanicReporterFunc(func(
			_ context.Context,
			report messenger.PanicReport,
		) {
			reports <- report
		})),
	)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	select {
	case <-service.started:
	case <-time.After(time.Second):
		t.Fatal("service did not start")
	}

	runtime.BeginDrain()
	select {
	case <-service.cancelled:
	case <-time.After(time.Second):
		t.Fatal("drain panic did not cancel the service run context")
	}
	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(time.Second):
		t.Fatal("runtime remained blocked after the drain panic")
	}
	assertRuntimeDrainPanicError(t, runErr)
	select {
	case report := <-reports:
		if report.HandlerID != "service."+testRuntimeServiceID+".begin_drain" ||
			report.Value != "sensitive drain panic" || len(report.Stack) == 0 {
			t.Fatalf("runtime drain panic report = %#v", report)
		}
	default:
		t.Fatal("drain panic was not reported")
	}
	if calls := service.shutdownCalls.Load(); calls != 1 {
		t.Fatalf("service shutdown calls = %d, want 1", calls)
	}
}

func TestRuntimeShutdownBeforeRunRetainsBeginDrainPanic(t *testing.T) {
	service := &panickingDrainService{}
	runtime := runtimeWithServices(
		t,
		map[string]messenger.Service{testRuntimeServiceID: service},
	)

	assertRuntimeDrainPanicError(t, runtime.Shutdown(t.Context()))
	assertRuntimeDrainPanicError(t, runtime.Shutdown(t.Context()))
	if calls := service.shutdownCalls.Load(); calls != 1 {
		t.Fatalf("service shutdown calls = %d, want 1", calls)
	}
	if err := runtime.Run(t.Context()); !errors.Is(err, messenger.ErrRuntimeClosed) {
		t.Fatalf("run after failed pre-run shutdown = %v, want ErrRuntimeClosed", err)
	}
}

func TestRuntimeWithoutServicesCanBeShutdownAndRejectsSecondRun(t *testing.T) {
	runtime := runtimeWithServices(t, nil)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	deadline := time.Now().Add(time.Second)
	for errors.Is(runtime.Readiness(t.Context()), messenger.ErrRuntimeNotRunning) {
		if time.Now().After(deadline) {
			t.Fatal("runtime did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := runtime.Run(t.Context()); !errors.Is(err, messenger.ErrRuntimeRunning) {
		t.Fatalf("second run = %v", err)
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := runtime.Run(t.Context()); !errors.Is(err, messenger.ErrRuntimeClosed) {
		t.Fatalf("closed run = %v", err)
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if err := runtime.Shutdown(nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil shutdown = %v", err)
	}
}

func TestRuntimeShutsServicesDownConcurrentlyAndJoinsErrorsByServiceID(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	alphaErr := errors.New("alpha failed")
	zuluErr := errors.New("zulu failed")
	runtime := runtimeWithServices(t, map[string]messenger.Service{
		"zulu":  &blockingShutdownService{started: started, release: release, id: "zulu", err: zuluErr},
		"alpha": &blockingShutdownService{started: started, release: release, id: "alpha", err: alphaErr},
	})
	done := make(chan error, 1)
	go func() { done <- runtime.Shutdown(t.Context()) }()

	seen := map[string]bool{}
	for range 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("shutdown services did not start concurrently")
		}
	}
	close(release)
	err := <-done
	if !errors.Is(err, alphaErr) || !errors.Is(err, zuluErr) || !seen["alpha"] || !seen["zulu"] {
		t.Fatalf("shutdown result = %v, started=%v", err, seen)
	}
	text := err.Error()
	if strings.Index(text, "alpha") > strings.Index(text, "zulu") {
		t.Fatalf("shutdown errors are not deterministic: %q", text)
	}
}

func runtimeWithServices(
	t *testing.T,
	services map[string]messenger.Service,
	options ...messenger.Option,
) *messenger.Runtime {
	t.Helper()
	options = append([]messenger.Option{messenger.WithSource("urn:service:test")}, options...)
	builder := messenger.NewBuilder(options...)
	for id, service := range services {
		builder.Use(id, service)
	}
	_, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return runtime
}
