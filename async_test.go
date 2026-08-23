package messenger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestLocalAsyncRoute_AcceptsAndDrains(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	route, err := messenger.NewLocalAsyncRoute("local.media", messenger.LocalAsyncConfig{Capacity: 2, Workers: 1})
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	handled := make(chan processPayload, 1)
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.HandleCommand(command, "media-processor", func(_ context.Context, message messenger.Message[processPayload]) error {
		handled <- message.Payload
		return nil
	})
	builder.RouteCommand(command, route)
	m, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runContext) }()
	deadline := time.Now().Add(time.Second)
	for {
		if err := runtime.Readiness(t.Context()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	receipt, err := m.Send(t.Context(), command, processPayload{JobID: 17})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if receipt.State != messenger.ReceiptAccepted {
		t.Fatalf("state = %q", receipt.State)
	}
	select {
	case payload := <-handled:
		if payload.JobID != 17 {
			t.Fatalf("payload = %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not run")
	}
	runtime.BeginDrain()
	if _, err := m.Send(t.Context(), command, processPayload{JobID: 18}); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("send after BeginDrain = %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not drain")
	}
}

func TestLocalAsyncRoute_AcceptedJobOutlivesAdmissionContext(t *testing.T) {
	command := messenger.MustCommand("async.admission", 1, messenger.JSON[processPayload]())
	route, err := messenger.NewLocalAsyncRoute(
		"local.admission", messenger.LocalAsyncConfig{Capacity: 2, Workers: 1},
	)
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	handled := make(chan int64, 2)
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.HandleCommand(command, "async-admission", func(ctx context.Context, message messenger.Message[processPayload]) error {
		if message.Payload.JobID == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		handled <- message.Payload.JobID
		return nil
	})
	builder.RouteCommand(command, route)
	m, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runContext) }()
	waitRuntimeReady(t, runtime)

	if _, err := m.Send(t.Context(), command, processPayload{JobID: 1}); err != nil {
		t.Fatalf("send blocker: %v", err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not start")
	}
	admissionContext, cancelAdmission := context.WithCancel(t.Context())
	if _, err := m.Send(admissionContext, command, processPayload{JobID: 2}); err != nil {
		t.Fatalf("send queued job: %v", err)
	}
	cancelAdmission()
	close(releaseFirst)

	for range 2 {
		select {
		case <-handled:
		case <-time.After(time.Second):
			t.Fatal("accepted job was skipped after admission context cancellation")
		}
	}
	runtime.BeginDrain()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not drain")
	}
}

func TestLocalAsyncRoute_RuntimeDeadlineCancelsHandler(t *testing.T) {
	command := messenger.MustCommand("async.force", 1, messenger.JSON[processPayload]())
	route, err := messenger.NewLocalAsyncRoute(
		"local.force", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
	)
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	started := make(chan struct{})
	handlerDone := make(chan error, 1)
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.HandleCommand(command, "async-force", func(ctx context.Context, _ messenger.Message[processPayload]) error {
		close(started)
		<-ctx.Done()
		handlerDone <- ctx.Err()
		return ctx.Err()
	})
	builder.RouteCommand(command, route)
	m, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runContext) }()
	waitRuntimeReady(t, runtime)
	if _, err := m.Send(t.Context(), command, processPayload{JobID: 1}); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancelShutdown()
	if err := runtime.Shutdown(shutdownContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown = %v, want deadline exceeded", err)
	}
	select {
	case err := <-handlerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime deadline did not cancel the active handler")
	}
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after forced cancellation")
	}
}

func TestLocalRoutesValidateConfigurationAndLifecycle(t *testing.T) {
	if _, err := messenger.NewLocalAsyncRoute(
		"bad route", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
	); !errors.Is(err, messenger.ErrRouteNotFound) {
		t.Fatalf("invalid name = %v", err)
	}
	if _, err := messenger.NewLocalAsyncRoute("local.invalid", messenger.LocalAsyncConfig{}); err == nil {
		t.Fatal("invalid queue config accepted")
	}
	route, err := messenger.NewLocalAsyncRoute("local.test", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if _, err := route.Deliver(t.Context(), nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil delivery = %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if _, err := route.Deliver(nil, nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil context = %v", err)
	}
	if err := route.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("initial readiness = %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if err := route.Run(nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil run = %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if err := route.Shutdown(nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil shutdown = %v", err)
	}
	if _, err := messenger.NewLocalSyncRoute().Deliver(t.Context(), nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("sync nil delivery = %v", err)
	}
	preDrained, err := messenger.NewLocalAsyncRoute(
		"local.pre-drained", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
	)
	if err != nil {
		t.Fatalf("pre-drained route: %v", err)
	}
	preDrained.BeginDrain()
	if err := preDrained.Run(t.Context()); err != nil {
		t.Fatalf("run pre-drained route: %v", err)
	}
	if err := preDrained.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown pre-drained route: %v", err)
	}
	if err := preDrained.Run(t.Context()); !errors.Is(err, messenger.ErrRuntimeClosed) {
		t.Fatalf("second pre-drained run = %v", err)
	}
}

func waitRuntimeReady(t *testing.T, runtime *messenger.Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if err := runtime.Readiness(t.Context()); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
}
