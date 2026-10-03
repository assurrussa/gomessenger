package messenger_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type goexitRunService struct {
	exited        chan struct{}
	shutdownCalls atomic.Int32
}

func (s *goexitRunService) Run(context.Context) error {
	defer close(s.exited)
	runtime.Goexit()
	return nil
}

func (*goexitRunService) Readiness(context.Context) error { return nil }
func (*goexitRunService) BeginDrain()                     {}
func (s *goexitRunService) Shutdown(context.Context) error {
	s.shutdownCalls.Add(1)
	return nil
}

func TestRuntimeReportsServiceGoexitAndCancelsPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	service := &goexitRunService{exited: make(chan struct{})}
	peer := newControlledService()
	observer := &recordingObserver{}
	managed := runtimeWithServices(t, map[string]messenger.Service{
		"exiting": service,
		"peer":    peer,
	}, messenger.WithObserver(observer))
	runDone := make(chan error, 1)
	go func() { runDone <- managed.Run(ctx) }()
	awaitHealthResult(t, ctx, service.exited)
	awaitHealthResult(t, ctx, peer.started)

	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(time.Second):
		cancel()
		// Join Run on the unfixed implementation without hiding a stuck cleanup.
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Fatal("runtime did not stop after cleanup cancellation")
		}
		t.Fatal("runtime remained running after a service exited without returning")
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "exiting") {
		t.Fatalf("service Goexit result = %v", runErr)
	}
	if len(observer.observations) != 1 || observer.observations[0].Operation != messenger.OperationService ||
		observer.observations[0].ServiceID != "exiting" || observer.observations[0].Err == nil {
		t.Fatalf("service observations = %#v", observer.observations)
	}
	var panicErr messenger.HandlerPanicError
	if errors.As(runErr, &panicErr) {
		t.Fatalf("Goexit was incorrectly reported as a panic: %v", runErr)
	}
	if !peer.cancelled.Load() || service.shutdownCalls.Load() != 1 {
		t.Fatalf("peer cancelled=%v, shutdown calls=%d", peer.cancelled.Load(), service.shutdownCalls.Load())
	}
	if err := managed.Readiness(ctx); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("readiness after service Goexit = %v", err)
	}
	if err := managed.Shutdown(ctx); !errors.Is(err, runErr) {
		t.Fatalf("shutdown lost Run error: %v", err)
	}
}
