//nolint:testpackage // Tests intentionally exercise the package-local lifecycle join helper.
package demo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOwnedRunnerOutlivesStartupContext(t *testing.T) {
	t.Parallel()
	startupContext, cancelStartup := context.WithCancel(context.Background())
	started := make(chan struct{})
	runtimeRunner := startOwnedRunner(startupContext, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	cancelStartup()
	select {
	case <-runtimeRunner.done:
		t.Fatal("owned runtime stopped when only its startup context was released")
	case <-time.After(25 * time.Millisecond):
	}
	stopContext, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	if err := runtimeRunner.stop(stopContext); err != nil {
		t.Fatalf("stop() error = %v", err)
	}
}

func TestRequiredRunnerFailureCancelsApplicationRuntime(t *testing.T) {
	t.Parallel()

	runtimeContext, cancelRuntime := context.WithCancelCause(context.Background())
	application := &Application{
		runtimeStopped: runtimeContext.Done(), cancelRuntime: cancelRuntime,
		runtimeCause: func() error { return context.Cause(runtimeContext) },
	}
	wantErr := errors.New("outbox stopped")
	release := make(chan struct{})
	runtimeRunner := startRunner(runtimeContext, func(context.Context) error {
		<-release
		return wantErr
	})
	application.superviseRunner("outbox", runtimeRunner)
	close(release)

	select {
	case <-application.runtimeDone():
	case <-time.After(time.Second):
		t.Fatal("required runner failure did not cancel the application runtime")
	}
	if !errors.Is(application.runtimeFailure(), wantErr) {
		t.Fatalf("runtime failure = %v, want %v", application.runtimeFailure(), wantErr)
	}
	if !application.draining.Load() {
		t.Fatal("required runner failure did not close admission")
	}
}

func TestWaitReadyFailsWhenPreviouslyReadyPeerStops(t *testing.T) {
	t.Parallel()

	runtimeContext, cancelRuntime := context.WithCancelCause(context.Background())
	application := &Application{
		runtimeStopped: runtimeContext.Done(), cancelRuntime: cancelRuntime,
		runtimeCause: func() error { return context.Cause(runtimeContext) },
	}
	wantErr := errors.New("outbox stopped")
	peerRunner := startRunner(runtimeContext, func(context.Context) error { return wantErr })
	application.superviseRunner("outbox", peerRunner)
	consumerRunner := startRunner(runtimeContext, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	waitContext, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	err := waitReady(
		waitContext,
		"consumer",
		func(context.Context) error { return errors.New("consumer starting") },
		consumerRunner,
		application.runtimeDone(),
		application.runtimeFailure,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("waitReady() error = %v, want peer failure %v", err, wantErr)
	}
}

func TestJoinError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		primaryErr error
	}{
		{name: "preserves primary", primaryErr: errors.New("scenario failed")},
		{name: "promotes cleanup", primaryErr: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cleanupErr := errors.New("drain failed")
			runErr := test.primaryErr
			joinError(&runErr, "shutdown consumer", cleanupErr)
			if test.primaryErr != nil && !errors.Is(runErr, test.primaryErr) {
				t.Fatalf("joined error does not preserve primary failure: %v", runErr)
			}
			if !errors.Is(runErr, cleanupErr) {
				t.Fatalf("joined error does not preserve cleanup failure: %v", runErr)
			}
			if !strings.Contains(runErr.Error(), "shutdown consumer") {
				t.Fatalf("joined error does not identify cleanup operation: %v", runErr)
			}
		})
	}
}
