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
