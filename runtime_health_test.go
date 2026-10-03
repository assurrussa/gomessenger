package messenger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type blockedHealthService struct {
	*controlledService
	checking chan struct{}
	release  chan struct{}
	checkErr error
}

func (s *blockedHealthService) check(ctx context.Context) error {
	close(s.checking)
	select {
	case <-s.release:
		return s.checkErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockedHealthService) Readiness(ctx context.Context) error  { return s.check(ctx) }
func (s *blockedHealthService) Liveness(ctx context.Context) error   { return s.check(ctx) }
func (s *blockedHealthService) DeepHealth(ctx context.Context) error { return s.check(ctx) }

func TestRuntimeHealthRechecksStateAfterServiceProbe(t *testing.T) {
	probeErr := errors.New("service probe failed")
	for _, test := range []struct {
		name     string
		probe    func(*messenger.Runtime, context.Context) error
		close    bool
		want     error
		checkErr error
	}{
		{"readiness during drain", (*messenger.Runtime).Readiness, false, messenger.ErrRuntimeNotRunning, nil},
		{"deep health during drain", (*messenger.Runtime).DeepHealth, false, messenger.ErrRuntimeNotRunning, nil},
		{"liveness during drain", (*messenger.Runtime).Liveness, false, nil, nil},
		{"readiness after close", (*messenger.Runtime).Readiness, true, messenger.ErrRuntimeNotRunning, nil},
		{"deep health after close", (*messenger.Runtime).DeepHealth, true, messenger.ErrRuntimeNotRunning, nil},
		{"liveness after close", (*messenger.Runtime).Liveness, true, messenger.ErrRuntimeClosed, nil},
		{
			name: "readiness retains service failure", probe: (*messenger.Runtime).Readiness,
			want: messenger.ErrRuntimeNotRunning, checkErr: probeErr,
		},
		{
			name: "deep health retains service failure", probe: (*messenger.Runtime).DeepHealth,
			want: messenger.ErrRuntimeNotRunning, checkErr: probeErr,
		},
		{
			name: "liveness retains service failure", probe: (*messenger.Runtime).Liveness, close: true,
			want: messenger.ErrRuntimeClosed, checkErr: probeErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			service := &blockedHealthService{
				controlledService: newControlledService(),
				checking:          make(chan struct{}),
				release:           make(chan struct{}),
				checkErr:          test.checkErr,
			}
			runtime := runtimeWithServices(t, map[string]messenger.Service{testRuntimeServiceID: service})
			runDone := make(chan error, 1)
			go func() { runDone <- runtime.Run(ctx) }()
			awaitHealthResult(t, ctx, service.started)
			probeDone := make(chan error, 1)
			go func() { probeDone <- test.probe(runtime, ctx) }()
			awaitHealthResult(t, ctx, service.checking)
			runtime.BeginDrain()
			if test.close {
				close(service.finish)
				if err := awaitHealthResult(t, ctx, runDone); err != nil {
					t.Fatalf("run: %v", err)
				}
			}
			close(service.release)
			probeResult := awaitHealthResult(t, ctx, probeDone)
			if !errors.Is(probeResult, test.want) {
				t.Errorf("probe after lifecycle transition = %v, want %v", probeResult, test.want)
			}
			if test.checkErr != nil && !errors.Is(probeResult, test.checkErr) {
				t.Errorf("probe lost service error: %v", probeResult)
			}
			if !test.close {
				close(service.finish)
				if err := awaitHealthResult(t, ctx, runDone); err != nil {
					t.Fatalf("run: %v", err)
				}
			}
		})
	}
}

func awaitHealthResult[T any](t *testing.T, ctx context.Context, results <-chan T) T {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-ctx.Done():
		t.Fatalf("timed out waiting for lifecycle transition: %v", ctx.Err())
		var zero T
		return zero
	}
}
