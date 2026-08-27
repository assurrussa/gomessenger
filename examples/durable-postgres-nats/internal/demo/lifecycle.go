package demo

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type runner struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

func startRunner(parent context.Context, run func(context.Context) error) *runner {
	ctx, cancel := context.WithCancel(parent)
	result := &runner{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(result.done)
		err := run(ctx)
		result.mu.Lock()
		result.err = err
		result.mu.Unlock()
	}()
	return result
}

func startOwnedRunner(startupContext context.Context, run func(context.Context) error) *runner {
	return startRunner(context.WithoutCancel(startupContext), run)
}

func (r *runner) result() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *runner) stop(parent context.Context) error {
	r.cancel()
	select {
	case <-r.done:
		err := r.result()
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-parent.Done():
		return parent.Err()
	}
}

func waitReady(
	ctx context.Context,
	name string,
	readiness func(context.Context) error,
	runtimeRunner *runner,
) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := readiness(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s readiness: %w", name, ctx.Err())
		case <-runtimeRunner.done:
			runErr := runtimeRunner.result()
			if runErr == nil {
				return fmt.Errorf("%s stopped before readiness", name)
			}
			return fmt.Errorf("%s stopped before readiness: %w", name, runErr)
		case <-ticker.C:
		}
	}
}

func waitFor(ctx context.Context, description string, condition func() (bool, error)) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if err != nil {
			return fmt.Errorf("wait for %s: %w", description, err)
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s: %w", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func joinError(target *error, operation string, err error) {
	if err == nil {
		return
	}
	*target = errors.Join(*target, fmt.Errorf("%s: %w", operation, err))
}

func randomID() (string, error) {
	var value [8]byte
	if _, err := crand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate demo run ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

// EnvOr returns the environment value or a fallback when it is empty.
func EnvOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
