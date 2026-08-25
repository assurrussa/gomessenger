package main

import (
	"errors"
	"strings"
	"testing"
)

func TestJoinRunError(t *testing.T) {
	t.Parallel()

	primaryErr := errors.New("scenario failed")
	cleanupErr := errors.New("drain failed")
	runErr := primaryErr

	joinRunError(&runErr, "shutdown consumer", cleanupErr)

	if !errors.Is(runErr, primaryErr) {
		t.Fatalf("joined error does not preserve primary failure: %v", runErr)
	}
	if !errors.Is(runErr, cleanupErr) {
		t.Fatalf("joined error does not preserve cleanup failure: %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "shutdown consumer") {
		t.Fatalf("joined error does not identify cleanup operation: %v", runErr)
	}
}

func TestJoinRunErrorPromotesCleanupFailure(t *testing.T) {
	t.Parallel()

	cleanupErr := errors.New("close failed")
	var runErr error

	joinRunError(&runErr, "close outbox runtime", cleanupErr)

	if !errors.Is(runErr, cleanupErr) {
		t.Fatalf("cleanup failure was not returned: %v", runErr)
	}
}
