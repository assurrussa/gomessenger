//nolint:testpackage,gosec // Tests cover package-local wiring with an isolated fake DSN.
package demo

import (
	"testing"
)

func TestWithApplicationName(t *testing.T) {
	t.Parallel()
	got, err := withApplicationName(
		"postgres://user:pass@db:5432/name?sslmode=disable&application_name=shared",
		producerApplicationName,
	)
	if err != nil {
		t.Fatalf("withApplicationName() error = %v", err)
	}
	want := "postgres://user:pass@db:5432/name?application_name=gomessenger-outbox-producer&sslmode=disable"
	if got != want {
		t.Fatalf("withApplicationName() = %q, want %q", got, want)
	}
}

func TestNormalizeConfigRejectsInvalidOutboxPools(t *testing.T) {
	t.Parallel()
	config := CorrectnessConfig(nil)
	config.OutboxProducerMaxConns = 0
	if err := normalizeConfig(&config); err == nil {
		t.Fatal("normalizeConfig() unexpectedly accepted a zero producer pool")
	}

	config = CorrectnessConfig(nil)
	config.OutboxRelayMaxConns = 0
	if err := normalizeConfig(&config); err == nil {
		t.Fatal("normalizeConfig() unexpectedly accepted a zero relay pool")
	}
}
