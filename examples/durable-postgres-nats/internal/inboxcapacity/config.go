// Package inboxcapacity implements the PostgreSQL-only ProcessAttempt harness.
package inboxcapacity

import (
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The credentials belong only to the isolated local Compose example.
//
//nolint:gosec // This is an isolated local example credential.
const defaultPostgresDSN = "postgres://gomessenger:gomessenger@127.0.0.1:5432/gomessenger?sslmode=disable"

// Config controls one isolated PostgreSQL Inbox experiment.
type Config struct {
	PostgresDSN    string
	ResultsRoot    string
	RunID          string
	Warmup         int
	Operations     int
	Concurrencies  []int
	Repetitions    int
	DBMaxOpenConns int
	ReadyTimeout   time.Duration
	HostOS         string
	HostArch       string
	HostCPUs       string
	GitCommit      string
	GitDirty       string
}

// ResultDir returns the ignored artifact directory for this run.
func (c Config) ResultDir() string { return filepath.Join(c.ResultsRoot, c.RunID) }

// FromEnvironment parses the PostgreSQL-only benchmark contract.
func FromEnvironment() (Config, error) { return fromLookup(os.LookupEnv, time.Now) }

func fromLookup(lookup func(string) (string, bool), now func() time.Time) (Config, error) {
	config := Config{
		PostgresDSN: envValue(lookup, "POSTGRES_DSN", defaultPostgresDSN),
		ResultsRoot: envValue(lookup, "CAPACITY_RESULTS_DIR", "tmp/capacity"),
		RunID:       envValue(lookup, "CAPACITY_RUN_ID", ""),
		Warmup:      1_000, Operations: 20_000, Concurrencies: []int{1, 4}, Repetitions: 3,
		DBMaxOpenConns: 10, ReadyTimeout: 60 * time.Second,
		HostOS:    envValue(lookup, "CAPACITY_HOST_OS", "unknown"),
		HostArch:  envValue(lookup, "CAPACITY_HOST_ARCH", "unknown"),
		HostCPUs:  envValue(lookup, "CAPACITY_HOST_CPUS", "unknown"),
		GitCommit: envValue(lookup, "CAPACITY_GIT_COMMIT", "unknown"),
		GitDirty:  envValue(lookup, "CAPACITY_GIT_DIRTY", "unknown"),
	}
	integers := []struct {
		name   string
		target *int
	}{
		{name: "INBOX_CAPACITY_WARMUP", target: &config.Warmup},
		{name: "INBOX_CAPACITY_OPERATIONS", target: &config.Operations},
		{name: "INBOX_CAPACITY_REPETITIONS", target: &config.Repetitions},
		{name: "DB_MAX_OPEN_CONNS", target: &config.DBMaxOpenConns},
	}
	for _, integer := range integers {
		value, err := positiveInt(lookup, integer.name, *integer.target)
		if err != nil {
			return Config{}, err
		}
		*integer.target = value
	}
	if value := envValue(lookup, "INBOX_CAPACITY_CONCURRENCIES", ""); value != "" {
		concurrencies, err := parseConcurrencies(value)
		if err != nil {
			return Config{}, err
		}
		config.Concurrencies = concurrencies
	}
	if value := envValue(lookup, "CAPACITY_READY_TIMEOUT", ""); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse CAPACITY_READY_TIMEOUT: %w", err)
		}
		config.ReadyTimeout = parsed
	}
	if config.RunID == "" {
		var err error
		config.RunID, err = newRunID(now())
		if err != nil {
			return Config{}, err
		}
	}
	if config.PostgresDSN == "" || config.ResultsRoot == "" || config.ReadyTimeout < time.Second {
		return Config{}, errors.New("PostgreSQL Inbox capacity connection, artifacts, and timeout must be valid")
	}
	if config.DBMaxOpenConns < config.Concurrencies[len(config.Concurrencies)-1] {
		return Config{}, errors.New("DB_MAX_OPEN_CONNS must cover maximum Inbox concurrency")
	}
	return config, nil
}

func parseConcurrencies(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	result := make([]int, 0, len(parts))
	previous := 0
	for _, part := range parts {
		parsed, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || parsed < 1 || parsed <= previous {
			return nil, fmt.Errorf("INBOX_CAPACITY_CONCURRENCIES must contain increasing positive integers, got %q", part)
		}
		result = append(result, parsed)
		previous = parsed
	}
	if len(result) == 0 {
		return nil, errors.New("INBOX_CAPACITY_CONCURRENCIES must not be empty")
	}
	return result, nil
}

func positiveInt(lookup func(string) (string, bool), name string, fallback int) (int, error) {
	value := envValue(lookup, name, "")
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, value)
	}
	return parsed, nil
}

func envValue(lookup func(string) (string, bool), name, fallback string) string {
	if value, ok := lookup(name); ok && value != "" {
		return value
	}
	return fallback
}

func newRunID(now time.Time) (string, error) {
	var random [4]byte
	if _, err := crand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate PostgreSQL Inbox capacity run ID: %w", err)
	}
	return now.UTC().Format("20060102T150405Z") + "-inbox-" + hex.EncodeToString(random[:]), nil
}
