// Package capacity implements the local k6 controller and business verifier.
package capacity

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

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

const (
	ProfileQuick = "quick"
	ProfileFull  = "full"
	ProfileSite  = "site"

	// The credentials belong only to the isolated local Compose example.
	//nolint:gosec // This is an isolated local example credential.
	defaultPostgresDSN = "postgres://gomessenger:gomessenger@127.0.0.1:5432/gomessenger?sslmode=disable"
)

// Config controls one isolated capacity experiment.
type Config struct {
	Profile                    string
	Rates                      []int
	WarmupDuration             time.Duration
	StageDuration              time.Duration
	DrainTimeout               time.Duration
	SampleInterval             time.Duration
	ReadyTimeout               time.Duration
	E2EP95SLO                  time.Duration
	MinimumRate                int
	AppURL                     string
	PostgresDSN                string
	NATSURL                    string
	ResultsRoot                string
	RunID                      string
	K6Binary                   string
	K6Script                   string
	OutboxWorkers              int
	OutboxReservationBatchSize int
	OutboxProducerMaxConns     int
	OutboxRelayMaxConns        int
	ConsumerConcurrency        int
	DBMaxOpenConns             int
	PayloadProfile             string
	HostOS                     string
	HostArch                   string
	HostCPUs                   string
	GitCommit                  string
	GitDirty                   string
}

// ResultDir returns the isolated artifact directory for this run.
func (c Config) ResultDir() string { return filepath.Join(c.ResultsRoot, c.RunID) }

// FromEnvironment parses and validates the capacity process contract.
func FromEnvironment() (Config, error) {
	return fromLookup(os.LookupEnv, time.Now)
}

func fromLookup(
	lookup func(string) (string, bool),
	now func() time.Time,
) (Config, error) {
	profile := envValue(lookup, "CAPACITY_PROFILE", ProfileQuick)
	if profile != ProfileQuick && profile != ProfileFull && profile != ProfileSite {
		return Config{}, fmt.Errorf("CAPACITY_PROFILE must be %q, %q, or %q", ProfileQuick, ProfileFull, ProfileSite)
	}
	config := defaultConfig(profile, lookup)
	if err := applyOverrides(&config, lookup); err != nil {
		return Config{}, err
	}
	if config.RunID == "" {
		var err error
		config.RunID, err = newRunID(now())
		if err != nil {
			return Config{}, err
		}
	}
	if err := validateConfig(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func defaultConfig(profile string, lookup func(string) (string, bool)) Config {
	config := Config{
		Profile:                    profile,
		Rates:                      []int{50, 100, 250, 500},
		WarmupDuration:             15 * time.Second,
		StageDuration:              30 * time.Second,
		DrainTimeout:               30 * time.Second,
		SampleInterval:             time.Second,
		ReadyTimeout:               60 * time.Second,
		E2EP95SLO:                  2 * time.Second,
		AppURL:                     strings.TrimRight(envValue(lookup, "CAPACITY_APP_URL", "http://127.0.0.1:8080"), "/"),
		PostgresDSN:                envValue(lookup, "POSTGRES_DSN", defaultPostgresDSN),
		NATSURL:                    envValue(lookup, "NATS_URL", "nats://127.0.0.1:4222"),
		ResultsRoot:                envValue(lookup, "CAPACITY_RESULTS_DIR", "tmp/capacity"),
		K6Binary:                   envValue(lookup, "K6_BIN", "k6"),
		K6Script:                   envValue(lookup, "K6_SCRIPT", "load/capacity.js"),
		OutboxWorkers:              4,
		OutboxReservationBatchSize: 1,
		OutboxProducerMaxConns:     9,
		OutboxRelayMaxConns:        1,
		ConsumerConcurrency:        4,
		DBMaxOpenConns:             32,
		PayloadProfile:             demo.CapacityPayloadMixed,
		HostOS:                     envValue(lookup, "CAPACITY_HOST_OS", unknownValue),
		HostArch:                   envValue(lookup, "CAPACITY_HOST_ARCH", unknownValue),
		HostCPUs:                   envValue(lookup, "CAPACITY_HOST_CPUS", unknownValue),
		GitCommit:                  envValue(lookup, "CAPACITY_GIT_COMMIT", unknownValue),
		GitDirty:                   envValue(lookup, "CAPACITY_GIT_DIRTY", unknownValue),
	}
	if profile == ProfileFull {
		config.Rates = []int{50, 100, 250, 500, 1_000, 2_000}
		config.WarmupDuration = 30 * time.Second
		config.StageDuration = 2 * time.Minute
		config.DrainTimeout = time.Minute
	}
	if profile == ProfileSite {
		config.Rates = []int{2000}
		config.WarmupDuration = 30 * time.Second
		config.StageDuration = 2 * time.Minute
		config.DrainTimeout = 30 * time.Second
		config.OutboxWorkers = 1
		config.ConsumerConcurrency = 1
		config.DBMaxOpenConns = 10
		config.PayloadProfile = demo.CapacityPayloadSmall
	}
	return config
}

func applyOverrides(config *Config, lookup func(string) (string, bool)) error {
	if value := envValue(lookup, "CAPACITY_RATES", ""); value != "" {
		rates, err := parseRates(value)
		if err != nil {
			return err
		}
		config.Rates = rates
	}
	durations := []struct {
		name   string
		target *time.Duration
	}{
		{name: "CAPACITY_WARMUP_DURATION", target: &config.WarmupDuration},
		{name: "CAPACITY_STAGE_DURATION", target: &config.StageDuration},
		{name: "CAPACITY_DRAIN_TIMEOUT", target: &config.DrainTimeout},
		{name: "CAPACITY_SAMPLE_INTERVAL", target: &config.SampleInterval},
		{name: "CAPACITY_READY_TIMEOUT", target: &config.ReadyTimeout},
		{name: "CAPACITY_E2E_P95_SLO", target: &config.E2EP95SLO},
	}
	for _, duration := range durations {
		value, err := envDuration(lookup, duration.name, *duration.target)
		if err != nil {
			return err
		}
		*duration.target = value
	}
	integers := []struct {
		name      string
		target    *int
		allowZero bool
	}{
		{name: "CAPACITY_MIN_RATE", target: &config.MinimumRate, allowZero: true},
		{name: "OUTBOX_WORKERS", target: &config.OutboxWorkers},
		{name: "OUTBOX_RESERVATION_BATCH_SIZE", target: &config.OutboxReservationBatchSize},
		{name: "OUTBOX_PRODUCER_MAX_CONNS", target: &config.OutboxProducerMaxConns},
		{name: "OUTBOX_RELAY_MAX_CONNS", target: &config.OutboxRelayMaxConns},
		{name: "NATS_CONSUMER_CONCURRENCY", target: &config.ConsumerConcurrency},
		{name: "DB_MAX_OPEN_CONNS", target: &config.DBMaxOpenConns},
	}
	for _, integer := range integers {
		var (
			value int
			err   error
		)
		if integer.allowZero {
			value, err = envNonNegativeInt(lookup, integer.name, *integer.target)
		} else {
			value, err = envPositiveInt(lookup, integer.name, *integer.target)
		}
		if err != nil {
			return err
		}
		*integer.target = value
	}
	config.RunID = envValue(lookup, "CAPACITY_RUN_ID", "")
	config.PayloadProfile = envValue(lookup, "CAPACITY_PAYLOAD_PROFILE", config.PayloadProfile)
	return nil
}

func validateConfig(config Config) error {
	if err := (demo.BenchmarkLabels{RunID: config.RunID, StageID: "warmup"}).Validate(); err != nil {
		return fmt.Errorf("validate CAPACITY_RUN_ID: %w", err)
	}
	if config.StageDuration < time.Second || config.WarmupDuration < time.Second ||
		config.DrainTimeout < time.Second || config.ReadyTimeout < time.Second {
		return errors.New("capacity durations must be at least one second")
	}
	if config.SampleInterval < 100*time.Millisecond || config.SampleInterval > config.StageDuration {
		return errors.New("CAPACITY_SAMPLE_INTERVAL must be between 100ms and the stage duration")
	}
	if config.E2EP95SLO <= 0 {
		return errors.New("CAPACITY_E2E_P95_SLO must be positive")
	}
	if config.DBMaxOpenConns < config.ConsumerConcurrency+2 {
		return errors.New("DB_MAX_OPEN_CONNS must cover consumer concurrency plus two")
	}
	if config.Profile == ProfileSite && config.OutboxProducerMaxConns+config.OutboxRelayMaxConns != 10 {
		return errors.New("site profile Outbox producer and relay connection budget must equal 10")
	}
	if config.OutboxReservationBatchSize < 1 || config.OutboxReservationBatchSize > 1_000 {
		return errors.New("OUTBOX_RESERVATION_BATCH_SIZE must be in 1..1000")
	}
	if config.PayloadProfile != demo.CapacityPayloadSmall && config.PayloadProfile != demo.CapacityPayloadMixed {
		return fmt.Errorf("CAPACITY_PAYLOAD_PROFILE must be %q or %q",
			demo.CapacityPayloadSmall, demo.CapacityPayloadMixed)
	}
	if config.AppURL == "" || config.PostgresDSN == "" || config.NATSURL == "" ||
		config.ResultsRoot == "" || config.K6Binary == "" || config.K6Script == "" {
		return errors.New("capacity connection and artifact settings must not be empty")
	}
	return nil
}

func parseRates(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	rates := make([]int, 0, len(parts))
	previous := 0
	for _, part := range parts {
		rate, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || rate < 1 {
			return nil, fmt.Errorf("CAPACITY_RATES must contain positive integers, got %q", part)
		}
		if rate <= previous {
			return nil, errors.New("CAPACITY_RATES must be strictly increasing")
		}
		rates = append(rates, rate)
		previous = rate
	}
	if len(rates) == 0 {
		return nil, errors.New("CAPACITY_RATES must not be empty")
	}
	return rates, nil
}

func envDuration(
	lookup func(string) (string, bool),
	name string,
	fallback time.Duration,
) (time.Duration, error) {
	value := envValue(lookup, name, "")
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func envPositiveInt(
	lookup func(string) (string, bool),
	name string,
	fallback int,
) (int, error) {
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

func envNonNegativeInt(
	lookup func(string) (string, bool),
	name string,
	fallback int,
) (int, error) {
	value := envValue(lookup, name, "")
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", name, value)
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
		return "", fmt.Errorf("generate capacity run ID: %w", err)
	}
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random[:]), nil
}
