//nolint:testpackage // Tests exercise package-local configuration parsing without mutating process environment.
package capacity

import (
	"reflect"
	"testing"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

func TestConfigProfilesAndOverrides(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		environment   map[string]string
		wantRates     []int
		wantStage     time.Duration
		wantDrain     time.Duration
		wantMinimum   int
		wantWarmup    time.Duration
		wantWorkers   int
		wantConsumers int
		wantDBMax     int
		wantPayload   string
	}{
		{
			name: "quick defaults", wantRates: []int{50, 100, 250, 500},
			wantStage: 30 * time.Second, wantDrain: 30 * time.Second,
			wantWarmup: 15 * time.Second, wantWorkers: 4, wantConsumers: 4, wantDBMax: 32,
			wantPayload: demo.CapacityPayloadMixed,
		},
		{
			name: "full defaults", environment: map[string]string{"CAPACITY_PROFILE": ProfileFull},
			wantRates: []int{50, 100, 250, 500, 1_000, 2_000},
			wantStage: 2 * time.Minute, wantDrain: time.Minute,
			wantWarmup: 30 * time.Second, wantWorkers: 4, wantConsumers: 4, wantDBMax: 32,
			wantPayload: demo.CapacityPayloadMixed,
		},
		{
			name: "site defaults", environment: map[string]string{"CAPACITY_PROFILE": ProfileSite},
			wantRates: []int{250, 325, 350, 400, 500}, wantStage: 2 * time.Minute,
			wantDrain: 30 * time.Second, wantWarmup: 30 * time.Second,
			wantWorkers: 1, wantConsumers: 1, wantDBMax: 10, wantPayload: demo.CapacityPayloadSmall,
		},
		{
			name: "single rate and gate",
			environment: map[string]string{
				"CAPACITY_RATES": "500", "CAPACITY_STAGE_DURATION": "10s", "CAPACITY_MIN_RATE": "500",
			},
			wantRates: []int{500}, wantStage: 10 * time.Second,
			wantDrain: 30 * time.Second, wantMinimum: 500,
			wantWarmup: 15 * time.Second, wantWorkers: 4, wantConsumers: 4, wantDBMax: 32,
			wantPayload: demo.CapacityPayloadMixed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config, err := fromLookup(mapLookup(test.environment), func() time.Time {
				return time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
			})
			if err != nil {
				t.Fatalf("fromLookup() error = %v", err)
			}
			if !reflect.DeepEqual(config.Rates, test.wantRates) || config.StageDuration != test.wantStage ||
				config.DrainTimeout != test.wantDrain || config.MinimumRate != test.wantMinimum ||
				config.WarmupDuration != test.wantWarmup || config.OutboxWorkers != test.wantWorkers ||
				config.ConsumerConcurrency != test.wantConsumers || config.DBMaxOpenConns != test.wantDBMax ||
				config.PayloadProfile != test.wantPayload {
				t.Fatalf("config = %#v", config)
			}
		})
	}
}

func TestConfigRejectsInvalidRates(t *testing.T) {
	t.Parallel()
	tests := []string{"", "0", "100,50", "50,50", "fast"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			environment := map[string]string{"CAPACITY_RATES": value}
			if value == "" {
				environment["CAPACITY_RATES"] = "0"
			}
			_, err := fromLookup(mapLookup(environment), time.Now)
			if err == nil {
				t.Fatal("fromLookup() unexpectedly succeeded")
			}
		})
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
