//nolint:testpackage // Tests exercise package-local configuration parsing without mutating process environment.
package capacity

import (
	"reflect"
	"testing"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

const capacityProfileEnvironment = "CAPACITY_PROFILE"

func TestConfigProfilesAndOverrides(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                      string
		environment               map[string]string
		wantRates                 []int
		wantStage                 time.Duration
		wantDrain                 time.Duration
		wantMinimum               int
		wantWarmup                time.Duration
		wantWorkers               int
		wantBatch                 int
		wantProducerMax           int
		wantRelayMax              int
		wantConsumers             int
		wantConsumerMode          demo.ConsumerMode
		wantConsumerBatchMessages int
		wantConsumerBatchBytes    int
		wantConsumerBatchWait     time.Duration
		wantDBMax                 int
		wantPayload               string
	}{
		{
			name: "quick defaults", wantRates: []int{50, 100, 250, 500},
			wantStage: 30 * time.Second, wantDrain: 30 * time.Second,
			wantWarmup: 15 * time.Second, wantWorkers: 4, wantBatch: 1,
			wantProducerMax: 9, wantRelayMax: 1,
			wantConsumers: 4, wantDBMax: 32,
			wantConsumerMode:          demo.ConsumerModeSingle,
			wantConsumerBatchMessages: 100, wantConsumerBatchBytes: 4 << 20,
			wantConsumerBatchWait: 25 * time.Millisecond,
			wantPayload:           demo.CapacityPayloadMixed,
		},
		{
			name: "full defaults", environment: map[string]string{capacityProfileEnvironment: ProfileFull},
			wantRates: []int{50, 100, 250, 500, 1_000, 2_000},
			wantStage: 2 * time.Minute, wantDrain: time.Minute,
			wantWarmup: 30 * time.Second, wantWorkers: 4, wantBatch: 1,
			wantProducerMax: 9, wantRelayMax: 1,
			wantConsumers: 4, wantDBMax: 32,
			wantConsumerMode:          demo.ConsumerModeSingle,
			wantConsumerBatchMessages: 100, wantConsumerBatchBytes: 4 << 20,
			wantConsumerBatchWait: 25 * time.Millisecond,
			wantPayload:           demo.CapacityPayloadMixed,
		},
		{
			name: "site defaults", environment: map[string]string{capacityProfileEnvironment: ProfileSite},
			wantRates: []int{2_000}, wantStage: 2 * time.Minute,
			wantDrain: 30 * time.Second, wantWarmup: 30 * time.Second,
			wantWorkers: 1, wantBatch: 1, wantProducerMax: 9, wantRelayMax: 1,
			wantConsumers: 1, wantDBMax: 10, wantPayload: demo.CapacityPayloadSmall,
			wantConsumerMode:          demo.ConsumerModeSingle,
			wantConsumerBatchMessages: 100, wantConsumerBatchBytes: 4 << 20,
			wantConsumerBatchWait: 25 * time.Millisecond,
		},
		{
			name: "single rate and gate",
			environment: map[string]string{
				"CAPACITY_RATES": "500", "CAPACITY_STAGE_DURATION": "10s", "CAPACITY_MIN_RATE": "500",
				"OUTBOX_RESERVATION_BATCH_SIZE": "32",
				"CONSUMER_MODE":                 "batch", "CONSUMER_BATCH_MAX_MESSAGES": "64",
				"CONSUMER_BATCH_MAX_BYTES": "2097152", "CONSUMER_BATCH_MAX_WAIT": "40ms",
			},
			wantRates: []int{500}, wantStage: 10 * time.Second,
			wantDrain: 30 * time.Second, wantMinimum: 500,
			wantWarmup: 15 * time.Second, wantWorkers: 4, wantBatch: 32,
			wantProducerMax: 9, wantRelayMax: 1,
			wantConsumers: 4, wantDBMax: 32,
			wantConsumerMode:          demo.ConsumerModeBatch,
			wantConsumerBatchMessages: 64, wantConsumerBatchBytes: 2 << 20,
			wantConsumerBatchWait: 40 * time.Millisecond,
			wantPayload:           demo.CapacityPayloadMixed,
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
				config.OutboxReservationBatchSize != test.wantBatch ||
				config.OutboxProducerMaxConns != test.wantProducerMax ||
				config.OutboxRelayMaxConns != test.wantRelayMax ||
				config.ConsumerConcurrency != test.wantConsumers || config.DBMaxOpenConns != test.wantDBMax ||
				config.ConsumerMode != test.wantConsumerMode ||
				config.ConsumerBatchMaxMessages != test.wantConsumerBatchMessages ||
				config.ConsumerBatchMaxBytes != test.wantConsumerBatchBytes ||
				config.ConsumerBatchMaxWait != test.wantConsumerBatchWait ||
				config.PayloadProfile != test.wantPayload {
				t.Fatalf("config = %#v", config)
			}
		})
	}
}

func TestConfigRejectsInvalidConsumerBatchSettings(t *testing.T) {
	t.Parallel()
	tests := []map[string]string{
		{"CONSUMER_MODE": "automatic"},
		{"CONSUMER_BATCH_MAX_MESSAGES": "-1"},
		{"CONSUMER_BATCH_MAX_BYTES": "0"},
		{"CONSUMER_BATCH_MAX_WAIT": "0s"},
	}
	for _, environment := range tests {
		if _, err := fromLookup(mapLookup(environment), time.Now); err == nil {
			t.Fatalf("fromLookup(%v) unexpectedly succeeded", environment)
		}
	}
}

func TestConfigRejectsReservationBatchAboveMaximum(t *testing.T) {
	t.Parallel()
	_, err := fromLookup(mapLookup(map[string]string{
		"OUTBOX_RESERVATION_BATCH_SIZE": "1001",
	}), time.Now)
	if err == nil {
		t.Fatal("fromLookup() unexpectedly accepted reservation batch size 1001")
	}
}

func TestSiteProfileRejectsOutboxPoolBudgetDrift(t *testing.T) {
	t.Parallel()
	_, err := fromLookup(mapLookup(map[string]string{
		capacityProfileEnvironment:  ProfileSite,
		"OUTBOX_PRODUCER_MAX_CONNS": "10",
		"OUTBOX_RELAY_MAX_CONNS":    "1",
	}), time.Now)
	if err == nil {
		t.Fatal("fromLookup() unexpectedly accepted an eleven-connection Outbox budget")
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
