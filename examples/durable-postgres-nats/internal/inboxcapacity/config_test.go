//nolint:testpackage // Tests exercise package-local environment parsing.
package inboxcapacity

import (
	"reflect"
	"testing"
	"time"
)

func TestConfigDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		environment       map[string]string
		wantOperations    int
		wantConcurrencies []int
		wantRepetitions   int
	}{
		{name: "defaults", wantOperations: 20_000, wantConcurrencies: []int{1, 4}, wantRepetitions: 3},
		{
			name: "smoke", environment: map[string]string{
				"INBOX_CAPACITY_OPERATIONS": "100", "INBOX_CAPACITY_CONCURRENCIES": "1,2",
				"INBOX_CAPACITY_REPETITIONS": "1", "DB_MAX_OPEN_CONNS": "4",
			},
			wantOperations: 100, wantConcurrencies: []int{1, 2}, wantRepetitions: 1,
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
			if config.Operations != test.wantOperations || config.Repetitions != test.wantRepetitions ||
				!reflect.DeepEqual(config.Concurrencies, test.wantConcurrencies) {
				t.Fatalf("config = %#v", config)
			}
		})
	}
}

func TestConfigRejectsInvalidConcurrency(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"0", "4,1", "1,1", "fast"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, err := fromLookup(mapLookup(map[string]string{"INBOX_CAPACITY_CONCURRENCIES": value}), time.Now)
			if err == nil {
				t.Fatal("fromLookup() unexpectedly succeeded")
			}
		})
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}
