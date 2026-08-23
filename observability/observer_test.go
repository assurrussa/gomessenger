package observability_test

import (
	"errors"
	"math"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/assurrussa/gomessenger/observability"
)

const (
	testEventName = "media.processed"
	testRouteName = "nats.events"
)

func TestObserverRecordsMetricsAndExplicitTraceTiming(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })
	observer, err := observability.New(observability.Config{Registerer: registry, TracerProvider: provider})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	started := time.Unix(1_700_000_000, 0).UTC()
	observer.Observe(t.Context(), messenger.Observation{
		Operation: messenger.OperationDeliver, Kind: messenger.KindEvent,
		Name: testEventName, SchemaVersion: 1, Route: testRouteName,
		State: messenger.ReceiptBrokerConfirmed, StartedAt: started,
		Duration: 25 * time.Millisecond, Err: errors.New("broker rejected"),
	})
	labels := []string{
		"deliver", "event", testEventName, "1", testRouteName, "", "", "", "broker_confirmed", "error",
	}
	if got := testutil.ToFloat64(observer.Operations().WithLabelValues(labels...)); got != 1 {
		t.Fatalf("operations = %v", got)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || !spans[0].StartTime.Equal(started) ||
		!spans[0].EndTime.Equal(started.Add(25*time.Millisecond)) || len(spans[0].Events) != 1 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestObserverReusesAlreadyRegisteredCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	first, err := observability.New(observability.Config{Registerer: registry})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := observability.New(observability.Config{Registerer: registry})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Operations() != second.Operations() || first.Duration() != second.Duration() {
		t.Fatal("collector instances were not reused")
	}
}

func TestObserverRejectsInvalidDurationBucketsAtConstruction(t *testing.T) {
	tests := []struct {
		name    string
		buckets []float64
	}{
		{name: "descending", buckets: []float64{1, 0}},
		{name: "duplicate", buckets: []float64{1, 1}},
		{name: "NaN", buckets: []float64{math.NaN()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := observability.New(observability.Config{
				Registerer: prometheus.NewRegistry(), DurationBuckets: test.buckets,
			})
			if err == nil {
				t.Fatal("invalid duration buckets accepted")
			}
		})
	}
}
