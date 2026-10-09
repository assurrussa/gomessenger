package observability_test

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
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
	batchSizeMetric     = "gomessenger_messenger_batch_size"
	batchOutcomesMetric = "gomessenger_messenger_batch_outcomes_total"
	duplicatesMetric    = "gomessenger_messenger_duplicates_total"
	messagesMetric      = "gomessenger_messenger_messages_total"
	testEventName       = "media.processed"
	testRouteName       = "nats.events"
)

func TestObserverRecordsMetricsAndExplicitTraceTiming(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter := tracetest.NewInMemoryExporter()
	// Keep this recording assertion independent of host OTEL_TRACES_SAMPLER settings.
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter), trace.WithSampler(trace.AlwaysSample()))
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

func TestObserverCountsItemsAndBatchDecisionsSeparately(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := observability.New(observability.Config{Registerer: registry})
	if err != nil {
		t.Fatal(err)
	}
	// Failed batches can contain selected ACKs without broker confirmation.
	for _, observation := range []messenger.Observation{
		{Operation: messenger.OperationHandle},
		{Operation: messenger.OperationHandle, Duplicate: true},
		{Operation: messenger.OperationHandle, Err: errors.New("handler failed")},
		{
			Operation: messenger.OperationBatchHandle, BatchSize: 6, BatchACKs: 2,
			BatchRetries: 1, BatchDeferrals: 1, BatchDLQs: 2, Err: errors.New("ACK failed"),
		},
		{Operation: messenger.OperationBrokerAck},
		{Operation: messenger.OperationOffsetCommit},
		{Operation: messenger.OperationRetryHandoff},
		{Operation: messenger.OperationDLQHandoff},
		{Operation: messenger.OperationDeliver, Duplicate: true, BatchSize: 99, BatchACKs: 99},
		{Operation: messenger.OperationQuery},
		{Operation: messenger.OperationService},
	} {
		observer.Observe(t.Context(), observation)
	}
	assertCounterTotal(t, registry, messagesMetric, "", "", 3)
	assertCounterTotal(t, registry, duplicatesMetric, "", "", 1)
	for result, want := range map[string]float64{"ack": 2, "retry": 1, "defer": 1, "dlq": 2} {
		assertCounterTotal(t, registry, batchOutcomesMetric, "result", result, want)
	}
	assertCounterTotal(t, registry, batchOutcomesMetric, "outcome", "error", 6)
	assertBatchHistogram(t, registry, 1, 6)
}

func TestObserverIgnoresInvalidBatchCounts(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := observability.New(observability.Config{Registerer: registry})
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{-1, 0} {
		observer.Observe(t.Context(), messenger.Observation{
			Operation: messenger.OperationBatchHandle, BatchSize: size, BatchACKs: 10,
		})
	}
	observer.Observe(t.Context(), messenger.Observation{
		Operation: messenger.OperationBatchHandle, BatchSize: 1,
		BatchACKs: -1, BatchRetries: -1, BatchDeferrals: -1, BatchDLQs: -1,
	})
	assertCounterTotal(t, registry, batchOutcomesMetric, "", "", 0)
	assertBatchHistogram(t, registry, 1, 1)
}

func TestObserverMetricsReuseAndBoundedLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	observers := make([]*observability.Observer, 2)
	for index := range observers {
		var err error
		observers[index], err = observability.New(observability.Config{Registerer: registry})
		if err != nil {
			t.Fatal(err)
		}
	}
	for index := range 20 {
		observation := messenger.Observation{
			Operation: messenger.OperationHandle, MessageID: messenger.MessageID{byte(index + 1)},
			Duplicate: true, Attempt: uint64(index + 1), RetryDelay: time.Duration(index) * time.Second,
			Err: errors.New("private error " + strconv.Itoa(index)),
		}
		observers[index%2].Observe(t.Context(), observation)
		observation.Operation = messenger.OperationBatchHandle
		observation.BatchSize = index + 1
		observation.BatchBytes = index * 100
		observation.BatchACKs = 1
		observers[index%2].Observe(t.Context(), observation)
	}
	assertCounterTotal(t, registry, messagesMetric, "", "", 20)
	assertCounterTotal(t, registry, duplicatesMetric, "", "", 20)
	assertCounterTotal(t, registry, batchOutcomesMetric, "result", "ack", 20)
	assertBatchHistogram(t, registry, 20, 210)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"operation": true, "kind": true, "name": true, "schema_version": true, "route": true,
		"handler": true, "consumer": true, "service": true, "state": true, "outcome": true,
	}
	for _, family := range families {
		wantSeries := 1
		if family.GetName() == "gomessenger_messenger_operations_total" ||
			family.GetName() == "gomessenger_messenger_operation_duration_seconds" {
			wantSeries = 2
		}
		if len(family.Metric) != wantSeries {
			t.Errorf("%s series = %d, want %d", family.GetName(), len(family.Metric), wantSeries)
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				validResult := family.GetName() == batchOutcomesMetric &&
					label.GetName() == "result" && label.GetValue() == "ack"
				if !allowed[label.GetName()] && !validResult {
					t.Errorf("unexpected label %s on %s", label.GetName(), family.GetName())
				}
				if strings.Contains(label.GetValue(), "private") {
					t.Errorf("private error leaked into %s", family.GetName())
				}
			}
		}
	}
}

func assertCounterTotal(t *testing.T, registry *prometheus.Registry, name, labelName, labelValue string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var got float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matches := labelName == ""
			for _, label := range metric.Label {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					matches = true
				}
			}
			if matches {
				got += metric.GetCounter().GetValue()
			}
		}
	}
	if got != want {
		t.Errorf("%s{%s=%s} = %v, want %v", name, labelName, labelValue, got, want)
	}
}

func assertBatchHistogram(t *testing.T, registry *prometheus.Registry, count uint64, sum float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != batchSizeMetric {
			continue
		}
		if len(family.Metric) != 1 {
			t.Fatalf("batch size series = %d", len(family.Metric))
		}
		histogram := family.Metric[0].GetHistogram()
		if histogram.GetSampleCount() != count || histogram.GetSampleSum() != sum {
			t.Errorf("batch size count/sum = %d/%g, want %d/%g",
				histogram.GetSampleCount(), histogram.GetSampleSum(), count, sum)
		}
		return
	}
	t.Fatal("missing batch size histogram")
}

func TestObserverCountsConcurrentItemsAndBatches(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := observability.New(observability.Config{Registerer: registry})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 100 {
		workers.Go(func() {
			observer.Observe(t.Context(), messenger.Observation{
				Operation: messenger.OperationHandle, Duplicate: true,
			})
			observer.Observe(t.Context(), messenger.Observation{
				Operation: messenger.OperationBatchHandle, BatchSize: 1, BatchACKs: 1,
			})
		})
	}
	workers.Wait()
	assertCounterTotal(t, registry, messagesMetric, "", "", 100)
	assertCounterTotal(t, registry, duplicatesMetric, "", "", 100)
	assertCounterTotal(t, registry, batchOutcomesMetric, "result", "ack", 100)
	assertBatchHistogram(t, registry, 100, 100)
}
