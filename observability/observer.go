package observability

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/assurrussa/gomessenger/observability"

var defaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// Config selects host-owned Prometheus and OpenTelemetry providers.
type Config struct {
	Namespace       string
	Registerer      prometheus.Registerer
	TracerProvider  trace.TracerProvider
	DurationBuckets []float64
}

// Observer records low-cardinality messaging operation metrics and traces.
type Observer struct {
	operations *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	tracer     trace.Tracer
}

// New constructs and registers an observer. A nil Registerer uses the default
// Prometheus registerer; a nil TracerProvider uses the global provider.
func New(config Config) (*Observer, error) {
	if config.Namespace == "" {
		config.Namespace = "gomessenger"
	}
	if config.Registerer == nil {
		config.Registerer = prometheus.DefaultRegisterer
	}
	if config.TracerProvider == nil {
		config.TracerProvider = otel.GetTracerProvider()
	}
	if len(config.DurationBuckets) == 0 {
		config.DurationBuckets = defaultBuckets
	}
	if err := validateDurationBuckets(config.DurationBuckets); err != nil {
		return nil, err
	}
	labels := []string{
		"operation", "kind", "name", "schema_version", "route", "handler", "consumer", "service", "state", "outcome",
	}
	observer := &Observer{
		operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: "messenger",
			Name:      "operations_total",
			Help:      "Total completed gomessenger operations.",
		}, labels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: "messenger",
			Name:      "operation_duration_seconds",
			Help:      "Duration of completed gomessenger operations.",
			Buckets:   append([]float64(nil), config.DurationBuckets...),
		}, labels),
		tracer: config.TracerProvider.Tracer(instrumentationName),
	}
	if err := registerCounter(config.Registerer, &observer.operations); err != nil {
		return nil, err
	}
	if err := registerHistogram(config.Registerer, &observer.duration); err != nil {
		return nil, err
	}
	return observer, nil
}

func validateDurationBuckets(buckets []float64) error {
	for index, bucket := range buckets {
		if math.IsNaN(bucket) {
			return fmt.Errorf("messenger/observability: duration bucket %d is NaN", index)
		}
		if index > 0 && bucket <= buckets[index-1] {
			return fmt.Errorf(
				"messenger/observability: duration buckets must be strictly increasing: %g >= %g",
				buckets[index-1], bucket,
			)
		}
	}
	return nil
}

// Observe implements messenger.Observer.
func (o *Observer) Observe(ctx context.Context, observation messenger.Observation) {
	if o == nil {
		return
	}
	outcome := "ok"
	if observation.Err != nil {
		outcome = "error"
	}
	labels := prometheus.Labels{
		"operation":      string(observation.Operation),
		"kind":           string(observation.Kind),
		"name":           observation.Name,
		"schema_version": strconv.Itoa(observation.SchemaVersion),
		"route":          observation.Route,
		"handler":        observation.HandlerID,
		"consumer":       observation.ConsumerID,
		"service":        observation.ServiceID,
		"state":          string(observation.State),
		"outcome":        outcome,
	}
	o.operations.With(labels).Inc()
	o.duration.With(labels).Observe(max(0, observation.Duration.Seconds()))

	startedAt := observation.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().Add(-observation.Duration)
	}
	endedAt := startedAt.Add(observation.Duration)
	attributes := []attribute.KeyValue{
		attribute.String("messaging.operation", string(observation.Operation)),
		attribute.String("messaging.message.kind", string(observation.Kind)),
		attribute.String("messaging.message.name", observation.Name),
		attribute.Int("messaging.message.schema_version", observation.SchemaVersion),
		attribute.String("messaging.route", observation.Route),
		attribute.String("messaging.handler.id", observation.HandlerID),
		attribute.String("messaging.consumer.id", observation.ConsumerID),
		attribute.String("messaging.service.id", observation.ServiceID),
		attribute.String("messaging.receipt.state", string(observation.State)),
		attribute.Bool("messaging.message.duplicate", observation.Duplicate),
	}
	if !observation.MessageID.IsZero() {
		attributes = append(attributes, attribute.String("messaging.message.id", observation.MessageID.String()))
	}
	if observation.Attempt != 0 {
		attributes = append(attributes,
			attribute.String("messaging.message.delivery_attempt", strconv.FormatUint(observation.Attempt, 10)))
	}
	if observation.RetryDelay != 0 {
		attributes = append(attributes,
			attribute.Int64("messaging.message.retry_delay_ms", observation.RetryDelay.Milliseconds()))
	}
	if observation.BatchSize != 0 {
		attributes = append(attributes,
			attribute.Int("messaging.batch.size", observation.BatchSize),
			attribute.Int("messaging.batch.bytes", observation.BatchBytes),
			attribute.Int("messaging.batch.handler_messages", observation.BatchHandlerMessages),
			attribute.Int("messaging.batch.acks", observation.BatchACKs),
			attribute.Int("messaging.batch.retries", observation.BatchRetries),
			attribute.Int("messaging.batch.deferrals", observation.BatchDeferrals),
			attribute.Int("messaging.batch.dlqs", observation.BatchDLQs),
			attribute.Int64("messaging.batch.fill_duration_ms", observation.BatchFillDuration.Milliseconds()),
			attribute.Int64("messaging.batch.handler_duration_ms", observation.BatchHandlerDuration.Milliseconds()),
		)
	}
	_, span := o.tracer.Start(
		ctx,
		"messenger."+string(observation.Operation),
		trace.WithTimestamp(startedAt),
		trace.WithAttributes(attributes...),
	)
	if observation.Err != nil {
		span.RecordError(observation.Err, trace.WithTimestamp(endedAt))
		span.SetStatus(codes.Error, "messaging operation failed")
	}
	span.End(trace.WithTimestamp(endedAt))
}

func registerCounter(registerer prometheus.Registerer, collector **prometheus.CounterVec) error {
	if err := registerer.Register(*collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
				*collector = existing
				return nil
			}
		}
		return fmt.Errorf("messenger/observability: register operation counter: %w", err)
	}
	return nil
}

func registerHistogram(registerer prometheus.Registerer, collector **prometheus.HistogramVec) error {
	if err := registerer.Register(*collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.HistogramVec); ok {
				*collector = existing
				return nil
			}
		}
		return fmt.Errorf("messenger/observability: register duration histogram: %w", err)
	}
	return nil
}

var _ messenger.Observer = (*Observer)(nil)
