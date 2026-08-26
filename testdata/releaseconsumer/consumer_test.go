package consumer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	"github.com/assurrussa/gomessenger/observability"
)

func TestImports(t *testing.T) {
	event := messenger.MustEvent("consumer.probe", 1, messenger.JSON[string]())
	query := messenger.MustQuery[string, int]("consumer.lookup", 1, messenger.JSON[string]())
	logger := messenger.AdaptSlog(nil)
	panicReporter := messenger.PanicReporterFunc(func(context.Context, messenger.PanicReport) {})
	failureSanitizer := messenger.DefaultFailureSanitizer()
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:consumer-probe"),
		messenger.WithLogger(logger),
		messenger.WithObserver(messenger.NewSanitizedLoggingObserver(logger, failureSanitizer)),
		messenger.WithContextPropagator(observability.NewTraceContextPropagator()),
		messenger.WithPanicReporter(panicReporter),
		messenger.WithRuntimeShutdownTimeout(time.Second),
	)
	builder.UseMiddleware(func(
		ctx context.Context,
		_ messenger.Metadata,
		_ string,
		next messenger.HandlerFunc,
	) error {
		return next(ctx)
	})
	builder.SubscribeFunc(event, "consumer-probe", func(context.Context, string) error { return nil })
	builder.HandleQueryFunc(query, "consumer-lookup", func(context.Context, string) (int, error) { return 1, nil })
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	bus, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := runtime.Liveness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("liveness before run: %v", err)
	}
	if value, err := messenger.BindQuerier(bus, query).Query(t.Context(), "key"); err != nil || value != 1 {
		t.Fatalf("query = %d, %v", value, err)
	}
	_ = inbox.Key{}
	_ = natsadapter.HandlerConfig{
		Logger: logger, Propagator: observability.NewTraceContextPropagator(),
		PanicReporter: panicReporter, FailureSanitizer: failureSanitizer,
	}
	_ = kafkaadapter.HandlerConfig{
		Namespace: "consumer", ConsumerID: "consumer-probe", Logger: logger,
		Propagator: observability.NewTraceContextPropagator(), PanicReporter: panicReporter,
		FailureSanitizer: failureSanitizer,
	}
	_ = outboxadapter.ProducerConfig{Name: "consumer.probe"}
}
