package consumer_test

import (
	"context"
	"testing"

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
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:consumer-probe"),
		messenger.WithLogger(logger),
		messenger.WithObserver(messenger.NewLoggingObserver(logger)),
		messenger.WithContextPropagator(observability.NewTraceContextPropagator()),
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
	bus, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if value, err := messenger.BindQuerier(bus, query).Query(t.Context(), "key"); err != nil || value != 1 {
		t.Fatalf("query = %d, %v", value, err)
	}
	_ = inbox.Key{}
	_ = natsadapter.HandlerConfig{
		Logger: logger, Propagator: observability.NewTraceContextPropagator(),
	}
	_ = kafkaadapter.HandlerConfig{
		Namespace: "consumer", ConsumerID: "consumer-probe", Logger: logger,
		Propagator: observability.NewTraceContextPropagator(),
	}
	_ = outboxadapter.ProducerConfig{Name: "consumer.probe"}
}
