package consumer_test

import (
	"context"
	"testing"

	"github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	"github.com/assurrussa/gomessenger/observability"
)

func TestImports(t *testing.T) {
	event := messenger.MustEvent("consumer.probe", 1, messenger.JSON[string]())
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
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	if _, _, err := builder.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
	_ = inbox.Key{}
	_ = natsadapter.HandlerConfig{
		Logger: logger, Propagator: observability.NewTraceContextPropagator(),
	}
	_ = outboxadapter.ProducerConfig{Name: "consumer.probe"}
}
