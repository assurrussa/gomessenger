package consumer_test

import (
	"context"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	"github.com/assurrussa/gomessenger/observability"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/shared/types"
	"github.com/prometheus/client_golang/prometheus"
)

type downloadJob struct {
	JobID int64 `json:"jobId"`
}

type putter struct{}

func (putter) PutVersionedUnique(
	_ context.Context,
	_ string,
	_ string,
	_ coreoutbox.SchemaVersion,
	_ string,
	_ time.Time,
) (coreoutbox.UniquePutResult, error) {
	return coreoutbox.UniquePutResult{JobID: types.NewJobID(), Created: true}, nil
}

type backend struct{}

func (backend) Process(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, handler(ctx)
}

func (backend) Prune(_ context.Context, _ time.Time, _ int) (int64, error) { return 0, nil }

func TestPublishedFacadeAndOptionalModulesCompileForConsumer(t *testing.T) {
	command := messenger.MustCommand("download.job", 1, messenger.JSON[downloadJob]())
	event := messenger.MustEvent("download.completed", 1, messenger.JSON[downloadJob]())
	telemetry, err := observability.New(observability.Config{Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatalf("observability: %v", err)
	}
	logger := messenger.AdaptSlog(nil)
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:download-video"),
		messenger.WithLogger(logger),
		messenger.WithObserver(telemetry),
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
	builder.HandleCommandFunc(command, "download-worker", func(context.Context, downloadJob) error { return nil })
	builder.SubscribeFunc(event, "download-audit", func(context.Context, downloadJob) error { return nil })
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	bus, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := messenger.BindSender(bus, command).Send(t.Context(), downloadJob{JobID: 42}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := messenger.BindPublisher(bus, event).Publish(t.Context(), downloadJob{JobID: 42}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	encoded, err := messenger.JSON[downloadJob]().Encode(downloadJob{JobID: 42})
	if err != nil || string(encoded) != `{"jobId":42}` {
		t.Fatalf("legacy download payload = %s, %v", encoded, err)
	}

	producer, err := outboxadapter.NewProducer(putter{}, outboxadapter.ProducerConfig{Name: "outbox.download"})
	if err != nil || producer.Name() != "outbox.download" {
		t.Fatalf("outbox producer = %#v, %v", producer, err)
	}
	store, err := inbox.New(backend{})
	if err != nil || store == nil {
		t.Fatalf("inbox store = %#v, %v", store, err)
	}
	if err := natsadapter.ValidateTopology(natsadapter.Topology{
		SpecVersion: "1.0",
		Streams: []natsadapter.StreamSpec{
			natsadapter.DevStream("MESSAGES", "download.command.>", "download.event.>"),
			natsadapter.DevDLQStream("MESSAGES_DLQ", "download.dlq"),
		},
	}); err != nil {
		t.Fatalf("topology: %v", err)
	}
	consumerConfig := natsadapter.HandlerConfig{
		Logger: logger, Observers: []messenger.Observer{telemetry},
		Middlewares: []messenger.Middleware{func(
			ctx context.Context,
			_ messenger.Metadata,
			_ string,
			next messenger.HandlerFunc,
		) error {
			return next(ctx)
		}},
		Propagator: observability.NewTraceContextPropagator(),
	}
	if consumerConfig.Logger == nil || consumerConfig.Propagator == nil {
		t.Fatal("consumer observability configuration did not compile")
	}
}

var (
	_ coreoutbox.UniqueVersionedPutter = putter{}
	_ inbox.Backend                    = backend{}
)
