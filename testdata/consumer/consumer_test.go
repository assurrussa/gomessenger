package consumer_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxpgsql "github.com/assurrussa/gomessenger/adapters/inbox/pgsql"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
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

func compileSQLInboxNamespaceAPI(ctx context.Context, db *sql.DB) {
	postgresOptions := []inboxpgsql.Option{
		inboxpgsql.WithSchema("messaging"),
		inboxpgsql.WithTablePrefix("site_"),
	}
	_, _ = inboxpgsql.New(db, postgresOptions...)
	_ = inboxpgsql.Migrate(ctx, db, postgresOptions...)

	sqliteOptions := []inboxsqlite.Option{inboxsqlite.WithTablePrefix("site_")}
	_, _ = inboxsqlite.New(db, sqliteOptions...)
	_ = inboxsqlite.Migrate(ctx, db, sqliteOptions...)
}

func (backend) Process(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, handler(ctx)
}

func (backend) Prune(_ context.Context, _ time.Time, _ int) (int64, error) { return 0, nil }

func (backend) ProcessAttempt(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	_ uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{Attempt: 1}, handler(ctx)
}

func (backend) ForgetAttempt(context.Context, inbox.Key, inbox.Fingerprint) error { return nil }

func (backend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	_ uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	result, err := handler(ctx, items)
	return inbox.BatchProcessResult{
		Items:           make([]inbox.BatchItemOutcome, len(result.Items)),
		HandlerMessages: len(items),
	}, err
}

func TestPublishedFacadeAndOptionalModulesCompileForConsumer(t *testing.T) {
	command := messenger.MustCommand("download.job", 1, messenger.JSON[downloadJob]())
	event := messenger.MustEvent("download.completed", 1, messenger.JSON[downloadJob]())
	query := messenger.MustQuery[downloadJob, string]("download.status", 1, messenger.JSON[downloadJob]())
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
	builder.HandleQueryFunc(query, "download-status", func(context.Context, downloadJob) (string, error) {
		return "ready", nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
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
	if result, err := messenger.BindQuerier(bus, query).Query(t.Context(), downloadJob{JobID: 42}); err != nil || result != "ready" {
		t.Fatalf("query = %q, %v", result, err)
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
	batchHandler := messenger.BatchHandler[downloadJob](func(
		_ context.Context,
		messages []messenger.Message[downloadJob],
	) (messenger.BatchResult, error) {
		result := messenger.BatchResult{Items: make([]messenger.BatchItemResult, len(messages))}
		for index, message := range messages {
			result.Items[index].Key = messenger.BatchItemKey{
				Source: message.Metadata.Source, MessageID: message.Metadata.ID,
			}
		}
		return result, nil
	})
	batchConfig := messenger.BatchConfig{Middlewares: []messenger.BatchMiddleware{func(
		ctx context.Context,
		_ []messenger.Metadata,
		_ string,
		next messenger.BatchHandlerFunc,
	) (messenger.BatchResult, error) {
		return next(ctx)
	}}}
	_, _ = natsadapter.NewBatchCommandConsumer(nil, store, command, batchHandler,
		natsadapter.HandlerConfig{}, batchConfig)
	_, _ = natsadapter.NewBatchEventConsumer(nil, store, event, batchHandler,
		natsadapter.HandlerConfig{}, batchConfig)
	_, _ = kafkaadapter.NewBatchCommandConsumer(nil, store, command, batchHandler,
		kafkaadapter.HandlerConfig{}, batchConfig)
	_, _ = kafkaadapter.NewBatchEventConsumer(nil, store, event, batchHandler,
		kafkaadapter.HandlerConfig{}, batchConfig)
	_ = messenger.DeferAfter(errors.New("later"), time.Second)
	if err := natsadapter.ValidateTopology(natsadapter.Topology{
		SpecVersion: "1.0",
		Streams: []natsadapter.StreamSpec{
			natsadapter.DevStream("MESSAGES", "download.command.>", "download.event.>"),
			natsadapter.DevDLQStream("MESSAGES_DLQ", "download.dlq"),
		},
	}); err != nil {
		t.Fatalf("topology: %v", err)
	}
	kafkaSource, err := kafkaadapter.Topic("download", command.Info())
	if err != nil {
		t.Fatalf("Kafka source topic: %v", err)
	}
	if _, err := kafkaadapter.RetryTopic(kafkaSource, "download-worker", 0); err != nil {
		t.Fatalf("Kafka retry topic: %v", err)
	}
	kafkaConsumerConfig := kafkaadapter.HandlerConfig{
		Namespace: "download", ConsumerID: "download-worker", Logger: logger,
		Observers: []messenger.Observer{telemetry}, Propagator: observability.NewTraceContextPropagator(),
	}
	if kafkaConsumerConfig.Logger == nil || kafkaConsumerConfig.Propagator == nil {
		t.Fatal("Kafka consumer observability configuration did not compile")
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
	_ inbox.AttemptBackend             = backend{}
	_ inbox.BatchAttemptBackend        = backend{}
)

func TestOptionalTerminalRetentionConsumerAPI(t *testing.T) {
	store, err := inbox.New(backend{})
	if err != nil {
		t.Fatal(err)
	}
	if store.SupportsTerminalRetention() {
		t.Fatal("legacy backend unexpectedly requires retention")
	}
	if _, err := store.PruneTerminalAttempts(t.Context(), time.Now(), 1); !errors.Is(err, inbox.ErrTerminalRetentionUnsupported) {
		t.Fatal(err)
	}
	var capability inbox.TerminalRetentionBackend = retentionBackend{}
	if err := capability.ConfirmTerminalHandoff(t.Context(), inbox.Key{}, inbox.Fingerprint{}); err != nil {
		t.Fatal(err)
	}
	_ = messenger.OperationExpire
	_ = natsadapter.QuarantineSpecVersion
	if _, err := natsadapter.PlanDLQReplay(natsadapter.DLQRecord{
		SpecVersion: natsadapter.QuarantineSpecVersion, Quarantine: &natsadapter.QuarantineInfo{Replayable: false},
	}); !errors.Is(err, natsadapter.ErrQuarantineReplay) {
		t.Fatal(err)
	}
}

type retentionBackend struct{ backend }

func (retentionBackend) ConfirmTerminalHandoff(context.Context, inbox.Key, inbox.Fingerprint) error {
	return nil
}

func (retentionBackend) PruneTerminalAttempts(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}
