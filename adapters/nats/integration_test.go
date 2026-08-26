package nats_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "modernc.org/sqlite"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

type testPayload struct {
	JobID int64 `json:"jobId"`
}

type constantGenerator struct{ id messenger.MessageID }

func (g constantGenerator) New() (messenger.MessageID, error) { return g.id, nil }

type traceContextKey struct{}

type testTracePropagator struct{}

func (testTracePropagator) Inject(_ context.Context, carrier map[string]string) {
	carrier["traceparent"] = testTraceParent
	carrier["tracestate"] = testTraceState
}

func (testTracePropagator) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return context.WithValue(ctx, traceContextKey{}, carrier["traceparent"]+"|"+carrier["tracestate"])
}

type observerFunc func(context.Context, messenger.Observation)

func (observe observerFunc) Observe(ctx context.Context, observation messenger.Observation) {
	observe(ctx, observation)
}

const (
	testSource               = "urn:service:test"
	testProducerSource       = "urn:service:producer"
	testHostOwner            = "host"
	testTopologyConsumerName = "worker"
)

func TestTopologyPlanApplyAndRejectDrift(t *testing.T) {
	connection := startJetStream(t)
	topology := nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams:     []nats.StreamSpec{nats.DevStream(testStreamName, "test.>")},
	}
	changes, err := nats.ApplyTopology(t.Context(), connection, topology)
	if err != nil || len(changes) != 1 || changes[0].Action != nats.ChangeCreate {
		t.Fatalf("apply = %#v, %v", changes, err)
	}
	changes, err = nats.PlanTopology(t.Context(), connection, topology)
	if err != nil || len(changes) != 1 || changes[0].Action != nats.ChangeNoop {
		t.Fatalf("plan = %#v, %v", changes, err)
	}
	drifted := topology
	drifted.Streams = append([]nats.StreamSpec(nil), topology.Streams...)
	drifted.Streams[0].Storage = jetstream.FileStorage
	changes, err = nats.ApplyTopology(t.Context(), connection, drifted)
	if !errors.Is(err, nats.ErrTopologyDrift) || changes[0].Action != nats.ChangeConflict {
		t.Fatalf("drift apply = %#v, %v", changes, err)
	}
}

func TestTopologyBootstrapsMissingStreamBeforeConsumer(t *testing.T) {
	connection := startJetStream(t)
	topology := nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams:     []nats.StreamSpec{nats.DevStream("BOOTSTRAP", "bootstrap.>")},
		Consumers: []nats.ConsumerSpec{{
			Stream: "BOOTSTRAP", Name: testTopologyConsumerName, FilterSubject: "bootstrap.>",
			AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1, Replicas: 1,
		}},
	}
	changes, err := nats.ApplyTopology(t.Context(), connection, topology)
	if err != nil || len(changes) != 2 {
		t.Fatalf("apply = %#v, %v", changes, err)
	}
	for _, change := range changes {
		if change.Action != nats.ChangeCreate {
			t.Fatalf("create plan = %#v", changes)
		}
	}
	changes, err = nats.PlanTopology(t.Context(), connection, topology)
	if err != nil || len(changes) != 2 {
		t.Fatalf("plan = %#v, %v", changes, err)
	}
	for _, change := range changes {
		if change.Action != nats.ChangeNoop {
			t.Fatalf("noop plan = %#v", changes)
		}
	}
}

func TestTopologyRejectsOutOfScopeConsumerBeforeCreatingStream(t *testing.T) {
	connection := startJetStream(t)
	topology := nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams:     []nats.StreamSpec{nats.DevStream("FILTER_SCOPE", "orders.>")},
		Consumers: []nats.ConsumerSpec{{
			Stream: "FILTER_SCOPE", Name: testTopologyConsumerName, FilterSubject: "payments.>",
			AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1,
		}},
	}
	changes, applyErr := nats.ApplyTopology(t.Context(), connection, topology)
	if !errors.Is(applyErr, nats.ErrInvalidConfig) || len(changes) != 0 {
		t.Fatalf("invalid apply = %#v, %v", changes, applyErr)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.Stream(t.Context(), "FILTER_SCOPE"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("stream created before consumer validation: %v", err)
	}
}

func TestTopologyRejectsConsumerOutsideExistingStreamSubjects(t *testing.T) {
	connection := startJetStream(t)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "EXISTING_SCOPE", Subjects: []string{"orders.>"}, Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("create existing stream: %v", err)
	}
	topology := nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Consumers: []nats.ConsumerSpec{{
			Stream: "EXISTING_SCOPE", Name: testTopologyConsumerName, FilterSubject: "payments.>",
			AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1,
		}},
	}
	changes, err := nats.ApplyTopology(t.Context(), connection, topology)
	if !errors.Is(err, nats.ErrTopologyDrift) || len(changes) != 1 || changes[0].Action != nats.ChangeConflict {
		t.Fatalf("out-of-scope existing stream apply = %#v, %v", changes, err)
	}
	_, consumerErr := js.Consumer(t.Context(), "EXISTING_SCOPE", testTopologyConsumerName)
	if !errors.Is(consumerErr, jetstream.ErrConsumerNotFound) {
		t.Fatalf("consumer created after conflicting preflight: %v", consumerErr)
	}
}

func TestTopologyUpdatesPreserveHostOwnedConfiguration(t *testing.T) {
	connection := startJetStream(t)
	topology := nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams:     []nats.StreamSpec{nats.DevStream("PRESERVE", "preserve.one")},
		Consumers: []nats.ConsumerSpec{{
			Stream: "PRESERVE", Name: testTopologyConsumerName, Description: "before", FilterSubject: "preserve.one",
			AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1, Replicas: 1,
		}},
	}
	if _, err := nats.ApplyTopology(t.Context(), connection, topology); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(t.Context(), "PRESERVE")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	streamInfo, err := stream.Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	streamInfo.Config.AllowDirect = true
	streamInfo.Config.Metadata = map[string]string{"owner": testHostOwner}
	if _, err := js.UpdateStream(t.Context(), streamInfo.Config); err != nil {
		t.Fatalf("set host stream config: %v", err)
	}
	consumer, err := js.Consumer(t.Context(), "PRESERVE", testTopologyConsumerName)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	consumerInfo, err := consumer.Info(t.Context())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	consumerInfo.Config.Metadata = map[string]string{"owner": testHostOwner}
	if _, err := js.UpdateConsumer(t.Context(), "PRESERVE", consumerInfo.Config); err != nil {
		t.Fatalf("set host consumer config: %v", err)
	}

	topology.Streams[0].Description = "after"
	topology.Streams[0].Subjects = append(topology.Streams[0].Subjects, "preserve.two")
	topology.Consumers[0].Description = "after"
	if _, err := nats.ApplyTopology(t.Context(), connection, topology); err != nil {
		t.Fatalf("compatible update: %v", err)
	}
	stream, err = js.Stream(t.Context(), "PRESERVE")
	if err != nil {
		t.Fatalf("updated stream: %v", err)
	}
	streamInfo, err = stream.Info(t.Context())
	if err != nil {
		t.Fatalf("updated stream info: %v", err)
	}
	if !streamInfo.Config.AllowDirect || streamInfo.Config.Metadata["owner"] != testHostOwner {
		t.Fatalf("host stream config was lost: %#v", streamInfo.Config)
	}
	consumer, err = js.Consumer(t.Context(), "PRESERVE", testTopologyConsumerName)
	if err != nil {
		t.Fatalf("updated consumer: %v", err)
	}
	consumerInfo, err = consumer.Info(t.Context())
	if err != nil {
		t.Fatalf("updated consumer info: %v", err)
	}
	if consumerInfo.Config.Metadata["owner"] != testHostOwner {
		t.Fatalf("host consumer config was lost: %#v", consumerInfo.Config)
	}
}

func TestRouteWaitsForPubAckAndUsesStableDeduplication(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000001")
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[testPayload]())
	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "nats.media", Namespace: testNamespace, WireMode: nats.WireNative,
	})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithIDGenerator(constantGenerator{id: id}),
	)
	builder.RouteEvent(event, route)
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for range 2 {
		receipt, err := m.Publish(t.Context(), event, testPayload{JobID: 42})
		if err != nil || receipt.State != messenger.ReceiptBrokerConfirmed || receipt.MessageID != id {
			t.Fatalf("publish = %#v, %v", receipt, err)
		}
	}
	js, _ := jetstream.New(connection)
	stream, err := js.Stream(t.Context(), testStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream messages = %d, want 1", info.State.Msgs)
	}
}

func TestRoutePublishesPersistedEnvelopeWithOriginalMessageID(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000002")
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[testPayload]())
	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "nats.media", Namespace: testNamespace, WireMode: nats.WireNative,
	})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
		Source: "urn:service:outbox", Time: time.Unix(1_700_000_000, 0).UTC(),
		CorrelationID: id, ContentType: testContentType,
	}
	envelope, err := messenger.EncodeEventEnvelope(event, metadata, testPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for range 2 {
		receipt, publishErr := route.PublishEnvelope(t.Context(), envelope)
		if publishErr != nil || receipt.MessageID != id || receipt.State != messenger.ReceiptBrokerConfirmed {
			t.Fatalf("publish = %#v, %v", receipt, publishErr)
		}
	}
	js, _ := jetstream.New(connection)
	stream, err := js.Stream(t.Context(), testStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream messages = %d, want 1", info.State.Msgs)
	}
}

func TestEventTraceAndMiddlewareRoundTripAcrossWireModes(t *testing.T) {
	for _, wireMode := range []nats.WireMode{
		nats.WireNative,
		nats.WireCloudEventsStructured,
		nats.WireCloudEventsBinary,
	} {
		t.Run(string(wireMode), func(t *testing.T) {
			testEventTraceAndMiddlewareRoundTrip(t, wireMode)
		})
	}
}

func testEventTraceAndMiddlewareRoundTrip(t *testing.T, wireMode nats.WireMode) {
	t.Helper()
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[testPayload]())
	order := make([]string, 0, 5)
	handled := make(chan messenger.Message[testPayload], 1)
	observed := make(chan messenger.Observation, 2)
	config := testHandlerConfig("trace-" + string(wireMode))
	config.WireMode = wireMode
	config.Propagator = testTracePropagator{}
	config.Middlewares = []messenger.Middleware{
		func(ctx context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
			order = append(order, "first.before")
			err := next(ctx)
			order = append(order, "first.after")
			return err
		},
		func(ctx context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
			order = append(order, "second.before")
			err := next(ctx)
			order = append(order, "second.after")
			return err
		},
	}
	config.Observers = []messenger.Observer{observerFunc(func(
		_ context.Context,
		observation messenger.Observation,
	) {
		observed <- observation
	})}
	consumer, err := nats.NewEventConsumer(
		connection,
		openInbox(t),
		event,
		func(ctx context.Context, message messenger.Message[testPayload]) error {
			handled <- message
			if ctx.Value(traceContextKey{}) != testTraceParent+"|"+testTraceState {
				return errors.New("trace context was not extracted")
			}
			order = append(order, "handler")
			return nil
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)

	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "trace.route", Namespace: testNamespace, WireMode: wireMode,
	})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithContextPropagator(testTracePropagator{}),
	)
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	receipt, err := instance.Publish(t.Context(), event, testPayload{JobID: 42})
	if err != nil || receipt.State != messenger.ReceiptBrokerConfirmed {
		t.Fatalf("publish = %#v, %v", receipt, err)
	}
	message := <-handled
	handlerObservation := <-observed
	ackObservation := <-observed
	if message.Metadata.Headers["traceparent"] == "" || message.Metadata.Headers["tracestate"] != testTraceState {
		t.Fatalf("message headers = %#v", message.Metadata.Headers)
	}
	wantOrder := "[first.before second.before handler second.after first.after]"
	if fmt.Sprint(order) != wantOrder {
		t.Fatalf("middleware order = %v, want %s", order, wantOrder)
	}
	if handlerObservation.Operation != messenger.OperationHandle ||
		handlerObservation.MessageID != receipt.MessageID || handlerObservation.ConsumerID != config.ConsumerID ||
		handlerObservation.Attempt != 1 || handlerObservation.Duplicate || handlerObservation.Err != nil {
		t.Fatalf("handler observation = %#v", handlerObservation)
	}
	if ackObservation.Operation != messenger.Operation("broker_ack") || ackObservation.MessageID != receipt.MessageID ||
		ackObservation.ConsumerID != config.ConsumerID || ackObservation.Err != nil {
		t.Fatalf("ack observation = %#v", ackObservation)
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerInboxAcknowledgesDuplicatesAfterOneHandlerCommit(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openInbox(t)
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[testPayload]())
	var calls atomic.Int32
	var contextLineage atomic.Bool
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(ctx context.Context, message messenger.Message[testPayload]) error {
			metadata, ok := messenger.MetadataFromContext(ctx)
			contextLineage.Store(ok && metadata.ID == message.Metadata.ID)
			calls.Add(1)
			return nil
		},
		testHandlerConfig(testConsumerID),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)

	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000010")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindCommand, Name: "media.processor", SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: testContentType,
	}
	data, err := messenger.EncodeCommandEnvelope(command, metadata, testPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	js, _ := jetstream.New(connection)
	subject, _ := nats.Subject(testNamespace, command.Info())
	if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID("broker-delivery-1")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID("broker-delivery-2")); err != nil {
		t.Fatalf("publish duplicate: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	if !contextLineage.Load() {
		t.Fatal("handler context did not preserve message lineage")
	}
	waitFor(t, func() bool {
		jsConsumer, err := js.Consumer(t.Context(), testStreamName, testConsumerID)
		if err != nil {
			return false
		}
		info, err := jsConsumer.Info(t.Context())
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func TestConsumerPermanentFailurePublishesDLQBeforeConfirmedAck(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openInbox(t)
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-dlq-worker")
	config.MaxAttempts = 1
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(context.Context, messenger.Message[testPayload]) error {
			return messenger.Permanent(errors.New("unsupported media"))
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	subscription, err := connection.SubscribeSync("test.dlq")
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000020", "permanent-1")
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	var record nats.DLQRecord
	if err := json.Unmarshal(dlqMessage.Data, &record); err != nil {
		t.Fatalf("decode DLQ: %v", err)
	}
	if record.FailureKind != testPermanentFailure || len(record.Envelope) == 0 ||
		record.ConsumerID != "media-dlq-worker" {
		t.Fatalf("DLQ record = %#v", record)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	key := inbox.Key{
		ConsumerID: config.ConsumerID,
		Source:     testProducerSource,
		MessageID:  mustID(t, "018f4f2c-4a00-7000-8000-000000000020"),
	}
	result, err := store.ProcessAttempt(
		t.Context(), key, inbox.FingerprintEnvelope(record.Envelope), 1, func(context.Context) error { return nil },
	)
	if err != nil || result.Attempt != 1 {
		t.Fatalf("fresh attempt after terminal hand-off = %#v, %v", result, err)
	}
	cancel()
	<-runDone
}

func TestConsumerPublishesDLQRecordLargerThanSourceStreamLimit(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	command := messenger.MustCommand("media.large-dlq", 1, messenger.Bytes())
	config := testHandlerConfig("media-large-dlq-worker")
	config.MaxAttempts = 1
	consumer, err := nats.NewCommandConsumer(
		connection,
		openInbox(t),
		command,
		func(context.Context, messenger.Message[[]byte]) error {
			return messenger.Permanent(errors.New("unsupported large payload"))
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	subscription, err := connection.SubscribeSync(config.DLQSubject)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)

	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000023")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindCommand, Name: command.Info().Name, SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: command.Info().ContentType,
	}
	envelope, err := messenger.EncodeCommandEnvelope(command, metadata, bytes.Repeat([]byte{0xab}, 600<<10))
	if err != nil {
		t.Fatalf("encode large command: %v", err)
	}
	if len(envelope) >= messenger.DefaultMaxEnvelopeBytes {
		t.Fatalf("source envelope = %d bytes, limit = %d", len(envelope), messenger.DefaultMaxEnvelopeBytes)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	subject, err := nats.Subject(testNamespace, command.Info())
	if err != nil {
		t.Fatalf("subject: %v", err)
	}
	if _, err := js.Publish(t.Context(), subject, envelope, jetstream.WithMsgID("large-dlq-1")); err != nil {
		t.Fatalf("publish source: %v", err)
	}
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	if len(dlqMessage.Data) <= messenger.DefaultMaxEnvelopeBytes {
		t.Fatalf("DLQ record = %d bytes, want more than source stream limit", len(dlqMessage.Data))
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode large DLQ record: %v", err)
	}
	if len(record.Envelope) != len(envelope) || record.FailureKind != testPermanentFailure {
		t.Fatalf("large DLQ record = %#v", record)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestEventConsumerRejectsCloudEventDataEncodingConflict(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	const contentType = "application/vnd.example.bytes"
	textCodec, err := messenger.CustomCodec(
		contentType,
		messenger.DataText,
		func(value []byte) ([]byte, error) { return append([]byte(nil), value...), nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil },
	)
	if err != nil {
		t.Fatalf("text codec: %v", err)
	}
	binaryCodec, err := messenger.CustomCodec(
		contentType,
		messenger.DataBinary,
		func(value []byte) ([]byte, error) { return append([]byte(nil), value...), nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil },
	)
	if err != nil {
		t.Fatalf("binary codec: %v", err)
	}
	produced := messenger.MustEvent("media.encoding-conflict", 1, textCodec)
	consumed := messenger.MustEvent("media.encoding-conflict", 1, binaryCodec)
	config := testHandlerConfig("media-encoding-conflict-worker")
	config.WireMode = nats.WireCloudEventsBinary
	var calls atomic.Int32
	consumer, err := nats.NewEventConsumer(
		connection,
		openInbox(t),
		consumed,
		func(context.Context, messenger.Message[[]byte]) error {
			calls.Add(1)
			return nil
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	subscription, err := connection.SubscribeSync(config.DLQSubject)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)

	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000024")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: produced.Info().Name, SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: contentType,
	}
	envelope, err := messenger.EncodeEventEnvelope(produced, metadata, []byte("job 42 done"))
	if err != nil {
		t.Fatalf("encode producer event: %v", err)
	}
	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "encoding-conflict", Namespace: testNamespace, WireMode: nats.WireCloudEventsBinary,
	})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if _, err := route.PublishEnvelope(t.Context(), envelope); err != nil {
		t.Fatalf("publish: %v", err)
	}
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ: %v", err)
	}
	if record.FailureKind != "decode" || calls.Load() != 0 {
		t.Fatalf("encoding conflict record = %#v, handler calls = %d", record, calls.Load())
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerRunRejectsUndersizedDLQStream(t *testing.T) {
	connection := startJetStream(t)
	command := messenger.MustCommand("media.small-dlq", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-small-dlq-worker")
	_, err := nats.ApplyTopology(t.Context(), connection, nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams: []nats.StreamSpec{
			nats.DevStream(testStreamName, "test.command.>"),
			nats.DevStream(testDLQStreamName, config.DLQSubject),
		},
	})
	if err != nil {
		t.Fatalf("apply topology: %v", err)
	}
	consumer, err := nats.NewCommandConsumer(
		connection,
		openInbox(t),
		command,
		func(context.Context, messenger.Message[testPayload]) error { return nil },
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if err := consumer.Run(t.Context()); !errors.Is(err, nats.ErrTopologyDrift) {
		t.Fatalf("undersized DLQ run error = %v", err)
	}
}

func TestConsumerRunRejectsInsufficientNATSMaxPayload(t *testing.T) {
	connection := startJetStreamWithMaxPayload(t, messenger.DefaultMaxEnvelopeBytes)
	command := messenger.MustCommand("media.small-server-payload", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-small-server-payload-worker")
	_, err := nats.ApplyTopology(t.Context(), connection, nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams: []nats.StreamSpec{
			nats.DevStream(testStreamName, "test.command.>"),
			nats.DevDLQStream(testDLQStreamName, config.DLQSubject),
		},
	})
	if err != nil {
		t.Fatalf("apply topology: %v", err)
	}
	consumer, err := nats.NewCommandConsumer(
		connection,
		openInbox(t),
		command,
		func(context.Context, messenger.Message[testPayload]) error { return nil },
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if err := consumer.Run(t.Context()); !errors.Is(err, nats.ErrTopologyDrift) {
		t.Fatalf("insufficient NATS max payload run error = %v", err)
	}
}

func TestConsumerIdentityConflictPublishesValidDLQAndStopsRedelivery(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openInbox(t)
	command := messenger.MustCommand("media.identity-conflict", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-identity-conflict-worker")
	const maxAttempts uint64 = 3
	config.MaxAttempts = int(maxAttempts)
	var calls atomic.Int32
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(context.Context, messenger.Message[testPayload]) error {
			calls.Add(1)
			return nil
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000021")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindCommand, Name: command.Info().Name, SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: testContentType,
	}
	original, err := messenger.EncodeCommandEnvelope(command, metadata, testPayload{JobID: 41})
	if err != nil {
		t.Fatalf("encode original: %v", err)
	}
	conflicting, err := messenger.EncodeCommandEnvelope(command, metadata, testPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode conflict: %v", err)
	}
	key := inbox.Key{ConsumerID: config.ConsumerID, Source: metadata.Source, MessageID: id}
	seedFailure := errors.New("seed failed attempt")
	result, err := store.ProcessAttempt(
		t.Context(), key, inbox.FingerprintEnvelope(original), maxAttempts,
		func(context.Context) error { return seedFailure },
	)
	if result.Attempt != 1 || !errors.Is(err, seedFailure) {
		t.Fatalf("seed attempt = %#v, %v", result, err)
	}
	subscription, err := connection.SubscribeSync(config.DLQSubject)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	js, _ := jetstream.New(connection)
	subject, _ := nats.Subject(testNamespace, command.Info())
	if _, err := js.Publish(t.Context(), subject, conflicting, jetstream.WithMsgID("identity-conflict-1")); err != nil {
		t.Fatalf("publish conflict: %v", err)
	}
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ: %v", err)
	}
	if record.Attempt != 1 || record.FailureKind != "identity_conflict" {
		t.Fatalf("DLQ record = %#v", record)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want 0", calls.Load())
	}
	_, err = store.ProcessAttempt(
		t.Context(), key, inbox.FingerprintEnvelope(conflicting), maxAttempts,
		func(context.Context) error {
			calls.Add(1)
			return nil
		},
	)
	if !errors.Is(err, inbox.ErrFingerprintConflict) || calls.Load() != 0 {
		t.Fatalf("preserved identity conflict = %v, handler calls = %d", err, calls.Load())
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerDeadLettersExpandedBinaryCloudEventHeaders(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	event := messenger.MustEvent("media.large-headers", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-large-headers-worker")
	config.WireMode = nats.WireCloudEventsBinary
	config.MaxAttempts = 1
	var calls atomic.Int32
	consumer, err := nats.NewEventConsumer(
		connection,
		openInbox(t),
		event,
		func(context.Context, messenger.Message[testPayload]) error {
			calls.Add(1)
			return messenger.Permanent(errors.New("unsupported headers"))
		},
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	subscription, err := connection.SubscribeSync(config.DLQSubject)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000022")
	headerName := "large"
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: event.Info().Name, SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: testContentType,
		Headers: map[string]string{
			headerName: strings.Repeat("x", messenger.DefaultMaxHeaderBytes-len(headerName)),
		},
	}
	envelope, err := messenger.EncodeEventEnvelope(event, metadata, testPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "large-headers", Namespace: testNamespace, WireMode: nats.WireCloudEventsBinary,
	})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if _, err := route.PublishEnvelope(t.Context(), envelope); err != nil {
		t.Fatalf("publish: %v", err)
	}
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ: %v", err)
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ: %v", err)
	}
	var expandedHeaders string
	for key, values := range record.OriginalHeaders {
		if strings.EqualFold(key, "Ce-Gmheaders") && len(values) > 0 {
			expandedHeaders = values[0]
			break
		}
	}
	if len(expandedHeaders) <= messenger.DefaultMaxHeaderBytes {
		t.Fatalf("encoded application headers = %d bytes, want more than %d", len(expandedHeaders), messenger.DefaultMaxHeaderBytes)
	}
	plan, err := nats.PlanDLQReplay(record)
	if err != nil || plan.MessageID != id.String() {
		t.Fatalf("replay plan = %#v, %v", plan, err)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerRetryAfterDelaysSecondAttempt(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openInbox(t)
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[testPayload]())
	attempts := make(chan time.Time, 2)
	var calls atomic.Int32
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(context.Context, messenger.Message[testPayload]) error {
			attempts <- time.Now()
			if calls.Add(1) == 1 {
				return messenger.RetryAfter(errors.New("busy"), 150*time.Millisecond)
			}
			return nil
		},
		testHandlerConfig("media-retry-worker"),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000030", "retry-1")
	first := <-attempts
	second := <-attempts
	if delay := second.Sub(first); delay < 120*time.Millisecond {
		t.Fatalf("retry delay = %s", delay)
	}
	cancel()
	<-runDone
}

func startJetStream(t *testing.T) *natsio.Conn {
	t.Helper()
	return startJetStreamWithMaxPayload(t, nats.DefaultMaxDLQMessageBytes)
}

func startJetStreamWithMaxPayload(t *testing.T, maxPayload int32) *natsio.Conn {
	t.Helper()
	instance, err := server.NewServer(&server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		MaxPayload: maxPayload,
	})
	if err != nil {
		t.Fatalf("new NATS server: %v", err)
	}
	instance.Start()
	if !instance.ReadyForConnections(10 * time.Second) {
		instance.Shutdown()
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() {
		instance.Shutdown()
		instance.WaitForShutdown()
	})
	connection, err := natsio.Connect(instance.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(connection.Close)
	return connection
}

func ensureTestStream(t *testing.T, connection *natsio.Conn) {
	t.Helper()
	_, err := nats.ApplyTopology(t.Context(), connection, nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams: []nats.StreamSpec{
			nats.DevStream(testStreamName, "test.command.>", "test.event.>"),
			nats.DevDLQStream(testDLQStreamName, "test.dlq"),
		},
	})
	if err != nil {
		t.Fatalf("apply topology: %v", err)
	}
}

func openInbox(t *testing.T) *inbox.Store {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open inbox: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = database.Close() })
	if err := inboxsqlite.Migrate(t.Context(), database); err != nil {
		t.Fatalf("migrate inbox: %v", err)
	}
	store, err := inboxsqlite.New(database)
	if err != nil {
		t.Fatalf("new inbox: %v", err)
	}
	return store
}

func testHandlerConfig(consumerID string) nats.HandlerConfig {
	return nats.HandlerConfig{
		Stream: testStreamName, Namespace: testNamespace, ConsumerID: consumerID,
		WireMode: nats.WireNative, Concurrency: 1, Timeout: time.Second,
		MaxAttempts: 3, BaseRetry: 10 * time.Millisecond, MaxRetry: time.Second,
		AckWait: 2 * time.Second, DLQSubject: "test.dlq", Replicas: 1,
	}
}

func publishCommand(
	t *testing.T,
	connection *natsio.Conn,
	descriptor messenger.Command[testPayload],
	idText string,
	brokerID string,
) {
	t.Helper()
	id := mustID(t, idText)
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindCommand, Name: descriptor.Info().Name, SchemaVersion: 1,
		Source: testProducerSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: testContentType,
	}
	data, err := messenger.EncodeCommandEnvelope(descriptor, metadata, testPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	js, _ := jetstream.New(connection)
	subject, _ := nats.Subject(testNamespace, descriptor.Info())
	if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID(brokerID)); err != nil {
		t.Fatalf("publish command: %v", err)
	}
}

func waitReady(t *testing.T, consumer *nats.Consumer) {
	t.Helper()
	waitFor(t, func() bool { return consumer.Readiness(t.Context()) == nil })
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustID(t *testing.T, value string) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID(value)
	if err != nil {
		t.Fatalf("parse message ID: %v", err)
	}
	return id
}
