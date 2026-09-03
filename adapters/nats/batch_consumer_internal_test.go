package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/assurrussa/gomessenger/internal/batchruntime"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNormalNATSBatchBoundaryIncludesCompletedPull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "typed sentinel", err: jetstream.ErrBatchCompleted, want: true},
		{name: "legacy formatted status", err: errors.New("nats: Batch Completed"), want: true},
		{name: "other server status", err: errors.New("nats: consumer deleted"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := normalNATSBatchBoundary(context.Background(), test.err); got != test.want {
				t.Fatalf("normalNATSBatchBoundary() = %t, want %t", got, test.want)
			}
		})
	}
}

const (
	testNATSSource  = "urn:test"
	testNATSSubject = "test.subject"
	testNATSCommand = "test.cmd"
)

func TestNATSBatchDecodedMessagesMatchActiveFingerprint(t *testing.T) {
	messageID, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000087")
	if err != nil {
		t.Fatal(err)
	}
	key := inbox.Key{ConsumerID: "batch-worker", Source: testNATSSource, MessageID: messageID}
	first := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("first"))}
	active := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("active"))}
	decoded, err := natsBatchDecodedMessages([]inbox.BatchItem{active}, map[inbox.BatchItem]decodedMessage{
		first:  {canonical: []byte("first")},
		active: {canonical: []byte("active")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(decoded[0].canonical); got != "active" {
		t.Fatalf("decoded payload = %q, want active fingerprint payload", got)
	}
}

func TestHasNATSBatchConflictingGeneration(t *testing.T) {
	t.Parallel()
	id, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	existing := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-1",
	}
	deliveries := []*natsBatchDelivery{existing}

	// Same (ID, Source) and same generation -> not conflicting
	sameGenCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-1",
	}
	if hasNATSBatchConflictingGeneration(deliveries, sameGenCandidate) {
		t.Fatal("expected candidate with same attempt generation to NOT be conflicting")
	}

	// Same (ID, Source) but different generation -> conflicting!
	diffGenCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-2",
	}
	if !hasNATSBatchConflictingGeneration(deliveries, diffGenCandidate) {
		t.Fatal("expected candidate with different attempt generation to be conflicting")
	}

	// Different ID -> not conflicting
	id2, _ := messenger.ParseMessageID("01991387-6880-7000-8000-000000000002")
	diffIDCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id2, Source: testNATSSource},
		},
		attemptGeneration: "gen-2",
	}
	if hasNATSBatchConflictingGeneration(deliveries, diffIDCandidate) {
		t.Fatal("expected candidate with different ID to NOT be conflicting")
	}
}

type testNATSBatchObserver struct {
	mu            sync.Mutex
	observations  []messenger.Observation
	onObservation func(messenger.Observation)
}

func (o *testNATSBatchObserver) Observe(_ context.Context, obs messenger.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observations = append(o.observations, obs)
	if o.onObservation != nil {
		o.onObservation(obs)
	}
}

func (o *testNATSBatchObserver) last() messenger.Observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.observations) == 0 {
		return messenger.Observation{}
	}
	return o.observations[len(o.observations)-1]
}

type testNATSBatchBackend struct {
	topLevelErr error
}

func (b *testNATSBatchBackend) Process(
	_ context.Context, _ inbox.Key, _ inbox.Fingerprint, _ inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, nil
}

func (b *testNATSBatchBackend) Prune(_ context.Context, _ time.Time, _ int) (int64, error) {
	return 0, nil
}

func (b *testNATSBatchBackend) ProcessAttempt(
	_ context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	_ uint64,
	_ inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{Attempt: 1}, nil
}

func (b *testNATSBatchBackend) ForgetAttempt(_ context.Context, _ inbox.Key, _ inbox.Fingerprint) error {
	return nil
}

func (b *testNATSBatchBackend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	_ uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	if b.topLevelErr != nil {
		return inbox.BatchProcessResult{}, b.topLevelErr
	}
	result, err := handler(ctx, items)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	expected := make([]messenger.BatchItemKey, len(items))
	for index, item := range items {
		expected[index] = messenger.BatchItemKey{Source: item.Key.Source, MessageID: item.Key.MessageID}
	}
	itemErrors, err := batchruntime.ValidateResult(expected, result)
	if err != nil {
		return inbox.BatchProcessResult{}, err
	}
	report := inbox.BatchProcessResult{Items: make([]inbox.BatchItemOutcome, len(items)), HandlerMessages: len(items)}
	for index, item := range items {
		kind, delay := batchruntime.Classify(itemErrors[index])
		outcome := inbox.BatchItemOutcome{
			Key: item.Key, Fingerprint: item.Fingerprint,
			Outcome: inbox.BatchRetry, Attempt: 1, Delay: delay, Err: itemErrors[index],
		}
		switch kind {
		case batchruntime.FailureSuccess:
			outcome.Outcome = inbox.BatchACK
		case batchruntime.FailurePermanent:
			outcome.Outcome = inbox.BatchDLQ
		case batchruntime.FailureDefer:
			outcome.Outcome = inbox.BatchDefer
		case batchruntime.FailureRetryAfter, batchruntime.FailureOrdinary:
		}
		report.Items[index] = outcome
	}
	return report, nil
}

func startInternalNATSServer(t *testing.T) (*natsio.Conn, func()) {
	t.Helper()
	instance, err := server.NewServer(&server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		MaxPayload: DefaultMaxDLQMessageBytes,
	})
	if err != nil {
		t.Fatalf("new NATS server: %v", err)
	}
	instance.Start()
	if !instance.ReadyForConnections(10 * time.Second) {
		instance.Shutdown()
		t.Fatal("NATS server not ready")
	}
	connection, err := natsio.Connect(instance.ClientURL())
	if err != nil {
		instance.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	cleanup := func() {
		connection.Close()
		instance.Shutdown()
		instance.WaitForShutdown()
	}
	return connection, cleanup
}

func TestNATSBatchStartupFatalCancelsRunContextBeforeWait(t *testing.T) {
	conn, cleanup := startInternalNATSServer(t)
	defer cleanup()

	command := messenger.MustCommand("batch.fatal.startup", 1, messenger.JSON[string]())
	subject, err := Subject("startup-fatal", command.Info())
	if err != nil {
		t.Fatal(err)
	}
	streamName := "BATCH_FATAL_STREAM"
	dlqSubject := "startup-fatal.dlq"
	if _, err := ApplyTopology(t.Context(), conn, Topology{
		SpecVersion: TopologySpecVersion,
		Streams: []StreamSpec{
			DevStream(streamName, subject),
			DevDLQStream("BATCH_FATAL_DLQ", dlqSubject),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Publish 2 messages so both workers pick up messages
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		id, err := messenger.ParseMessageID(fmt.Sprintf("018f4f2c-4a00-7000-8000-%012x", i))
		if err != nil {
			t.Fatal(err)
		}
		meta := messenger.Metadata{
			ID: id, Source: testNATSSource, Kind: messenger.KindCommand,
			Name: command.Info().Name, SchemaVersion: 1, Time: time.Now().UTC(),
			ContentType: testDLQContentType, CorrelationID: id,
		}
		data, err := messenger.EncodeCommandEnvelope(command, meta, "payload")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID(id.String())); err != nil {
			t.Fatal(err)
		}
	}

	worker1Started := make(chan struct{})
	failClosedErr := fmt.Errorf("%w: fatal test error", messenger.ErrInvalidBatchResult)
	var workerCount atomic.Int32

	store, _ := inbox.New(&testNATSBatchBackend{})
	consumer, err := NewBatchCommandConsumer(
		conn,
		store,
		command,
		func(ctx context.Context, _ []messenger.Message[string]) (messenger.BatchResult, error) {
			idx := workerCount.Add(1)
			if idx == 1 {
				close(worker1Started)
				// Worker 1 blocks until runContext is cancelled
				<-ctx.Done()
				return messenger.BatchResult{}, ctx.Err()
			}
			// Worker 2 waits for worker 1 to enter handler, then fails closed
			<-worker1Started
			return messenger.BatchResult{}, failClosedErr
		},
		HandlerConfig{
			Stream: streamName, Namespace: "startup-fatal", ConsumerID: "batch-fatal-worker",
			WireMode: WireNative, Concurrency: 2, Timeout: 5 * time.Second,
			FinalizationTimeout: 5 * time.Second, MaxAttempts: 3, BaseRetry: 10 * time.Millisecond,
			MaxRetry: time.Second, AckWait: 5 * time.Second, DLQSubject: dlqSubject,
		},
		messenger.BatchConfig{MaxMessages: 1, MaxWait: 50 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(t.Context())
	}()

	select {
	case err := <-runDone:
		if !errors.Is(err, messenger.ErrInvalidBatchResult) {
			t.Fatalf("expected ErrInvalidBatchResult, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: runBatch hung on fatal error during startup")
	}
}

//nolint:gocognit // TestProcessNATSBatchObservability covers the full batch error observability matrix.
func TestProcessNATSBatchObservability(t *testing.T) {
	conn, cleanup := startInternalNATSServer(t)
	defer cleanup()

	command := messenger.MustCommand("batch.obs.test", 1, messenger.JSON[string]())
	subject, err := Subject("obs-test", command.Info())
	if err != nil {
		t.Fatal(err)
	}
	streamName := "BATCH_OBS_STREAM"
	dlqSubject := "obs-test.dlq"
	if _, err := ApplyTopology(t.Context(), conn, Topology{
		SpecVersion: TopologySpecVersion,
		Streams: []StreamSpec{
			DevStream(streamName, subject),
			DevDLQStream("BATCH_OBS_DLQ", dlqSubject),
		},
	}); err != nil {
		t.Fatal(err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	publishMsg := func(idStr string) {
		id, err := messenger.ParseMessageID(idStr)
		if err != nil {
			t.Fatal(err)
		}
		meta := messenger.Metadata{
			ID: id, Source: testNATSSource, Kind: messenger.KindCommand,
			Name: command.Info().Name, SchemaVersion: 1, Time: time.Now().UTC(),
			ContentType: testDLQContentType, CorrelationID: id,
		}
		data, err := messenger.EncodeCommandEnvelope(command, meta, "payload")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID(id.String())); err != nil {
			t.Fatal(err)
		}
	}

	decode := func(data []byte, _ natsio.Header, _ time.Time) (decodedMessage, error) {
		canonical, err := messenger.CanonicalizeEnvelope(data)
		if err != nil {
			return decodedMessage{}, err
		}
		msg, err := messenger.DecodeCommand(command, canonical)
		if err != nil {
			return decodedMessage{}, err
		}
		return decodedMessage{
			metadata: msg.Metadata, canonical: canonical, value: msg,
		}, nil
	}

	pullOne := func(consumerID string) *natsio.Msg {
		_, err := js.CreateOrUpdateConsumer(t.Context(), streamName, jetstream.ConsumerConfig{
			Durable:   consumerID,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatal(err)
		}
		sub, err := legacy.PullSubscribe(subject, "",
			natsio.Bind(streamName, consumerID), natsio.ManualAck())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		msgs, err := sub.Fetch(1, natsio.MaxWait(2*time.Second))
		if err != nil || len(msgs) == 0 {
			t.Fatalf("Fetch() err = %v, len = %d", err, len(msgs))
		}
		return msgs[0]
	}

	newConsumer := func(obs *testNATSBatchObserver, backend *testNATSBatchBackend, consumerID string) *Consumer {
		store, _ := inbox.New(backend)
		config := HandlerConfig{
			ConsumerID: consumerID, Observers: []messenger.Observer{obs},
			BaseRetry: 50 * time.Millisecond, MaxRetry: time.Second, MaxAttempts: 3,
		}
		applyConsumerDefaults(&config)
		_ = applyObservabilityDefaults(&config)
		return &Consumer{
			config:      config,
			maxAttempts: uint64(config.MaxAttempts), //nolint:gosec // Configuration specifies positive attempts.
			descriptor:  command.Info(),
			store:       store,
			decode:      decode,
			clock:       time.Now,
		}
	}

	t.Run("top-level ordinary error", func(t *testing.T) {
		publishMsg("018f4f2c-4a00-7000-8000-0000000000a1")
		msg := pullOne("obs-consumer-1")
		obs := &testNATSBatchObserver{}
		consumer := newConsumer(obs, &testNATSBatchBackend{topLevelErr: errors.New("db error")}, "obs-consumer-1")
		batch := &natsBatch{
			startedAt: time.Now(),
			heartbeat: newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{
				consumer.decodeNATSBatchDelivery(msg),
			},
		}
		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err != nil {
			t.Fatalf("processNATSBatch error = %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchRetries != 1 {
			t.Fatalf("BatchRetries = %d, want 1", o.BatchRetries)
		}
		if o.RetryDelay <= 0 {
			t.Fatalf("RetryDelay = %v, want > 0", o.RetryDelay)
		}
		if o.BatchACKs != 0 || o.BatchDeferrals != 0 {
			t.Fatalf("unexpected ACKs=%d or Deferrals=%d", o.BatchACKs, o.BatchDeferrals)
		}
	})

	t.Run("top-level RetryAfter", func(t *testing.T) {
		publishMsg("018f4f2c-4a00-7000-8000-0000000000a2")
		msg := pullOne("obs-consumer-2")
		obs := &testNATSBatchObserver{}
		consumer := newConsumer(obs, &testNATSBatchBackend{
			topLevelErr: messenger.RetryAfter(errors.New("retry error"), 5*time.Second),
		}, "obs-consumer-2")
		batch := &natsBatch{
			startedAt: time.Now(),
			heartbeat: newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{
				consumer.decodeNATSBatchDelivery(msg),
			},
		}
		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err != nil {
			t.Fatalf("processNATSBatch error = %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchRetries != 1 {
			t.Fatalf("BatchRetries = %d, want 1", o.BatchRetries)
		}
		if o.RetryDelay != 5*time.Second {
			t.Fatalf("RetryDelay = %v, want 5s", o.RetryDelay)
		}
	})

	t.Run("top-level DeferAfter", func(t *testing.T) {
		publishMsg("018f4f2c-4a00-7000-8000-0000000000a3")
		msg := pullOne("obs-consumer-3")
		obs := &testNATSBatchObserver{}
		consumer := newConsumer(obs, &testNATSBatchBackend{
			topLevelErr: messenger.DeferAfter(errors.New("defer error"), 3*time.Second),
		}, "obs-consumer-3")
		batch := &natsBatch{
			startedAt: time.Now(),
			heartbeat: newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{
				consumer.decodeNATSBatchDelivery(msg),
			},
		}
		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err != nil {
			t.Fatalf("processNATSBatch error = %v", err)
		}
		o := obs.last()
		if o.Err == nil {
			t.Fatal("expected non-nil observation Err")
		}
		if o.BatchDeferrals != 1 {
			t.Fatalf("BatchDeferrals = %d, want 1", o.BatchDeferrals)
		}
		if o.BatchRetries != 0 {
			t.Fatalf("BatchRetries = %d, want 0", o.BatchRetries)
		}
		if o.RetryDelay != 3*time.Second {
			t.Fatalf("RetryDelay = %v, want 3s", o.RetryDelay)
		}
	})
}

func TestCollectNATSBatchExactMaxBytesFlushesImmediately(t *testing.T) {
	conn, cleanup := startInternalNATSServer(t)
	defer cleanup()

	command := messenger.MustCommand("test.exact.bytes", 1, messenger.JSON[string]())
	subject, err := Subject("exact-bytes", command.Info())
	if err != nil {
		t.Fatal(err)
	}
	streamName := "EXACT_BYTES"
	dlqSubject := "exact-bytes.dlq"
	if _, err := ApplyTopology(t.Context(), conn, Topology{
		SpecVersion: TopologySpecVersion,
		Streams: []StreamSpec{
			DevStream(streamName, subject),
			DevDLQStream("EXACT_BYTES_DLQ", dlqSubject),
		},
	}); err != nil {
		t.Fatal(err)
	}

	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-0000000000e1")
	if err != nil {
		t.Fatal(err)
	}
	meta := messenger.Metadata{
		ID: id, Source: testNATSSource, Kind: messenger.KindCommand,
		Name: command.Info().Name, SchemaVersion: 1, Time: time.Now().UTC(),
		ContentType: testDLQContentType, CorrelationID: id,
	}
	data, err := messenger.EncodeCommandEnvelope(command, meta, "hello")
	if err != nil {
		t.Fatal(err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(t.Context(), subject, data, jetstream.WithMsgID(id.String())); err != nil {
		t.Fatal(err)
	}

	consumerID := "exact-bytes-worker"
	_, err = js.CreateOrUpdateConsumer(t.Context(), streamName, jetstream.ConsumerConfig{
		Durable: consumerID, AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}

	legacy, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := legacy.PullSubscribe(subject, "", natsio.Bind(streamName, consumerID), natsio.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	decode := func(data []byte, _ natsio.Header, _ time.Time) (decodedMessage, error) {
		canonical, err := messenger.CanonicalizeEnvelope(data)
		if err != nil {
			return decodedMessage{}, err
		}
		msg, err := messenger.DecodeCommand(command, canonical)
		if err != nil {
			return decodedMessage{}, err
		}
		return decodedMessage{
			metadata: msg.Metadata, canonical: canonical, value: msg,
		}, nil
	}

	canonical, err := messenger.CanonicalizeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	exactBytes := len(canonical)

	consumer := &Consumer{
		config: HandlerConfig{
			ConsumerID: consumerID,
			AckWait:    30 * time.Second,
		},
		descriptor: command.Info(),
		decode:     decode,
		clock:      time.Now,
		batch: &batchConsumer{
			config: messenger.BatchConfig{
				MaxMessages: 10,
				MaxBytes:    exactBytes,
				MaxWait:     5 * time.Second,
			},
		},
	}

	start := time.Now()
	batch, err := consumer.collectNATSBatch(t.Context(), t.Context(), sub)
	duration := time.Since(start)
	if err != nil {
		t.Fatalf("collectNATSBatch error = %v", err)
	}
	if batch == nil || len(batch.deliveries) != 1 {
		t.Fatalf("expected batch with 1 delivery, got %v", batch)
	}
	if duration >= 2*time.Second {
		t.Fatalf("collectNATSBatch took %v, want immediate flush", duration)
	}
}

func TestNATSBatchFinalizeDLQFailureEmitsObservations(t *testing.T) {
	messageID, err := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000c1")
	if err != nil {
		t.Fatal(err)
	}
	obs := &testNATSBatchObserver{}
	config := HandlerConfig{
		ConsumerID: "nats-dlq-test",
		Observers:  []messenger.Observer{obs},
		DLQSubject: "test.dlq",
	}
	applyConsumerDefaults(&config)
	_ = applyObservabilityDefaults(&config)

	consumer := &Consumer{
		config:     config,
		descriptor: messenger.DescriptorInfo{Kind: messenger.KindEvent, Name: "test.event", SchemaVersion: 1},
		clock:      time.Now,
	}

	metadata := messenger.Metadata{
		ID: messageID, CorrelationID: messageID, Source: "urn:test",
		Kind: messenger.KindEvent, Name: "test.event", SchemaVersion: 1,
	}
	delivery := &natsBatchDelivery{
		broker:  &natsio.Msg{Subject: testNATSSubject, Header: natsio.Header{"Nats-Msg-Id": []string{"id-1"}}, Data: []byte("raw")},
		decoded: decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000c1"}`)},
	}

	batch := &natsBatch{
		startedAt:  time.Now(),
		heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
		deliveries: []*natsBatchDelivery{delivery},
	}
	outcomes := []natsBatchFinalOutcome{
		{
			item: inbox.BatchItemOutcome{
				Outcome: inbox.BatchDLQ,
				Attempt: 3,
			},
			failureKind: "permanent",
			err:         errors.New("terminal business error"),
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	finRes := consumer.finalizeNATSBatch(ctx, batch, outcomes)
	if finRes.fatalErr == nil {
		t.Fatal("expected finalizeNATSBatch error on cancelled context, got nil")
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()

	var dlqObs *messenger.Observation
	for i := range obs.observations {
		if obs.observations[i].Operation == messenger.Operation("dlq_handoff") {
			dlqObs = &obs.observations[i]
		}
	}

	if dlqObs == nil {
		t.Fatal("expected dlq_handoff observation on DLQ failure")
	}
	if dlqObs.Err == nil {
		t.Fatal("expected non-nil Err in dlq_handoff observation")
	}
}

type testNATSBrokerMsg struct {
	ackCalls        atomic.Int32
	ackErr          error
	nakCalls        atomic.Int32
	nakErr          error
	nakErrs         []error
	inProgressCalls atomic.Int32
	inProgressErr   error
	mu              sync.Mutex
}

func (m *testNATSBrokerMsg) AckSync(_ ...natsio.AckOpt) error {
	m.ackCalls.Add(1)
	return m.ackErr
}

func (m *testNATSBrokerMsg) NakWithDelay(_ time.Duration, _ ...natsio.AckOpt) error {
	call := int(m.nakCalls.Add(1))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nakErr != nil {
		return m.nakErr
	}
	if call <= len(m.nakErrs) {
		return m.nakErrs[call-1]
	}
	return nil
}

func (m *testNATSBrokerMsg) InProgress(_ ...natsio.AckOpt) error {
	m.inProgressCalls.Add(1)
	return m.inProgressErr
}

type testNATSTxMarkerKey struct{}

type delayedNATSBatchBackend struct {
	testNATSBatchBackend
	delay time.Duration
}

func (b *delayedNATSBatchBackend) ProcessBatchAttempt(
	ctx context.Context,
	items []inbox.BatchItem,
	maxAttempts uint64,
	handler inbox.BatchHandler,
) (inbox.BatchProcessResult, error) {
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return inbox.BatchProcessResult{}, ctx.Err()
		}
	}
	txCtx := context.WithValue(ctx, testNATSTxMarkerKey{}, "sentinel-nats-tx-marker")
	return b.testNATSBatchBackend.ProcessBatchAttempt(txCtx, items, maxAttempts, handler)
}

func TestNATSBatchHandlerContextInheritsTransactionContextDeadlineAndCancellation(t *testing.T) {
	t.Run("handler inherits transaction context sentinel, deadline, and cancellation", func(t *testing.T) {
		backend := &delayedNATSBatchBackend{delay: 90 * time.Millisecond}
		store, _ := inbox.New(backend)
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID:          "nats-batch-test",
				Timeout:             80 * time.Millisecond,
				FinalizationTimeout: 40 * time.Millisecond,
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}
		messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d1")
		metadata := messenger.Metadata{
			ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
		}
		delivery := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d1"}`)},
		}
		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{delivery},
		}

		var (
			sawSentinel       bool
			sawDeadline       bool
			deadlineInherited bool
			sawCancellation   bool
		)

		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
			invoke: func(ctx context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				if val, ok := ctx.Value(testNATSTxMarkerKey{}).(string); ok && val == "sentinel-nats-tx-marker" {
					sawSentinel = true
				}
				handlerDeadline, hasDeadline := ctx.Deadline()
				sawDeadline = hasDeadline
				if hasDeadline {
					deadlineInherited = time.Until(handlerDeadline) <= 45*time.Millisecond
				}
				select {
				case <-ctx.Done():
					sawCancellation = true
					return messenger.BatchResult{}, ctx.Err()
				case <-time.After(200 * time.Millisecond):
					return messenger.BatchResult{
						Items: []messenger.BatchItemResult{
							{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
						},
					}, nil
				}
			},
		}

		streak := uint64(0)
		_ = consumer.processNATSBatch(t.Context(), batch, &streak)
		if !sawSentinel {
			t.Fatal("expected handler context to inherit sentinel value from transactionHandlerCtx")
		}
		if !sawDeadline {
			t.Fatal("expected handler context to have deadline")
		}
		if !deadlineInherited {
			t.Fatal("expected handler deadline to be inherited/bounded by transaction deadline")
		}
		if !sawCancellation {
			t.Fatal("expected transaction cancellation to be visible in handler")
		}
	})
}

func TestNATSBatchFinalizeNakWithDelayRetriesUntilSuccess(t *testing.T) {
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: "nats-batch-retry-test"},
		clock:  time.Now,
	}
	brokerMsg := &testNATSBrokerMsg{
		nakErrs: []error{errors.New("connection reset")},
	}
	messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d2")
	metadata := messenger.Metadata{
		ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
	}
	delivery := &natsBatchDelivery{
		broker:    &natsio.Msg{Subject: testNATSSubject},
		brokerMsg: brokerMsg,
		decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d2"}`)},
	}
	heartbeat := newNATSBatchHeartbeat(t.Context(), consumer)
	defer heartbeat.Stop()
	heartbeat.Add(delivery.brokerMessage())

	outcome := natsBatchFinalOutcome{
		item: inbox.BatchItemOutcome{Outcome: inbox.BatchRetry, Delay: time.Second},
	}

	finRes := consumer.finalizeNATSBatchNonDLQ(t.Context(), heartbeat, delivery, outcome)
	if finRes.fatalErr != nil {
		t.Fatalf("finalizeNATSBatchNonDLQ failed: %v", finRes.fatalErr)
	}
	if brokerMsg.nakCalls.Load() != 2 {
		t.Fatalf("nakCalls = %d, want 2 (1 retry)", brokerMsg.nakCalls.Load())
	}

	heartbeat.mu.Lock()
	_, stillInHeartbeat := heartbeat.messages[delivery.brokerMessage()]
	heartbeat.mu.Unlock()
	if stillInHeartbeat {
		t.Fatal("expected finalized delivery to be removed from heartbeat")
	}
}

func TestNATSBatchFinalizeNakWithDelayFailsOnContextCancellation(t *testing.T) {
	consumer := &Consumer{
		config: HandlerConfig{ConsumerID: "nats-batch-cancel-test"},
		clock:  time.Now,
	}
	brokerMsg := &testNATSBrokerMsg{
		nakErr: errors.New("fatal broker error"),
	}
	messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d3")
	metadata := messenger.Metadata{
		ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
	}
	delivery := &natsBatchDelivery{
		broker:    &natsio.Msg{Subject: testNATSSubject},
		brokerMsg: brokerMsg,
		decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d3"}`)},
	}
	heartbeat := newNATSBatchHeartbeat(t.Context(), consumer)
	defer heartbeat.Stop()
	heartbeat.Add(delivery.brokerMessage())

	outcome := natsBatchFinalOutcome{
		item: inbox.BatchItemOutcome{Outcome: inbox.BatchRetry, Delay: time.Second},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	finRes := consumer.finalizeNATSBatchNonDLQ(ctx, heartbeat, delivery, outcome)
	if finRes.fatalErr == nil {
		t.Fatal("expected error on context cancellation, got nil")
	}
}

func TestNATSBatchFinalizeAckErrorReflectedInBatchObservation(t *testing.T) {
	obs := &testNATSBatchObserver{}
	store, _ := inbox.New(&testNATSBatchBackend{})
	consumer := &Consumer{
		config: HandlerConfig{
			ConsumerID: "nats-batch-ack-fail",
			Observers:  []messenger.Observer{obs},
		},
		descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
		store:       store,
		maxAttempts: 3,
		clock:       time.Now,
	}
	brokerMsg := &testNATSBrokerMsg{
		ackErr: errors.New("ack network timeout"),
	}
	messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d4")
	metadata := messenger.Metadata{
		ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
	}
	delivery := &natsBatchDelivery{
		broker:    &natsio.Msg{Subject: testNATSSubject},
		brokerMsg: brokerMsg,
		decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d4"}`)},
	}
	batch := &natsBatch{
		startedAt:  time.Now(),
		heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
		deliveries: []*natsBatchDelivery{delivery},
	}
	consumer.batch = &batchConsumer{
		config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
		invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
				},
			}, nil
		},
	}

	streak := uint64(0)
	err := consumer.processNATSBatch(t.Context(), batch, &streak)
	if err != nil {
		t.Fatalf("expected nil error (non-fatal) from processNATSBatch when ACK fails, got: %v", err)
	}

	o := obs.last()
	if o.Err == nil {
		t.Fatal("expected observation.Err to reflect ACK finalization failure")
	}
}

func TestNATSBatchOperationHandleFilteredForUninvokedItems(t *testing.T) {
	t.Run("backend error before callback produces 0 OperationHandle", func(t *testing.T) {
		obs := &testNATSBatchObserver{}
		store, _ := inbox.New(&testNATSBatchBackend{topLevelErr: errors.New("backend failed before callback")})
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID: "nats-batch-obs-filter",
				Observers:  []messenger.Observer{obs},
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}
		brokerMsg := &testNATSBrokerMsg{}
		messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d5")
		metadata := messenger.Metadata{
			ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
		}
		delivery := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: brokerMsg,
			decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d5"}`)},
		}
		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{delivery},
		}
		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
		}

		streak := uint64(0)
		_ = consumer.processNATSBatch(t.Context(), batch, &streak)

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
			}
		}
		if handleCount != 0 {
			t.Fatalf("expected 0 OperationHandle observations on pre-callback error, got %d", handleCount)
		}
	})

	t.Run("mixed expired and active item produces 1 OperationHandle", func(t *testing.T) {
		obs := &testNATSBatchObserver{}
		store, _ := inbox.New(&testNATSBatchBackend{})
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID: "nats-batch-obs-filter",
				Observers:  []messenger.Observer{obs},
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}
		msgActive, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d6")
		metaActive := messenger.Metadata{
			ID: msgActive, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
		}
		deliveryActive := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded:   decodedMessage{metadata: metaActive, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d6"}`)},
		}

		msgExpired, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000d7")
		metaExpired := messenger.Metadata{
			ID: msgExpired, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand,
			SchemaVersion: 1, ExpiresAt: time.Now().Add(-time.Hour),
		}
		deliveryExpired := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded:   decodedMessage{metadata: metaExpired, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000d7"}`)},
		}

		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{deliveryExpired, deliveryActive},
		}
		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
			invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				if len(decoded) != 1 {
					t.Fatalf("expected 1 active message, got %d", len(decoded))
				}
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			},
		}

		streak := uint64(0)
		_ = consumer.processNATSBatch(t.Context(), batch, &streak)

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
			}
		}
		if handleCount != 1 {
			t.Fatalf("expected 1 OperationHandle observation for active item, got %d", handleCount)
		}
	})
}

func TestNATSBatchAckSyncErrorDoesNotAbortWorker(t *testing.T) {
	t.Run("AckSync failure is captured in observation without returning fatal worker error", func(t *testing.T) {
		obs := &testNATSBatchObserver{}
		store, _ := inbox.New(&testNATSBatchBackend{})
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID: "nats-batch-ack-err-test",
				Observers:  []messenger.Observer{obs},
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}
		messageID, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000e1")
		metadata := messenger.Metadata{
			ID: messageID, Source: testNATSSource, Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1,
		}
		ackErr := errors.New("nats: ack sync timeout")
		brokerMsg := &testNATSBrokerMsg{ackErr: ackErr}
		delivery := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: brokerMsg,
			decoded:   decodedMessage{metadata: metadata, canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000e1"}`)},
		}
		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{delivery},
		}
		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
			invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			},
		}

		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err != nil {
			t.Fatalf("processNATSBatch returned fatal error = %v, want nil (non-fatal ACK error)", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var batchObs messenger.Observation
		var sawBatchObs bool
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationBatchHandle {
				batchObs = o
				sawBatchObs = true
			}
		}
		if !sawBatchObs {
			t.Fatal("expected OperationBatchHandle observation")
		}
		if !errors.Is(batchObs.Err, ackErr) {
			t.Fatalf("batch observation Err = %v, want %v", batchObs.Err, ackErr)
		}
	})
}

func TestNATSBatchFailClosedEmitsOperationHandle(t *testing.T) {
	t.Run("top-level Permanent after invocation emits OperationHandle for active items", func(t *testing.T) {
		obs := &testNATSBatchObserver{}
		store, _ := inbox.New(&testNATSBatchBackend{})
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID: "nats-batch-fail-closed-test",
				Observers:  []messenger.Observer{obs},
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}

		id1, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000e2")
		id2, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000e3")
		d1 := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded: decodedMessage{
				metadata:  messenger.Metadata{ID: id1, Source: testNATSSource},
				canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000e2"}`),
			},
		}
		d2 := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded: decodedMessage{
				metadata:  messenger.Metadata{ID: id2, Source: testNATSSource},
				canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000e3"}`),
			},
		}
		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{d1, d2},
		}

		permErr := errors.New("poison pill permanent")
		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
			invoke: func(_ context.Context, _ []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, messenger.Permanent(permErr)
			},
		}

		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err == nil {
			t.Fatal("expected processNATSBatch to return error on Permanent")
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
				if !errors.Is(o.Err, permErr) {
					t.Fatalf("OperationHandle Err = %v, want %v", o.Err, permErr)
				}
			}
		}
		if handleCount != 2 {
			t.Fatalf("expected 2 OperationHandle observations, got %d", handleCount)
		}
	})

	t.Run("invalid exact-cover result emits OperationHandle with ErrInvalidBatchResult", func(t *testing.T) {
		obs := &testNATSBatchObserver{}
		store, _ := inbox.New(&testNATSBatchBackend{})
		consumer := &Consumer{
			config: HandlerConfig{
				ConsumerID: "nats-batch-invalid-cover-test",
				Observers:  []messenger.Observer{obs},
			},
			descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
			store:       store,
			maxAttempts: 3,
			clock:       time.Now,
		}

		id1, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000e4")
		id2, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000e5")
		d1 := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded: decodedMessage{
				metadata:  messenger.Metadata{ID: id1, Source: testNATSSource},
				canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000e4"}`),
			},
		}
		d2 := &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: &testNATSBrokerMsg{},
			decoded: decodedMessage{
				metadata:  messenger.Metadata{ID: id2, Source: testNATSSource},
				canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000e5"}`),
			},
		}
		batch := &natsBatch{
			startedAt:  time.Now(),
			heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
			deliveries: []*natsBatchDelivery{d1, d2},
		}

		consumer.batch = &batchConsumer{
			config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
			invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
				return messenger.BatchResult{
					Items: []messenger.BatchItemResult{
						{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
					},
				}, nil
			},
		}

		streak := uint64(0)
		err := consumer.processNATSBatch(t.Context(), batch, &streak)
		if err == nil {
			t.Fatal("expected processNATSBatch to return error on incomplete batch result")
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		var handleCount int
		for _, o := range obs.observations {
			if o.Operation == messenger.OperationHandle {
				handleCount++
				if !errors.Is(o.Err, messenger.ErrInvalidBatchResult) {
					t.Fatalf("OperationHandle Err = %v, want ErrInvalidBatchResult", o.Err)
				}
			}
		}
		if handleCount != 2 {
			t.Fatalf("expected 2 OperationHandle observations, got %d", handleCount)
		}
	})
}

type testBlockingNATSBrokerMsg struct {
	startedCount *atomic.Int32
	ackCalls     atomic.Int32
}

func (m *testBlockingNATSBrokerMsg) AckSync(opts ...natsio.AckOpt) error {
	m.ackCalls.Add(1)
	if m.startedCount != nil {
		m.startedCount.Add(1)
	}
	var ackCtx context.Context
	for _, opt := range opts {
		if cOpt, ok := opt.(natsio.ContextOpt); ok {
			ackCtx = cOpt.Context
		}
	}
	if ackCtx != nil {
		<-ackCtx.Done()
		return ackCtx.Err()
	}
	return nil
}

func (m *testBlockingNATSBrokerMsg) NakWithDelay(_ time.Duration, _ ...natsio.AckOpt) error {
	return nil
}

func (m *testBlockingNATSBrokerMsg) InProgress(_ ...natsio.AckOpt) error {
	return nil
}

func TestNATSBatchACKFinalizationObeysForceShutdown(t *testing.T) {
	store, _ := inbox.New(&testNATSBatchBackend{})
	consumer := &Consumer{
		config: HandlerConfig{
			ConsumerID: "nats-batch-ack-shutdown-test",
		},
		descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
		store:       store,
		maxAttempts: 3,
		clock:       time.Now,
	}

	const batchSize = 32
	var startedCount atomic.Int32
	deliveries := make([]*natsBatchDelivery, batchSize)
	brokerMsgs := make([]*testBlockingNATSBrokerMsg, batchSize)
	for i := 0; i < batchSize; i++ {
		id, _ := messenger.ParseMessageID(fmt.Sprintf("01991387-6880-7000-8000-%012d", i+1))
		bm := &testBlockingNATSBrokerMsg{startedCount: &startedCount}
		brokerMsgs[i] = bm
		deliveries[i] = &natsBatchDelivery{
			broker:    &natsio.Msg{Subject: testNATSSubject},
			brokerMsg: bm,
			decoded: decodedMessage{
				metadata:  messenger.Metadata{ID: id, Source: testNATSSource},
				canonical: []byte(fmt.Sprintf(`{"id":"%s"}`, id.String())),
			},
		}
	}

	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	batch := &natsBatch{
		startedAt:  time.Now(),
		heartbeat:  newNATSBatchHeartbeat(runCtx, consumer),
		deliveries: deliveries,
	}

	consumer.batch = &batchConsumer{
		config: messenger.BatchConfig{MaxMessages: batchSize, MaxBytes: 65536, MaxWait: 50 * time.Millisecond},
		invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			items := make([]messenger.BatchItemResult, len(decoded))
			for i, d := range decoded {
				items[i] = messenger.BatchItemResult{
					Key: messenger.BatchItemKey{Source: d.metadata.Source, MessageID: d.metadata.ID},
				}
			}
			return messenger.BatchResult{Items: items}, nil
		},
	}

	var batchHandleMessages int
	var handleObservations atomic.Int32
	obs := &testNATSBatchObserver{
		onObservation: func(o messenger.Observation) {
			switch o.Operation {
			case messenger.OperationBatchHandle:
				batchHandleMessages = o.BatchHandlerMessages
			case messenger.OperationHandle:
				handleObservations.Add(1)
			default:
			}
		},
	}
	consumer.config.Observers = []messenger.Observer{obs}

	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		streak := uint64(0)
		errCh <- consumer.processNATSBatch(runCtx, batch, &streak)
	}()

	// Wait for the first wave (16 workers) to enter AckSync.
	for startedCount.Load() < int32(batchAckParallelism) {
		if time.Since(start) > 2*time.Second {
			t.Fatalf("timed out waiting for %d workers to start AckSync, got %d", batchAckParallelism, startedCount.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Trigger force shutdown.
	cancel()

	select {
	case err := <-errCh:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("expected processNATSBatch to return error on cancelled context")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("processNATSBatch took too long (%v), want immediate bounded shutdown", elapsed)
		}
		if batchHandleMessages != batchSize {
			t.Fatalf("BatchHandlerMessages = %d, want %d", batchHandleMessages, batchSize)
		}
		if handles := handleObservations.Load(); handles != int32(batchSize) {
			t.Fatalf("OperationHandle observations = %d, want %d", handles, batchSize)
		}
		totalStarted := startedCount.Load()
		if totalStarted > int32(batchAckParallelism) {
			t.Fatalf("unstarted tasks started new timeouts after cancellation: %d started, max %d", totalStarted, batchAckParallelism)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("processNATSBatch did not finish boundedly within shutdown timeout")
	}
}

func TestNATSBatchAggregateDurationExcludesObserverFanOut(t *testing.T) {
	currentTime := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	fakeClock := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	var batchObsDuration time.Duration
	obs := &testNATSBatchObserver{
		onObservation: func(o messenger.Observation) {
			if o.Operation == messenger.OperationBatchHandle {
				batchObsDuration = o.Duration
			}
			timeMu.Lock()
			currentTime = currentTime.Add(100 * time.Millisecond)
			timeMu.Unlock()
		},
	}

	store, _ := inbox.New(&testNATSBatchBackend{})
	consumer := &Consumer{
		config: HandlerConfig{
			ConsumerID: "nats-batch-timing-test",
			Observers:  []messenger.Observer{obs},
		},
		descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
		store:       store,
		maxAttempts: 3,
		clock:       fakeClock,
	}

	id, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000b1")
	bm := &testBlockingNATSBrokerMsg{}
	delivery := &natsBatchDelivery{
		broker:    &natsio.Msg{Subject: testNATSSubject},
		brokerMsg: bm,
		decoded: decodedMessage{
			metadata:  messenger.Metadata{ID: id, Source: testNATSSource},
			canonical: []byte(fmt.Sprintf(`{"id":"%s"}`, id.String())),
		},
	}

	runCtx := t.Context()
	batch := &natsBatch{
		startedAt:  currentTime,
		heartbeat:  newNATSBatchHeartbeat(runCtx, consumer),
		deliveries: []*natsBatchDelivery{delivery},
	}

	consumer.batch = &batchConsumer{
		config: messenger.BatchConfig{MaxMessages: 1, MaxBytes: 65536, MaxWait: 50 * time.Millisecond},
		invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			timeMu.Lock()
			currentTime = currentTime.Add(25 * time.Millisecond)
			timeMu.Unlock()
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID}},
				},
			}, nil
		},
	}

	streak := uint64(0)
	_ = consumer.processNATSBatch(runCtx, batch, &streak)

	if batchObsDuration > 50*time.Millisecond {
		t.Fatalf("OperationBatchHandle.Duration (%v) included observer fan-out; expected ~25ms", batchObsDuration)
	}
}

func TestNATSBatchDLQConstructionFailureFailsClosed(t *testing.T) {
	store, _ := inbox.New(&testNATSBatchBackend{})
	consumer := &Consumer{
		config: HandlerConfig{
			ConsumerID: "nats-batch-dlq-headers-test",
			DLQSubject: "test.dlq",
		},
		descriptor:  messenger.DescriptorInfo{Kind: messenger.KindCommand, Name: testNATSCommand, SchemaVersion: 1},
		store:       store,
		maxAttempts: 3,
		clock:       time.Now,
	}

	headers := make(natsio.Header)
	for i := 0; i < 65; i++ {
		headers[fmt.Sprintf("X-Test-Header-%02d", i)] = []string{"val"}
	}
	id, _ := messenger.ParseMessageID("01991387-6880-7000-8000-0000000000f1")
	brokerMsg := &testNATSBrokerMsg{}
	delivery := &natsBatchDelivery{
		broker: &natsio.Msg{
			Subject: testNATSSubject,
			Header:  headers,
		},
		brokerMsg: brokerMsg,
		decoded: decodedMessage{
			metadata:  messenger.Metadata{ID: id, Source: testNATSSource},
			canonical: []byte(`{"id":"01991387-6880-7000-8000-0000000000f1"}`),
		},
	}
	batch := &natsBatch{
		startedAt:  time.Now(),
		heartbeat:  newNATSBatchHeartbeat(t.Context(), consumer),
		deliveries: []*natsBatchDelivery{delivery},
	}

	consumer.batch = &batchConsumer{
		config: messenger.BatchConfig{MaxMessages: 10, MaxBytes: 1024, MaxWait: 50 * time.Millisecond},
		invoke: func(_ context.Context, decoded []decodedMessage) (messenger.BatchResult, error) {
			return messenger.BatchResult{
				Items: []messenger.BatchItemResult{
					{
						Key: messenger.BatchItemKey{Source: decoded[0].metadata.Source, MessageID: decoded[0].metadata.ID},
						Err: messenger.Permanent(errors.New("poison pill permanent")),
					},
				},
			}, nil
		},
	}

	streak := uint64(0)
	err := consumer.processNATSBatch(t.Context(), batch, &streak)
	if err == nil {
		t.Fatal("expected processNATSBatch to return error on DLQ construction failure, got nil")
	}
	if !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("expected error wrapping ErrInvalidMessage, got %v", err)
	}
	if brokerMsg.ackCalls.Load() != 0 {
		t.Fatalf("delivery was unexpectedly acknowledged: %d ack calls", brokerMsg.ackCalls.Load())
	}
}
