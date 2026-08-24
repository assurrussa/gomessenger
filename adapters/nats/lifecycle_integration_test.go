package nats_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

func TestConsumerHeartbeatPreventsLongHandlerRedelivery(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openConcurrentInbox(t)
	command := messenger.MustCommand("media.heartbeat", 1, messenger.JSON[testPayload]())
	var calls atomic.Int32

	config := testHandlerConfig("media-heartbeat-worker")
	config.AckWait = 150 * time.Millisecond
	config.Timeout = time.Second
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(ctx context.Context, _ messenger.Message[testPayload]) error {
			calls.Add(1)
			select {
			case <-time.After(450 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000040", "heartbeat-1")

	js, _ := jetstream.New(connection)
	waitFor(t, func() bool {
		brokerConsumer, brokerErr := js.Consumer(t.Context(), testStreamName, config.ConsumerID)
		if brokerErr != nil {
			return false
		}
		info, infoErr := brokerConsumer.Info(t.Context())
		return infoErr == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
	time.Sleep(2 * config.AckWait)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	brokerConsumer, err := js.Consumer(t.Context(), testStreamName, config.ConsumerID)
	if err != nil {
		t.Fatalf("broker consumer: %v", err)
	}
	info, err := brokerConsumer.Info(t.Context())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.NumRedelivered != 0 {
		t.Fatalf("redeliveries = %d, want 0", info.NumRedelivered)
	}
	if info.Config.MaxDeliver != -1 {
		t.Fatalf("broker max deliver = %d, want unlimited", info.Config.MaxDeliver)
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerBeginDrainBeforeRunPreventsStartup(t *testing.T) {
	connection := startJetStream(t)
	store := openConcurrentInbox(t)
	command := messenger.MustCommand("media.pre-drained", 1, messenger.JSON[testPayload]())
	var calls atomic.Int32
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(context.Context, messenger.Message[testPayload]) error {
			calls.Add(1)
			return nil
		},
		testHandlerConfig("media-pre-drained-worker"),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	consumer.BeginDrain()
	if err := consumer.Run(t.Context()); err != nil {
		t.Fatalf("run pre-drained consumer: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d", calls.Load())
	}
	if err := consumer.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := consumer.Run(t.Context()); !errors.Is(err, nats.ErrConsumerClosed) {
		t.Fatalf("second run error = %v", err)
	}
}

func TestRuntimeShutdownBeforeRunClosesConsumer(t *testing.T) {
	connection := startJetStream(t)
	command := messenger.MustCommand("media.never-started", 1, messenger.JSON[testPayload]())
	consumer, err := nats.NewCommandConsumer(
		connection,
		openConcurrentInbox(t),
		command,
		func(context.Context, messenger.Message[testPayload]) error { return nil },
		testHandlerConfig("media-never-started-worker"),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	builder := messenger.NewBuilder(messenger.WithSource(testProducerSource))
	builder.Use("consumer.media-never-started", consumer)
	_, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}
	shutdownContext, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownContext); err != nil {
		t.Fatalf("shutdown before run: %v", err)
	}
	if err := consumer.Run(t.Context()); !errors.Is(err, nats.ErrConsumerClosed) {
		t.Fatalf("run after shutdown = %v", err)
	}
}

func TestConsumerRejectsDLQOnItsInputSubject(t *testing.T) {
	connection := startJetStream(t)
	command := messenger.MustCommand("media.self-dlq", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-self-dlq-worker")
	inputSubject, err := nats.Subject(config.Namespace, command.Info())
	if err != nil {
		t.Fatalf("input subject: %v", err)
	}
	config.DLQSubject = inputSubject
	if _, err := nats.NewCommandConsumer(
		connection,
		openConcurrentInbox(t),
		command,
		func(context.Context, messenger.Message[testPayload]) error { return nil },
		config,
	); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("self-DLQ error = %v", err)
	}
}

func TestConsumerRejectsInvalidConfiguration(t *testing.T) {
	connection := startJetStream(t)
	store := openConcurrentInbox(t)
	command := messenger.MustCommand("media.invalid-resource", 1, messenger.JSON[testPayload]())
	handler := func(context.Context, messenger.Message[testPayload]) error { return nil }
	tests := []struct {
		name   string
		mutate func(*nats.HandlerConfig)
	}{
		{name: "stream", mutate: func(config *nats.HandlerConfig) { config.Stream = "BAD.STREAM" }},
		{name: "consumer", mutate: func(config *nats.HandlerConfig) { config.ConsumerID = "BAD CONSUMER" }},
		{name: "finalization timeout", mutate: func(config *nats.HandlerConfig) {
			config.FinalizationTimeout = -time.Second
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testHandlerConfig("valid-worker")
			test.mutate(&config)
			if _, err := nats.NewCommandConsumer(connection, store, command, handler, config); !errors.Is(err, nats.ErrInvalidConfig) {
				t.Fatalf("invalid configuration error = %v", err)
			}
		})
	}
}

func TestConsumerPersistsPermanentOutcomeAcrossInterruptedDLQHandoff(t *testing.T) {
	connection := startJetStream(t)
	sourceSubject := "test.command.media.dlq-retry.v1"
	command := messenger.MustCommand("media.dlq-retry", 1, messenger.JSON[testPayload]())
	var calls atomic.Int32
	config := testHandlerConfig("media-dlq-retry-worker")
	config.MaxAttempts = 3
	config.AckWait = 120 * time.Millisecond
	config.DLQSubject = "unrouted.dlq"
	_, err := nats.ApplyTopology(t.Context(), connection, nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams: []nats.StreamSpec{
			nats.DevStream(testStreamName, sourceSubject),
			nats.DevDLQStream(testDLQStreamName, config.DLQSubject),
		},
	})
	if err != nil {
		t.Fatalf("apply topology: %v", err)
	}
	store := openInbox(t)
	handler := func(context.Context, messenger.Message[testPayload]) error {
		calls.Add(1)
		return messenger.Permanent(errors.New("reject"))
	}
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		handler,
		config,
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := js.DeleteStream(t.Context(), testDLQStreamName); err != nil {
		t.Fatalf("remove DLQ stream: %v", err)
	}
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000080", "dlq-retry-1")
	waitFor(t, func() bool { return calls.Load() == 1 })
	time.Sleep(3 * config.AckWait)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls while DLQ was unavailable = %d, want 1", got)
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("first run: %v", err)
	}

	_, err = nats.ApplyTopology(t.Context(), connection, nats.Topology{
		SpecVersion: nats.TopologySpecVersion,
		Streams:     []nats.StreamSpec{nats.DevDLQStream(testDLQStreamName, config.DLQSubject)},
	})
	if err != nil {
		t.Fatalf("route DLQ subject: %v", err)
	}
	subscription, err := connection.SubscribeSync(config.DLQSubject)
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush subscription: %v", err)
	}
	replacement, err := nats.NewCommandConsumer(connection, store, command, handler, config)
	if err != nil {
		t.Fatalf("replacement consumer: %v", err)
	}
	replacementContext, cancelReplacement := context.WithCancel(t.Context())
	replacementDone := make(chan error, 1)
	go func() { replacementDone <- replacement.Run(replacementContext) }()
	waitReady(t, replacement)
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait DLQ after restart: %v", err)
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode DLQ after restart: %v", err)
	}
	if record.Attempt != 1 || record.FailureKind != testPermanentFailure {
		t.Fatalf("DLQ record after restart = %#v", record)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls after DLQ recovery = %d, want 1", got)
	}
	cancelReplacement()
	if err := <-replacementDone; err != nil {
		t.Fatalf("replacement run: %v", err)
	}
}

func TestDLQReplayStartsFreshCycleWhenPostAckCleanupFails(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	backend := &failForgetAttemptBackend{delegate: openInbox(t)}
	backend.failures.Store(1)
	store, err := inbox.New(backend)
	if err != nil {
		t.Fatalf("new wrapped inbox: %v", err)
	}
	command := messenger.MustCommand("media.replay-cleanup", 1, messenger.JSON[testPayload]())
	config := testHandlerConfig("media-replay-cleanup-worker")
	config.MaxAttempts = 1
	var calls atomic.Int32
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(context.Context, messenger.Message[testPayload]) error {
			if calls.Add(1) == 1 {
				return messenger.Permanent(errors.New("reject original"))
			}
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
		t.Fatalf("flush subscription: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000081", "replay-cleanup-1")
	dlqMessage, err := subscription.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("wait original DLQ: %v", err)
	}
	record, err := nats.DecodeDLQRecord(dlqMessage.Data)
	if err != nil {
		t.Fatalf("decode original DLQ: %v", err)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	waitFor(t, func() bool { return backend.forgetCalls.Load() == 1 })

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := nats.ReplayDLQ(t.Context(), js, record); err != nil {
		t.Fatalf("replay: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 2 })
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	if _, err := subscription.NextMsg(150 * time.Millisecond); !errors.Is(err, natsio.ErrTimeout) {
		t.Fatalf("unexpected replay DLQ: %v", err)
	}
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerInfrastructureFailureDoesNotConsumeHandlerAttempt(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	backend := &failFirstAttemptBackend{delegate: openInbox(t)}
	backend.failures.Store(1)
	store, err := inbox.New(backend)
	if err != nil {
		t.Fatalf("wrap inbox: %v", err)
	}
	command := messenger.MustCommand("media.infrastructure-retry", 1, messenger.JSON[testPayload]())
	var calls atomic.Int32
	config := testHandlerConfig("media-infrastructure-retry-worker")
	config.MaxAttempts = 1
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
		t.Fatalf("new consumer: %v", err)
	}
	runContext, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runContext) }()
	waitReady(t, consumer)
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000031", "infrastructure-retry")
	waitFor(t, func() bool { return calls.Load() == 1 })
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerConcurrencyIsExplicitAndBounded(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openConcurrentInbox(t)
	command := messenger.MustCommand("media.concurrent", 1, messenger.JSON[testPayload]())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32

	config := testHandlerConfig("media-concurrent-worker")
	config.Concurrency = 2
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(ctx context.Context, _ messenger.Message[testPayload]) error {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000050", "concurrent-1")
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000051", "concurrent-2")

	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("two handlers did not start concurrently")
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrency = %d, want 2", got)
	}
	close(release)
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerConcurrencyOneProcessesStrictly(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openConcurrentInbox(t)
	command := messenger.MustCommand("media.strict", 1, messenger.JSON[testPayload]())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32

	config := testHandlerConfig("media-strict-worker")
	consumer, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(ctx context.Context, _ messenger.Message[testPayload]) error {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000060", "strict-1")
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000061", "strict-2")

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler did not start")
	}
	select {
	case <-started:
		t.Fatal("second handler started before the first completed")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("second handler did not start after release")
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrency = %d, want 1", got)
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestConsumerDrainLeavesCancelledWorkForRedelivery(t *testing.T) {
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	store := openInbox(t)
	command := messenger.MustCommand("media.drain", 1, messenger.JSON[testPayload]())
	started := make(chan struct{}, 1)
	var calls atomic.Int32

	config := testHandlerConfig("media-drain-worker")
	config.AckWait = 150 * time.Millisecond
	config.Timeout = 2 * time.Second
	first, err := nats.NewCommandConsumer(
		connection,
		store,
		command,
		func(ctx context.Context, _ messenger.Message[testPayload]) error {
			calls.Add(1)
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
		config,
	)
	if err != nil {
		t.Fatalf("first consumer: %v", err)
	}
	firstContext, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstContext) }()
	waitReady(t, first)
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000070", "drain-1")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	first.BeginDrain()
	if err := first.Readiness(t.Context()); err == nil {
		t.Fatal("draining consumer remained ready")
	}
	shutdownContext, cancelShutdown := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err = first.Shutdown(shutdownContext)
	cancelShutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	cancelFirst()
	if err := <-firstDone; err != nil {
		t.Fatalf("first run: %v", err)
	}

	second, err := nats.NewCommandConsumer(
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
		t.Fatalf("replacement consumer: %v", err)
	}
	secondContext, cancelSecond := context.WithCancel(t.Context())
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondContext) }()
	waitReady(t, second)
	waitFor(t, func() bool { return calls.Load() == 2 })
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	cancelSecond()
	if err := <-secondDone; err != nil {
		t.Fatalf("second run: %v", err)
	}
}

func waitForConsumerEmpty(t *testing.T, connection *natsio.Conn, consumerID string) {
	t.Helper()
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	waitFor(t, func() bool {
		consumer, consumerErr := js.Consumer(t.Context(), testStreamName, consumerID)
		if consumerErr != nil {
			return false
		}
		info, infoErr := consumer.Info(t.Context())
		return infoErr == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
}

type concurrentInboxBackend struct {
	mu       sync.Mutex
	attempts map[inbox.Key]uint64
}

type failFirstAttemptBackend struct {
	delegate *inbox.Store
	failures atomic.Int32
}

type failForgetAttemptBackend struct {
	delegate    *inbox.Store
	failures    atomic.Int32
	forgetCalls atomic.Int32
}

func (b *failFirstAttemptBackend) Process(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return b.delegate.Process(ctx, key, fingerprint, handler)
}

func (b *failFirstAttemptBackend) ProcessAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	maxAttempts uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	if b.failures.CompareAndSwap(1, 0) {
		return inbox.Result{}, errors.New("temporary inbox failure")
	}
	return b.delegate.ProcessAttempt(ctx, key, fingerprint, maxAttempts, handler)
}

func (b *failFirstAttemptBackend) ForgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	return b.delegate.ForgetAttempt(ctx, key, fingerprint)
}

func (b *failFirstAttemptBackend) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	return b.delegate.Prune(ctx, before, limit)
}

func (b *failForgetAttemptBackend) Process(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return b.delegate.Process(ctx, key, fingerprint, handler)
}

func (b *failForgetAttemptBackend) ProcessAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	maxAttempts uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	return b.delegate.ProcessAttempt(ctx, key, fingerprint, maxAttempts, handler)
}

func (b *failForgetAttemptBackend) ForgetAttempt(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
) error {
	b.forgetCalls.Add(1)
	if b.failures.CompareAndSwap(1, 0) {
		return errors.New("temporary attempt cleanup failure")
	}
	return b.delegate.ForgetAttempt(ctx, key, fingerprint)
}

func (b *failForgetAttemptBackend) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	return b.delegate.Prune(ctx, before, limit)
}

func (*concurrentInboxBackend) Process(
	ctx context.Context,
	_ inbox.Key,
	_ inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{}, handler(ctx)
}

func (b *concurrentInboxBackend) ProcessAttempt(
	ctx context.Context,
	key inbox.Key,
	_ inbox.Fingerprint,
	maxAttempts uint64,
	handler inbox.Handler,
) (inbox.Result, error) {
	b.mu.Lock()
	attempt := b.attempts[key]
	if attempt >= maxAttempts {
		b.mu.Unlock()
		return inbox.Result{Attempt: attempt}, inbox.ErrAttemptsExhausted
	}
	attempt++
	b.attempts[key] = attempt
	b.mu.Unlock()
	return inbox.Result{Attempt: attempt}, handler(ctx)
}

func (b *concurrentInboxBackend) ForgetAttempt(
	_ context.Context,
	key inbox.Key,
	_ inbox.Fingerprint,
) error {
	b.mu.Lock()
	delete(b.attempts, key)
	b.mu.Unlock()
	return nil
}

func (*concurrentInboxBackend) Prune(_ context.Context, _ time.Time, _ int) (int64, error) {
	return 0, nil
}

func openConcurrentInbox(t *testing.T) *inbox.Store {
	t.Helper()
	store, err := inbox.New(&concurrentInboxBackend{attempts: make(map[inbox.Key]uint64)})
	if err != nil {
		t.Fatalf("new concurrent inbox: %v", err)
	}
	return store
}
