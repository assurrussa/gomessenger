package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ackWindowBackend struct{ testNATSBatchBackend }

func (*ackWindowBackend) ProcessAttempt(
	ctx context.Context, _ inbox.Key, _ inbox.Fingerprint, _ uint64, handler inbox.Handler,
) (inbox.Result, error) {
	return inbox.Result{Attempt: 1}, handler(ctx)
}

type ackWindowFixture struct {
	connection *natsio.Conn
	js         jetstream.JetStream
	command    messenger.Command[string]
	subject    string
	config     HandlerConfig
}

func newAckWindowFixture(t *testing.T) ackWindowFixture {
	t.Helper()
	connection, cleanup := startInternalNATSServer(t)
	t.Cleanup(cleanup)
	command := messenger.MustCommand("ack.window", 1, messenger.JSON[string]())
	subject, err := Subject("test", command.Info())
	if err != nil {
		t.Fatal(err)
	}
	config := HandlerConfig{
		Stream: "ACK_WINDOW", Namespace: "test", ConsumerID: "shared-worker",
		Concurrency: 1, Timeout: 10 * time.Second, AckWait: 10 * time.Second,
	}
	if _, err := ApplyTopology(t.Context(), connection, Topology{
		SpecVersion: TopologySpecVersion,
		Streams:     []StreamSpec{DevStream(config.Stream, subject), DevDLQStream("ACK_WINDOW_DLQ", "test.dlq")},
	}); err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	return ackWindowFixture{connection: connection, js: js, command: command, subject: subject, config: config}
}

func (f ackWindowFixture) consumer(
	t *testing.T, config HandlerConfig, batchSize int, handle func(context.Context, int) error,
) *Consumer {
	t.Helper()
	peer, err := natsio.Connect(f.connection.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Close)
	store, err := inbox.New(&ackWindowBackend{})
	if err != nil {
		t.Fatal(err)
	}
	var consumer *Consumer
	if batchSize == 0 {
		consumer, err = NewCommandConsumer(peer, store, f.command,
			func(ctx context.Context, _ messenger.Message[string]) error { return handle(ctx, 1) }, config)
	} else {
		consumer, err = NewBatchCommandConsumer(peer, store, f.command,
			func(ctx context.Context, messages []messenger.Message[string]) (messenger.BatchResult, error) {
				if err := handle(ctx, len(messages)); err != nil {
					return messenger.BatchResult{}, err
				}
				return messenger.NewBatchResultBuilder(messages).Build()
			}, config, messenger.BatchConfig{MaxMessages: batchSize, MaxWait: 100 * time.Millisecond})
	}
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func (f ackWindowFixture) publish(t *testing.T, offset, count int) {
	t.Helper()
	for i := range count {
		id, err := messenger.ParseMessageID(fmt.Sprintf("018f4f2c-4a00-7000-8000-%012x", offset+i+1))
		if err != nil {
			t.Fatal(err)
		}
		wire, err := messenger.EncodeCommandEnvelope(f.command, messenger.Metadata{
			ID: id, Source: testNATSSource, Kind: messenger.KindCommand, Name: f.command.Info().Name,
			SchemaVersion: 1, Time: time.Now().UTC(), ContentType: testDLQContentType, CorrelationID: id,
		}, "payload")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.js.Publish(t.Context(), f.subject, wire); err != nil {
			t.Fatal(err)
		}
	}
}

func (f ackWindowFixture) info(t *testing.T) *jetstream.ConsumerInfo {
	t.Helper()
	consumer, err := f.js.Consumer(t.Context(), f.config.Stream, f.config.ConsumerID)
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func startAckWindowConsumer(t *testing.T, consumer *Consumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("replica Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("replica did not stop")
		}
	})
	deadline := time.After(5 * time.Second)
	for consumer.Readiness(t.Context()) != nil {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("replica startup: %v", err)
		case <-deadline:
			t.Fatal("replica did not become ready")
		case <-time.After(time.Millisecond):
		}
	}
}

func blockAckWindowHandler(started chan<- int, release <-chan struct{}) func(context.Context, int) error {
	return func(ctx context.Context, size int) error {
		select {
		case started <- size:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func waitAckWindowHandler(t *testing.T, started <-chan int, maximum int) {
	t.Helper()
	select {
	case size := <-started:
		if size < 1 || size > maximum {
			t.Fatalf("handler received %d messages, want 1..%d", size, maximum)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica was blocked by another replica's local-sized durable ACK window")
	}
}

func TestConsumerGlobalAckWindowAcrossReplicas(t *testing.T) {
	for _, batchSize := range []int{0, 2} {
		t.Run(fmt.Sprintf("batch-size-%d", batchSize), func(t *testing.T) {
			fixture := newAckWindowFixture(t)
			config := fixture.config
			config.MaxAckPending = 32
			started := []chan int{make(chan int, 128), make(chan int, 128)}
			release := make(chan struct{})
			defer close(release)
			first := fixture.consumer(t, config, batchSize, blockAckWindowHandler(started[0], release))
			startAckWindowConsumer(t, first)
			fixture.publish(t, 0, max(1, batchSize))
			waitAckWindowHandler(t, started[0], max(1, batchSize))

			// The shared durable contract must not depend on either replica's local worker count.
			config.Concurrency = 2
			second := fixture.consumer(t, config, batchSize, blockAckWindowHandler(started[1], release))
			startAckWindowConsumer(t, second)
			fixture.publish(t, 10, 40)
			for range config.Concurrency {
				waitAckWindowHandler(t, started[1], max(1, batchSize))
			}
			for _, consumer := range []*Consumer{first, second} {
				if err := consumer.DeepHealth(t.Context()); err != nil {
					t.Fatalf("matching global window with different local concurrency: %v", err)
				}
			}
			assertAckWindowLocalBounds(t, fixture, started, batchSize)
		})
	}
}

func assertAckWindowLocalBounds(t *testing.T, fixture ackWindowFixture, started []chan int, batchSize int) {
	t.Helper()
	// Single delivery retains bounded workers, jobs, the current Next result and
	// the local iterator buffer. Batch workers each own one filling/active batch.
	localBound := 4 * (1 + 2)
	if batchSize != 0 {
		localBound = (1 + 2) * batchSize
	}
	deadline := time.After(100 * time.Millisecond)
	for {
		info := fixture.info(t)
		if info.Config.MaxAckPending != 32 || info.NumAckPending > localBound || info.NumPending == 0 {
			t.Fatalf("global/local pending bounds: global=%d unacked=%d local-bound=%d pending=%d",
				info.Config.MaxAckPending, info.NumAckPending, localBound, info.NumPending)
		}
		select {
		case <-started[0]:
			t.Fatal("first replica exceeded one local worker")
		case <-started[1]:
			t.Fatal("second replica exceeded two local workers")
		case <-deadline:
			return
		case <-time.After(time.Millisecond):
		}
	}
}

func TestConsumerAckWindowDefaultsAndOverrides(t *testing.T) {
	fixture := newAckWindowFixture(t)
	for _, test := range []struct {
		name        string
		concurrency int
		batchSize   int
		window      int
		want        int
	}{
		{name: "default single", want: 1},
		{name: "legacy single", concurrency: 3, want: 3},
		{name: "legacy batch", concurrency: 3, batchSize: 4, want: 12},
		{name: "explicit single", concurrency: 3, window: 32, want: 32},
		{name: "explicit batch", concurrency: 3, batchSize: 4, window: 32, want: 32},
		{name: "smaller single", concurrency: 3, window: 1, want: 1},
		{name: "smaller batch", concurrency: 3, batchSize: 4, window: 1, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := fixture.config
			config.Concurrency, config.MaxAckPending = test.concurrency, test.window
			consumer := fixture.consumer(t, config, test.batchSize, func(context.Context, int) error { return nil })
			if got := consumer.consumerSpec().MaxAckPending; got != test.want {
				t.Fatalf("MaxAckPending = %d, want %d", got, test.want)
			}
		})
	}
}

func TestConsumerRejectsInvalidAckWindowAndLocalOverflow(t *testing.T) {
	fixture := newAckWindowFixture(t)
	store, err := inbox.New(&ackWindowBackend{})
	if err != nil {
		t.Fatal(err)
	}
	for _, window := range []int{-1, -2} {
		config := fixture.config
		config.MaxAckPending = window
		_, err := NewCommandConsumer(fixture.connection, store, fixture.command,
			func(context.Context, messenger.Message[string]) error { return nil }, config)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("single MaxAckPending=%d: got %v, want ErrInvalidConfig", window, err)
		}
		_, err = NewBatchCommandConsumer(fixture.connection, store, fixture.command,
			func(context.Context, []messenger.Message[string]) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, nil
			}, config, messenger.BatchConfig{})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("batch MaxAckPending=%d: got %v, want ErrInvalidConfig", window, err)
		}
	}
	config := fixture.config
	config.Concurrency, config.MaxAckPending = 2, 32
	for _, batchConfig := range []messenger.BatchConfig{
		{MaxMessages: int(^uint(0) >> 1)},
		{MaxBytes: int(^uint(0) >> 1)},
	} {
		_, err := NewBatchCommandConsumer(fixture.connection, store, fixture.command,
			func(context.Context, []messenger.Message[string]) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, nil
			}, config, batchConfig)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("explicit global window bypassed local batch overflow validation: %v", err)
		}
	}
}

func TestConsumerSmallAckWindowIsBrokerBounded(t *testing.T) {
	for _, batchSize := range []int{0, 2} {
		t.Run(fmt.Sprintf("batch-size-%d", batchSize), func(t *testing.T) {
			fixture := newAckWindowFixture(t)
			config := fixture.config
			config.Concurrency, config.MaxAckPending = 2, 1
			started := make(chan int, 16)
			release := make(chan struct{})
			defer close(release)
			consumer := fixture.consumer(t, config, batchSize, blockAckWindowHandler(started, release))
			startAckWindowConsumer(t, consumer)
			fixture.publish(t, 0, 10)
			waitAckWindowHandler(t, started, 1)
			deadline := time.After(100 * time.Millisecond)
			for {
				info := fixture.info(t)
				if info.Config.MaxAckPending != 1 || info.NumAckPending != 1 || info.NumPending != 9 {
					t.Fatalf("small global window: config=%d unacked=%d pending=%d",
						info.Config.MaxAckPending, info.NumAckPending, info.NumPending)
				}
				select {
				case <-started:
					t.Fatal("local concurrency bypassed the smaller broker ACK window")
				case <-deadline:
					return
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}

func TestConsumerAckWindowTopologyConflict(t *testing.T) {
	fixture := newAckWindowFixture(t)
	config := fixture.config
	config.MaxAckPending = 32
	first := fixture.consumer(t, config, 0, func(context.Context, int) error { return nil })
	startAckWindowConsumer(t, first)
	for _, window := range []int{0, 16, 64} {
		t.Run(fmt.Sprintf("window-%d", window), func(t *testing.T) {
			config.MaxAckPending = window
			second := fixture.consumer(t, config, 0, func(context.Context, int) error { return nil })
			runCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if err := second.Run(runCtx); !errors.Is(err, ErrTopologyDrift) {
				t.Fatalf("mismatched Run = %v, want ErrTopologyDrift", err)
			}
			if err := second.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
				t.Fatalf("mismatched consumer Readiness = %v, want ErrRuntimeNotRunning", err)
			}
			changes, err := ApplyTopology(t.Context(), fixture.connection, Topology{
				SpecVersion: TopologySpecVersion, Consumers: []ConsumerSpec{second.consumerSpec()},
			})
			if !errors.Is(err, ErrTopologyDrift) || len(changes) != 1 || changes[0].Action != ChangeConflict {
				t.Fatalf("mismatched ApplyTopology = %#v, %v", changes, err)
			}
			if got := fixture.info(t).Config.MaxAckPending; got != 32 {
				t.Fatalf("mismatch mutated the durable window to %d", got)
			}
			if err := first.DeepHealth(t.Context()); err != nil {
				t.Fatalf("original replica health changed: %v", err)
			}
		})
	}
	// A deliberate external topology change is detected without rewriting it.
	drifted := fixture.info(t).Config
	drifted.MaxAckPending = 64
	if _, err := fixture.js.UpdateConsumer(t.Context(), config.Stream, drifted); err != nil {
		t.Fatal(err)
	}
	if err := first.DeepHealth(t.Context()); !errors.Is(err, ErrTopologyDrift) {
		t.Fatalf("DeepHealth after global window drift = %v, want ErrTopologyDrift", err)
	}
	if got := fixture.info(t).Config.MaxAckPending; got != 64 {
		t.Fatalf("DeepHealth mutated the durable window to %d", got)
	}
}

func TestBatchConsumerDefaultAckWindowUsesNormalizedBatchSize(t *testing.T) {
	fixture := newAckWindowFixture(t)
	store, err := inbox.New(&ackWindowBackend{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		concurrency int
		window      int
		want        int
	}{
		{concurrency: 0, want: messenger.DefaultBatchMaxMessages},
		{concurrency: 3, want: 3 * messenger.DefaultBatchMaxMessages},
		{concurrency: 3, window: 32, want: 32},
	} {
		config := fixture.config
		config.Concurrency, config.MaxAckPending = test.concurrency, test.window
		consumer, err := NewBatchCommandConsumer(fixture.connection, store, fixture.command,
			func(context.Context, []messenger.Message[string]) (messenger.BatchResult, error) {
				return messenger.BatchResult{}, nil
			}, config, messenger.BatchConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if got := consumer.consumerSpec().MaxAckPending; got != test.want {
			t.Fatalf("default batch MaxAckPending = %d, want %d", got, test.want)
		}
	}
}
