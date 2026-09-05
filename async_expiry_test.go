package messenger_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type expiryRecorder struct {
	mu    sync.Mutex
	items []messenger.Observation
}

func (o *expiryRecorder) Observe(_ context.Context, item messenger.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.items = append(o.items, item)
}

func TestLocalAsyncRoute_ExpiresQueuedCommandAndEvent(t *testing.T) {
	for _, event := range []bool{false, true} {
		name := "command"
		if event {
			name = "event"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { testQueuedExpiry(t, event) })
		})
	}
}

func testQueuedExpiry(t *testing.T, event bool) {
	t.Helper()
	route, err := messenger.NewLocalAsyncRoute("expiry-queue", messenger.LocalAsyncConfig{Workers: 1, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	observer := &expiryRecorder{}
	builder := messenger.NewBuilder(messenger.WithSource("urn:expiry"), messenger.WithObserver(observer))
	blocker := messenger.MustCommand("expiry.blocker", 1, messenger.JSON[int]())
	started := make(chan struct{})
	release := make(chan struct{})
	builder.HandleCommandFunc(blocker, "blocker", func(context.Context, int) error { close(started); <-release; return nil })
	builder.RouteCommand(blocker, route)
	var calls atomic.Int32
	var middlewareCalls atomic.Int32
	builder.UseMiddleware(func(ctx context.Context, metadata messenger.Metadata, _ string, next messenger.HandlerFunc) error {
		if metadata.Name != blocker.Info().Name {
			middlewareCalls.Add(1)
		}
		return next(ctx)
	})
	command := messenger.MustCommand("expiry.command", 1, messenger.JSON[int]())
	descriptor := messenger.MustEvent("expiry.event", 1, messenger.JSON[int]())
	if event {
		builder.SubscribeFunc(descriptor, "subscriber", func(context.Context, int) error { calls.Add(1); return nil })
		builder.RouteEvent(descriptor, route)
	} else {
		builder.HandleCommandFunc(command, "handler", func(context.Context, int) error { calls.Add(1); return nil })
		builder.RouteCommand(command, route)
	}
	bus, runtime, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	waitRuntimeReady(t, runtime)
	if _, err := bus.SendMessage(t.Context(), blocker, messenger.Outgoing[int]{
		Metadata: messenger.OutgoingMetadata{ExpiresAt: time.Now().Add(time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	outgoing := messenger.Outgoing[int]{Payload: 1, Metadata: messenger.OutgoingMetadata{ExpiresAt: time.Now().Add(time.Second)}}
	var receipt messenger.Receipt
	if event {
		receipt, err = bus.PublishMessage(t.Context(), descriptor, outgoing)
	} else {
		receipt, err = bus.SendMessage(t.Context(), command, outgoing)
	}
	if err != nil || receipt.State != messenger.ReceiptAccepted {
		t.Fatalf("admission: %+v, %v", receipt, err)
	}
	time.Sleep(2 * time.Second) // Virtual time: the queued job expires while the worker is blocked.
	close(release)
	synctest.Wait()
	runtime.BeginDrain()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 || middlewareCalls.Load() != 0 {
		t.Fatal("expired job invoked handler or middleware")
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	expired := 0
	for _, item := range observer.items {
		if item.MessageID != receipt.MessageID {
			continue
		}
		if item.Operation == messenger.OperationHandle {
			t.Fatal("expiry counted as handler execution")
		}
		if item.Operation == messenger.OperationExpire {
			expired++
			if !errors.Is(item.Err, messenger.ErrMessageExpired) || !messenger.IsPermanent(item.Err) || item.Route != route.Name() ||
				item.Name == "" || item.SchemaVersion != 1 || item.HandlerID != "" {
				t.Fatalf("expiry observation: %+v", item)
			}
		}
	}
	if expired != 1 {
		t.Fatalf("expiry observations: %d", expired)
	}
}
