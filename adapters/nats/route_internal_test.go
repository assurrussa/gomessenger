package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type stubPubAckFuture struct {
	ok  chan *jetstream.PubAck
	err chan error
	msg *natsio.Msg
}

func (f *stubPubAckFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *stubPubAckFuture) Err() <-chan error            { return f.err }
func (f *stubPubAckFuture) Msg() *natsio.Msg             { return f.msg }

func TestNewRouteRejectsNegativePublishWindow(t *testing.T) {
	conn, cleanup := startInternalNATSServer(t)
	defer cleanup()

	_, err := NewRoute(conn, RouteConfig{
		Name:          "test-route",
		Namespace:     "test-ns",
		WireMode:      WireNative,
		PublishWindow: -1,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRoute with negative PublishWindow = %v, want ErrInvalidConfig", err)
	}
}

func TestRoutePublishEnvelopeBatchHonorsCancellationWhenWindowSaturated(t *testing.T) {
	conn, cleanup := startInternalNATSServer(t)
	defer cleanup()

	event := messenger.MustEvent("test.batch.publish.cancel", 1, messenger.JSON[string]())
	route, err := NewRoute(conn, RouteConfig{
		Name:          "test-route",
		Namespace:     "test-ns",
		WireMode:      WireNative,
		PublishWindow: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	payloads := make([][]byte, 5)
	for i := range payloads {
		id, err := messenger.ParseMessageID(fmt.Sprintf("018f4f2c-4a00-7000-8000-%012x", i+1))
		if err != nil {
			t.Fatal(err)
		}
		payloads[i], err = messenger.EncodeEventEnvelope(event, messenger.Metadata{
			ID: id, Kind: messenger.KindEvent, Name: "test.batch.publish.cancel", SchemaVersion: 1,
			Source: "urn:test", Time: time.Now().UTC(),
			CorrelationID: id,
			ContentType:   "application/json",
		}, fmt.Sprintf("job-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var enqueued atomic.Int32
	route.publishMsgAsyncHook = func(item preparedBatchMessage) (jetstream.PubAckFuture, error) {
		count := enqueued.Add(1)
		if count == 2 {
			// Saturated window: cancel the context while publishes are in-flight.
			cancel()
		}
		return &stubPubAckFuture{
			ok:  make(chan *jetstream.PubAck),
			err: make(chan error),
			msg: item.message,
		}, nil
	}

	started := time.Now()
	receipts, itemErrors, batchErr := route.PublishEnvelopeBatch(ctx, payloads)
	duration := time.Since(started)

	if batchErr != nil {
		t.Fatalf("batch error = %v, want nil", batchErr)
	}
	if duration > time.Second {
		t.Fatalf("PublishEnvelopeBatch took %v, want < 1s", duration)
	}
	if enqueued.Load() != 2 {
		t.Fatalf("enqueued count = %d, want exactly 2 (window size)", enqueued.Load())
	}
	if len(receipts) != 5 || len(itemErrors) != 5 {
		t.Fatalf("receipts/itemErrors length = %d/%d, want 5", len(receipts), len(itemErrors))
	}
	for i, err := range itemErrors {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("item %d error = %v, want context.Canceled", i, err)
		}
		if !receipts[i].MessageID.IsZero() {
			t.Fatalf("item %d receipt = %#v, want zero receipt", i, receipts[i])
		}
	}
}
