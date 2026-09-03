package messenger_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type fixedGenerator struct{ id messenger.MessageID }

// Shared test fixture values reused across the external test package.
const (
	testSource      = "urn:service:test"
	testEventName   = "media.processed"
	testContentType = "application/json"
	testHandlerID   = "audit"
	testServiceID   = "consumer.audit"
	testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

func (g fixedGenerator) New() (messenger.MessageID, error) { return g.id, nil }

type processPayload struct {
	JobID int64 `json:"jobId"`
}

type processedPayload struct {
	JobID int64 `json:"jobId"`
}

func TestMessenger_LocalCommandPreservesTypedMetadataWithoutSerialization(t *testing.T) {
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	fixedTime := time.Unix(1_700_000_000, 0).UTC()
	var encodeCalls atomic.Int32
	codec, err := messenger.CustomCodec(
		"application/json",
		messenger.DataJSON,
		func(value processPayload) ([]byte, error) {
			encodeCalls.Add(1)
			return []byte(fmt.Sprintf(`{"jobId":%d}`, value.JobID)), nil
		},
		func([]byte) (processPayload, error) { return processPayload{}, nil },
	)
	if err != nil {
		t.Fatalf("custom codec: %v", err)
	}
	command := messenger.MustCommand("media.processor", 1, codec)
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:media-resizer"),
		messenger.WithIDGenerator(fixedGenerator{id: id}),
		messenger.WithClock(func() time.Time { return fixedTime }),
	)
	var handled messenger.Message[processPayload]
	builder.HandleCommand(command, "media-processor", func(_ context.Context, message messenger.Message[processPayload]) error {
		handled = message
		return nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())

	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	receipt, err := m.Send(t.Context(), command, processPayload{JobID: 42})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if receipt.State != messenger.ReceiptCompleted || receipt.MessageID != id || !receipt.At.Equal(fixedTime) {
		t.Fatalf("receipt = %#v", receipt)
	}
	if handled.Payload.JobID != 42 || handled.Metadata.ID != id ||
		handled.Metadata.CorrelationID != id || handled.Metadata.Source != "urn:service:media-resizer" ||
		!handled.Metadata.Time.Equal(fixedTime) {
		t.Fatalf("handled message = %#v", handled)
	}
	if got := encodeCalls.Load(); got != 0 {
		t.Fatalf("local route encoded payload %d times", got)
	}
}

func TestMessenger_ChildEventInheritsLineage(t *testing.T) {
	ids := []messenger.MessageID{
		mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001"),
		mustMessageID(t, "018f4f2c-4a01-7000-8000-000000000002"),
	}
	generator := &sequenceGenerator{ids: ids}
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:media-resizer"),
		messenger.WithIDGenerator(generator),
	)
	var m *messenger.Messenger
	var child messenger.Metadata
	builder.HandleCommand(command, "media-processor", func(ctx context.Context, message messenger.Message[processPayload]) error {
		_, err := m.Publish(ctx, event, processedPayload{JobID: message.Payload.JobID})
		return err
	})
	builder.Subscribe(event, "audit", func(_ context.Context, message messenger.Message[processedPayload]) error {
		child = message.Metadata
		return nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	var err error
	m, _, err = builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parentReceipt, err := m.Send(t.Context(), command, processPayload{JobID: 7})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if child.CorrelationID != parentReceipt.MessageID || child.CausationID != parentReceipt.MessageID {
		t.Fatalf("child lineage = correlation %s causation %s; parent %s",
			child.CorrelationID, child.CausationID, parentReceipt.MessageID)
	}
}

func TestMessenger_LocalEventWithoutSubscribersIsNoop(t *testing.T) {
	event := messenger.MustEvent("cache.invalidated", 1, messenger.JSON[string]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	receipt, err := m.Publish(t.Context(), event, "key")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if receipt.State != messenger.ReceiptNoop {
		t.Fatalf("state = %q, want %q", receipt.State, messenger.ReceiptNoop)
	}
}

func TestMessenger_LocalEventJoinsSubscriberErrors(t *testing.T) {
	errFirst := errors.New("first")
	errSecond := errors.New("second")
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.Subscribe(event, "first", func(context.Context, messenger.Message[processedPayload]) error { return errFirst })
	builder.Subscribe(event, "second", func(context.Context, messenger.Message[processedPayload]) error { return errSecond })
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	_, err = m.Publish(t.Context(), event, processedPayload{JobID: 9})
	if !errors.Is(err, errFirst) || !errors.Is(err, errSecond) {
		t.Fatalf("publish error = %v", err)
	}
}

func TestBuilder_RejectsLocalCommandWithoutHandler(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	_, _, err := builder.Build()
	if !errors.Is(err, messenger.ErrHandlerNotFound) {
		t.Fatalf("build error = %v", err)
	}
}

func TestBindSenderAndPublisher(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	builder.Subscribe(event, "subscriber", func(context.Context, messenger.Message[processedPayload]) error { return nil })
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := messenger.BindSender(m, command).Send(t.Context(), processPayload{}); err != nil {
		t.Fatalf("bound send: %v", err)
	}
	if _, err := messenger.BindPublisher(m, event).Publish(t.Context(), processedPayload{}); err != nil {
		t.Fatalf("bound publish: %v", err)
	}
}

type sequenceGenerator struct {
	ids   []messenger.MessageID
	index atomic.Uint32
}

func (g *sequenceGenerator) New() (messenger.MessageID, error) {
	index := int(g.index.Add(1)) - 1
	if index >= len(g.ids) {
		return messenger.MessageID{}, errors.New("sequence exhausted")
	}
	return g.ids[index], nil
}

func mustMessageID(t *testing.T, value string) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID(value)
	if err != nil {
		t.Fatalf("parse message id: %v", err)
	}
	return id
}

type recordingDeliveryRoute struct {
	deliveries []messenger.Delivery
}

func (r *recordingDeliveryRoute) Name() string { return "recording.route" }

func (r *recordingDeliveryRoute) Deliver(_ context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
	r.deliveries = append(r.deliveries, delivery)
	return messenger.Receipt{State: messenger.ReceiptBrokerConfirmed}, nil
}

func TestMessengerNilInterfaceCommandAndEvent_LocalDispatch(t *testing.T) {
	command := messenger.MustCommand[any]("test.nil-command", 1, messenger.JSON[any]())
	event := messenger.MustEvent[any]("test.nil-event", 1, messenger.JSON[any]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	var commandCalls, eventCalls atomic.Int32

	builder.HandleCommandFunc(command, "command.handler", func(_ context.Context, value any) error {
		commandCalls.Add(1)
		if value != nil {
			return fmt.Errorf("expected nil command payload, got %#v", value)
		}
		return nil
	})
	builder.SubscribeFunc(event, "event.subscriber", func(_ context.Context, value any) error {
		eventCalls.Add(1)
		if value != nil {
			return fmt.Errorf("expected nil event payload, got %#v", value)
		}
		return nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())

	bus, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if _, err := bus.Send(t.Context(), command, nil); err != nil {
		t.Fatalf("send nil command: %v", err)
	}
	if commandCalls.Load() != 1 {
		t.Fatalf("command calls = %d, want 1", commandCalls.Load())
	}

	if _, err := bus.Publish(t.Context(), event, nil); err != nil {
		t.Fatalf("publish nil event: %v", err)
	}
	if eventCalls.Load() != 1 {
		t.Fatalf("event calls = %d, want 1", eventCalls.Load())
	}
}

func TestMessengerNilInterfaceCommandAndEvent_RemotePublish(t *testing.T) {
	command := messenger.MustCommand[any]("test.nil-command", 1, messenger.JSON[any]())
	event := messenger.MustEvent[any]("test.nil-event", 1, messenger.JSON[any]())
	route := &recordingDeliveryRoute{}
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:test"))
	builder.RouteCommand(command, route)
	builder.RouteEvent(event, route)

	bus, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if _, err := bus.Send(t.Context(), command, nil); err != nil {
		t.Fatalf("remote send nil command: %v", err)
	}
	if _, err := bus.Publish(t.Context(), event, nil); err != nil {
		t.Fatalf("remote publish nil event: %v", err)
	}
	if len(route.deliveries) != 2 {
		t.Fatalf("recorded deliveries = %d, want 2", len(route.deliveries))
	}
	for i, delivery := range route.deliveries {
		data, err := delivery.MarshalEnvelope()
		if err != nil {
			t.Fatalf("delivery %d marshal envelope: %v", i, err)
		}
		envelope, err := messenger.UnmarshalEnvelope(data)
		if err != nil {
			t.Fatalf("delivery %d unmarshal envelope: %v", i, err)
		}
		payload, encoding, err := envelope.Payload()
		if err != nil {
			t.Fatalf("delivery %d payload: %v", i, err)
		}
		if string(payload) != "null" || encoding != messenger.DataJSON {
			t.Fatalf("delivery %d payload = %q, encoding = %v", i, string(payload), encoding)
		}
	}
}
