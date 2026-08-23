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
