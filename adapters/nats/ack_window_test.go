package nats_test

import (
	"context"
	"testing"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

func TestHandlerConfigGlobalAckWindowPublicConstructors(t *testing.T) {
	connection := startJetStream(t)
	store := openInbox(t)
	command := messenger.MustCommand("window.command", 1, messenger.JSON[testPayload]())
	event := messenger.MustEvent("window.event", 1, messenger.JSON[testPayload]())
	config := nats.HandlerConfig{
		Stream: testStreamName, Namespace: testNamespace, ConsumerID: testConsumerID,
		Concurrency: 2, MaxAckPending: 16,
	}
	handler := func(context.Context, messenger.Message[testPayload]) error { return nil }
	batchHandler := func(_ context.Context, messages []messenger.Message[testPayload]) (messenger.BatchResult, error) {
		return messenger.NewBatchResultBuilder(messages).Build()
	}
	constructors := []struct {
		name string
		new  func() (*nats.Consumer, error)
	}{
		{name: "command", new: func() (*nats.Consumer, error) {
			return nats.NewCommandConsumer(connection, store, command, handler, config)
		}},
		{name: "event", new: func() (*nats.Consumer, error) {
			return nats.NewEventConsumer(connection, store, event, handler, config)
		}},
		{name: "batch command", new: func() (*nats.Consumer, error) {
			return nats.NewBatchCommandConsumer(connection, store, command, batchHandler, config, messenger.BatchConfig{})
		}},
		{name: "batch event", new: func() (*nats.Consumer, error) {
			return nats.NewBatchEventConsumer(connection, store, event, batchHandler, config, messenger.BatchConfig{})
		}},
	}
	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			if _, err := constructor.new(); err != nil {
				t.Fatalf("public MaxAckPending config: %v", err)
			}
		})
	}
}
