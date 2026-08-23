package messenger_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type routeFunc struct {
	name    string
	deliver func(context.Context, messenger.Delivery) (messenger.Receipt, error)
}

func (r *routeFunc) Name() string { return r.name }
func (r *routeFunc) Deliver(ctx context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
	return r.deliver(ctx, delivery)
}

type errorGenerator struct{ err error }

func (g errorGenerator) New() (messenger.MessageID, error) {
	return messenger.MessageID{}, g.err
}

type recordingObserver struct {
	observations []messenger.Observation
	panic        bool
}

func (o *recordingObserver) Observe(_ context.Context, observation messenger.Observation) {
	o.observations = append(o.observations, observation)
	if o.panic {
		panic("observer")
	}
}

func TestExternalRouteLazilySerializesOnceAndCoreNormalizesReceipt(t *testing.T) {
	var encodeCalls atomic.Int32
	codec, err := messenger.CustomCodec(
		testContentType, messenger.DataJSON,
		func(_ processPayload) ([]byte, error) {
			encodeCalls.Add(1)
			return []byte(`{"jobId":42}`), nil
		},
		func([]byte) (processPayload, error) { return processPayload{JobID: 42}, nil },
	)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	event := messenger.MustEvent(testEventName, 1, codec)
	var handled atomic.Int32
	route := &routeFunc{
		name: "transport.test",
		deliver: func(ctx context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
			if delivery.HandlerCount() != 1 || delivery.Metadata().Name != testEventName {
				t.Fatalf("delivery metadata/count = %#v/%d", delivery.Metadata(), delivery.HandlerCount())
			}
			first, err := delivery.MarshalEnvelope()
			if err != nil {
				return messenger.Receipt{}, err
			}
			second, err := delivery.MarshalEnvelope()
			if err != nil || string(first) != string(second) {
				t.Fatalf("second marshal = %s, %v", second, err)
			}
			if _, err := delivery.Fingerprint(); err != nil {
				t.Fatalf("fingerprint: %v", err)
			}
			if err := delivery.Invoke(ctx); err != nil {
				return messenger.Receipt{}, err
			}
			return messenger.Receipt{State: messenger.ReceiptCompleted}, nil
		},
	}
	observer := &recordingObserver{}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithLogger(messenger.AdaptSlog(slog.New(slog.DiscardHandler))),
		messenger.WithObserver(observer),
	)
	builder.Subscribe(event, testHandlerID, func(context.Context, messenger.Message[processPayload]) error {
		handled.Add(1)
		return nil
	})
	builder.RouteEvent(event, route)
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	receipt, err := messenger.BindPublisher(m, event).PublishMessage(t.Context(), messenger.Outgoing[processPayload]{
		Payload: processPayload{JobID: 42}, Metadata: messenger.OutgoingMetadata{Subject: "job/42"},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if receipt.MessageID.IsZero() || receipt.Route != route.name || receipt.At.IsZero() ||
		handled.Load() != 1 || encodeCalls.Load() != 1 || len(observer.observations) != 2 {
		t.Fatalf("receipt=%#v handled=%d encoded=%d observations=%#v",
			receipt, handled.Load(), encodeCalls.Load(), observer.observations)
	}
	manifest := m.Manifest()
	manifest.Descriptors[0].HandlerIDs[0] = "changed"
	if m.Manifest().Descriptors[0].HandlerIDs[0] != testHandlerID {
		t.Fatal("manifest getter did not clone handler IDs")
	}
	if data, err := m.MarshalManifest(); err != nil || len(data) == 0 {
		t.Fatalf("manifest JSON = %s, %v", data, err)
	}
}

func TestMessengerRejectsInvalidRouteReceiptsAndPropagatesErrors(t *testing.T) {
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[processPayload]())
	otherID := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000099")
	routeErr := errors.New("route failed")
	tests := []struct {
		name    string
		receipt messenger.Receipt
		err     error
		want    error
	}{
		{
			name:    "identity",
			receipt: messenger.Receipt{MessageID: otherID, State: messenger.ReceiptCompleted},
			want:    messenger.ErrInvalidMessage,
		},
		{
			name:    "route",
			receipt: messenger.Receipt{Route: "wrong", State: messenger.ReceiptCompleted},
			want:    messenger.ErrInvalidMessage,
		},
		{name: "state", receipt: messenger.Receipt{}, want: messenger.ErrInvalidMessage},
		{name: "error", err: routeErr, want: routeErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := &routeFunc{name: "transport.test", deliver: func(context.Context, messenger.Delivery) (messenger.Receipt, error) {
				return test.receipt, test.err
			}}
			builder := messenger.NewBuilder(messenger.WithSource(testSource))
			builder.RouteEvent(event, route)
			m, _, err := builder.Build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			_, err = m.Publish(t.Context(), event, processPayload{})
			if !errors.Is(err, test.want) {
				t.Fatalf("publish error = %v", err)
			}
		})
	}
}

func TestMessengerValidatesBindingContextGeneratorAndMetadata(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[processPayload]())
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if _, err := m.Send(nil, command, processPayload{}); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil command context = %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if _, err := m.Publish(nil, event, processPayload{}); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil event context = %v", err)
	}
	if _, err := m.Publish(t.Context(), event, processPayload{}); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("unregistered event = %v", err)
	}
	wrong := messenger.MustCommand("media.processor", 1, messenger.JSON[string]())
	if _, err := m.Send(t.Context(), wrong, "payload"); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("wrong payload descriptor = %v", err)
	}

	noRouteBuilder := messenger.NewBuilder(messenger.WithSource(testSource))
	noRouteBuilder.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	noRoute, _, err := noRouteBuilder.Build()
	if err != nil {
		t.Fatalf("build no route: %v", err)
	}
	if _, err := noRoute.Send(t.Context(), command, processPayload{}); !errors.Is(err, messenger.ErrRouteNotFound) {
		t.Fatalf("no route = %v", err)
	}

	generatorErr := errors.New("entropy unavailable")
	generatorBuilder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithIDGenerator(errorGenerator{err: generatorErr}),
	)
	generatorBuilder.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	generatorBuilder.RouteCommand(command, messenger.NewLocalSyncRoute())
	generated, _, err := generatorBuilder.Build()
	if err != nil {
		t.Fatalf("build generator: %v", err)
	}
	if _, err := generated.Send(t.Context(), command, processPayload{}); !errors.Is(err, generatorErr) {
		t.Fatalf("generator error = %v", err)
	}

	invalidMetadata := messenger.Outgoing[processPayload]{Payload: processPayload{}, Metadata: messenger.OutgoingMetadata{
		NotBefore: time.Unix(20, 0), ExpiresAt: time.Unix(10, 0),
	}}
	if _, err := m.SendMessage(t.Context(), command, invalidMetadata); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid metadata = %v", err)
	}
	invalidMetadata.Metadata = messenger.OutgoingMetadata{Headers: map[string]string{"bad\n": "value"}}
	sender := messenger.BindSender(m, command)
	if _, err := sender.SendMessage(t.Context(), invalidMetadata); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid bound metadata = %v", err)
	}
}

func TestBuilderAggregatesDeclarationErrorsAndObserverPanicsAreIsolated(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[processPayload]())
	typedNilRoute := (*routeFunc)(nil)
	typedNilService := (*controlledService)(nil)
	invalid := messenger.NewBuilder(
		nil,
		messenger.WithSource(" bad "),
		messenger.WithIDGenerator(nil),
		messenger.WithClock(nil),
		messenger.WithLogger(nil),
		messenger.WithObserver(nil),
	)
	invalid.HandleCommand(command, "", nil)
	invalid.HandleCommand(command, "one", func(context.Context, messenger.Message[processPayload]) error { return nil })
	invalid.HandleCommand(command, "two", func(context.Context, messenger.Message[processPayload]) error { return nil })
	invalid.Subscribe(event, "duplicate", func(context.Context, messenger.Message[processPayload]) error { return nil })
	invalid.Subscribe(event, "duplicate", func(context.Context, messenger.Message[processPayload]) error { return nil })
	invalid.RouteCommand(command, typedNilRoute)
	invalid.RouteEvent(event, &routeFunc{name: "bad route", deliver: nil})
	invalid.Use("bad service", typedNilService)
	_, _, err := invalid.Build()
	if err == nil || !errors.Is(err, messenger.ErrInvalidMessage) ||
		!errors.Is(err, messenger.ErrHandlerConflict) || !errors.Is(err, messenger.ErrRouteNotFound) ||
		!errors.Is(err, messenger.ErrServiceConflict) {
		t.Fatalf("builder errors = %v", err)
	}

	duplicateRoute := messenger.NewBuilder(messenger.WithSource(testSource))
	duplicateRoute.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	duplicateRoute.RouteCommand(command, messenger.NewLocalSyncRoute())
	duplicateRoute.RouteCommand(command, messenger.NewLocalSyncRoute())
	duplicateRoute.RouteEvent(event, messenger.NewLocalSyncRoute())
	duplicateRoute.RouteEvent(event, messenger.NewLocalSyncRoute())
	serviceOne := newControlledService()
	duplicateRoute.Use("worker", serviceOne)
	duplicateRoute.Use("worker", serviceOne)
	duplicateRoute.Use("worker", newControlledService())
	_, _, err = duplicateRoute.Build()
	if !errors.Is(err, messenger.ErrRouteConflict) || !errors.Is(err, messenger.ErrServiceConflict) {
		t.Fatalf("duplicate errors = %v", err)
	}

	observer := &recordingObserver{panic: true}
	valid := messenger.NewBuilder(messenger.WithSource(testSource), messenger.WithObserver(observer))
	valid.HandleCommand(command, "handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	valid.RouteCommand(command, messenger.NewLocalSyncRoute())
	m, _, err := valid.Build()
	if err != nil {
		t.Fatalf("valid build: %v", err)
	}
	if _, err := m.Send(t.Context(), command, processPayload{}); err != nil {
		t.Fatalf("observer panic escaped: %v", err)
	}
}

func TestLocalEventRecoversHandlerPanicAndContinuesSubscribers(t *testing.T) {
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[processPayload]())
	var handled atomic.Int32
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.Subscribe(event, "panicking", func(context.Context, messenger.Message[processPayload]) error {
		panic("boom")
	})
	builder.Subscribe(event, "healthy", func(context.Context, messenger.Message[processPayload]) error {
		handled.Add(1)
		return nil
	})
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	_, err = instance.Publish(t.Context(), event, processPayload{})
	if err == nil || !strings.Contains(err.Error(), "handler panicking panicked: boom") {
		t.Fatalf("publish error = %v", err)
	}
	if handled.Load() != 1 {
		t.Fatalf("healthy subscriber calls = %d", handled.Load())
	}
}

var _ messenger.Route = (*routeFunc)(nil)
