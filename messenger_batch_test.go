package messenger_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

const outboxBatchRouteName = "outbox.batch"

type recordingBatchRoute struct {
	name       string
	batchCalls int
	deliveries []messenger.Delivery
	batch      func(context.Context, []messenger.Delivery) ([]messenger.Receipt, error)
}

func (r *recordingBatchRoute) Name() string { return r.name }

func (r *recordingBatchRoute) Deliver(
	context.Context,
	messenger.Delivery,
) (messenger.Receipt, error) {
	return messenger.Receipt{}, errors.New("unexpected single delivery")
}

func (r *recordingBatchRoute) DeliverBatch(
	ctx context.Context,
	deliveries []messenger.Delivery,
) ([]messenger.Receipt, error) {
	r.batchCalls++
	r.deliveries = append([]messenger.Delivery(nil), deliveries...)
	if r.batch != nil {
		return r.batch(ctx, deliveries)
	}
	receipts := make([]messenger.Receipt, len(deliveries))
	for index, delivery := range deliveries {
		receipts[index] = messenger.Receipt{
			MessageID: delivery.Metadata().ID,
			State:     messenger.ReceiptStaged,
		}
	}
	return receipts, nil
}

func TestBoundBatchCommandAndEventFacadesUseBatchRoute(t *testing.T) {
	command := messenger.MustCommand("batch.create", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	route := &recordingBatchRoute{name: outboxBatchRouteName}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteCommand(command, route)
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	sender := messenger.BindBatchSender(instance, command)
	publisher := messenger.BindBatchPublisher(instance, event)
	if receipts, err := sender.SendBatch(t.Context(), []processPayload{{JobID: 1}, {JobID: 2}}); err != nil || len(receipts) != 2 {
		t.Fatalf("bound send batch = %#v, %v", receipts, err)
	}
	receipts, err := publisher.PublishBatch(t.Context(), []processPayload{{JobID: 3}, {JobID: 4}})
	if err != nil || len(receipts) != 2 {
		t.Fatalf("bound publish batch = %#v, %v", receipts, err)
	}
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000014")
	if receipts, err := sender.SendMessageBatch(t.Context(), []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 5}, Metadata: messenger.OutgoingMetadata{ID: id}},
	}); err != nil || len(receipts) != 1 || receipts[0].MessageID != id {
		t.Fatalf("bound send message batch = %#v, %v", receipts, err)
	}
	if receipts, err := publisher.PublishMessageBatch(t.Context(), []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 6}, Metadata: messenger.OutgoingMetadata{ID: id}},
	}); err != nil || len(receipts) != 1 || receipts[0].MessageID != id {
		t.Fatalf("bound publish message batch = %#v, %v", receipts, err)
	}
	if route.batchCalls != 4 {
		t.Fatalf("batch route calls = %d, want 4", route.batchCalls)
	}
}

func TestPublishMessageBatchUsesOneAtomicRouteCallAndPreservesOrder(t *testing.T) {
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	route := &recordingBatchRoute{name: outboxBatchRouteName}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	first := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000011")
	second := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000012")
	receipts, err := instance.PublishMessageBatch(t.Context(), event, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 11}, Metadata: messenger.OutgoingMetadata{ID: first}},
		{Payload: processPayload{JobID: 12}, Metadata: messenger.OutgoingMetadata{ID: second}},
	})
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}
	if route.batchCalls != 1 || len(route.deliveries) != 2 || len(receipts) != 2 {
		t.Fatalf("batch calls/deliveries/receipts = %d/%d/%d", route.batchCalls, len(route.deliveries), len(receipts))
	}
	if receipts[0].MessageID != first || receipts[1].MessageID != second ||
		route.deliveries[0].Metadata().ID != first || route.deliveries[1].Metadata().ID != second {
		t.Fatalf("batch order changed: %#v", receipts)
	}
}

func TestPublishMessageBatchRejectsDuplicateIdentityBeforeRouteCall(t *testing.T) {
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	route := &recordingBatchRoute{name: outboxBatchRouteName}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000013")
	_, err = instance.PublishMessageBatch(t.Context(), event, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 1}, Metadata: messenger.OutgoingMetadata{ID: id}},
		{Payload: processPayload{JobID: 2}, Metadata: messenger.OutgoingMetadata{ID: id}},
	})
	if !errors.Is(err, messenger.ErrInvalidMessage) || route.batchCalls != 0 {
		t.Fatalf("duplicate result = %v, calls=%d", err, route.batchCalls)
	}
}

func TestBatchAPIFailsClosedForSingleRoute(t *testing.T) {
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	route := &routeFunc{name: "direct.nats", deliver: func(context.Context, messenger.Delivery) (messenger.Receipt, error) {
		return messenger.Receipt{State: messenger.ReceiptBrokerConfirmed}, nil
	}}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	_, err = instance.PublishBatch(t.Context(), event, []processPayload{{JobID: 1}})
	if !errors.Is(err, messenger.ErrUnsupportedCapability) {
		t.Fatalf("single route batch error = %v", err)
	}
}

func TestBatchFacadeValidationFailsBeforeRoute(t *testing.T) {
	command := messenger.MustCommand("batch.create", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	route := &recordingBatchRoute{name: outboxBatchRouteName}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteCommand(command, route)
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	unknownCommand := messenger.MustCommand("batch.unknown", 1, messenger.JSON[processPayload]())
	unknownEvent := messenger.MustEvent("batch.unknown", 1, messenger.JSON[processPayload]())

	checks := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil command context", err: batchSendNilContext(instance, command), want: messenger.ErrInvalidMessage},
		{name: "empty command", err: batchSendEmpty(t.Context(), instance, command), want: messenger.ErrInvalidMessage},
		{name: "unknown command", err: batchSendUnknown(t.Context(), instance, unknownCommand), want: messenger.ErrDescriptorConflict},
		{name: "nil event context", err: batchPublishNilContext(instance, event), want: messenger.ErrInvalidMessage},
		{name: "empty event", err: batchPublishEmpty(t.Context(), instance, event), want: messenger.ErrInvalidMessage},
		{name: "unknown event", err: batchPublishUnknown(t.Context(), instance, unknownEvent), want: messenger.ErrDescriptorConflict},
	}
	for _, check := range checks {
		if !errors.Is(check.err, check.want) {
			t.Errorf("%s error = %v, want %v", check.name, check.err, check.want)
		}
	}

	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000015")
	_, err = instance.SendMessageBatch(t.Context(), command, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 1}, Metadata: messenger.OutgoingMetadata{ID: id}},
		{Payload: processPayload{JobID: 2}, Metadata: messenger.OutgoingMetadata{ID: id}},
	})
	if !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("duplicate command error = %v", err)
	}
	if route.batchCalls != 0 {
		t.Fatalf("validation reached route %d times", route.batchCalls)
	}
}

func TestBatchFacadeReportsRouteAndReceiptDefectsToEveryObservation(t *testing.T) {
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	observer := &recordingObserver{}
	routeErr := errors.New("stage failed")
	route := &recordingBatchRoute{name: outboxBatchRouteName, batch: func(
		context.Context,
		[]messenger.Delivery,
	) ([]messenger.Receipt, error) {
		return nil, routeErr
	}}
	builder := messenger.NewBuilder(messenger.WithSource(testSource), messenger.WithObserver(observer))
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.PublishBatch(t.Context(), event, []processPayload{{JobID: 1}, {JobID: 2}})
	if !errors.Is(err, routeErr) || len(observer.observations) != 2 {
		t.Fatalf("route error/observations = %v/%d", err, len(observer.observations))
	}
	for _, observation := range observer.observations {
		if !errors.Is(observation.Err, routeErr) || observation.Route != route.name {
			t.Fatalf("observation = %#v", observation)
		}
	}

	observer.observations = nil
	route.batch = func(_ context.Context, deliveries []messenger.Delivery) ([]messenger.Receipt, error) {
		return []messenger.Receipt{{
			MessageID: deliveries[0].Metadata().ID,
			State:     messenger.ReceiptStaged,
		}}, nil
	}
	_, err = instance.PublishBatch(t.Context(), event, []processPayload{{JobID: 3}, {JobID: 4}})
	if !errors.Is(err, messenger.ErrInvalidMessage) || len(observer.observations) != 2 {
		t.Fatalf("receipt count error/observations = %v/%d", err, len(observer.observations))
	}

	route.batch = func(_ context.Context, _ []messenger.Delivery) ([]messenger.Receipt, error) {
		return []messenger.Receipt{{
			MessageID: mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000099"),
			State:     messenger.ReceiptStaged,
		}}, nil
	}
	_, err = instance.PublishBatch(t.Context(), event, []processPayload{{JobID: 5}})
	if !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("changed receipt identity error = %v", err)
	}
}

func TestBatchFacadeReportsMissingRoutes(t *testing.T) {
	command := messenger.MustCommand("batch.create", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("batch.created", 1, messenger.JSON[processPayload]())
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleCommand(command, "batch.handler", func(context.Context, messenger.Message[processPayload]) error { return nil })
	builder.Subscribe(event, "batch.subscriber", func(context.Context, messenger.Message[processPayload]) error { return nil })
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.SendBatch(t.Context(), command, []processPayload{{}}); !errors.Is(err, messenger.ErrRouteNotFound) {
		t.Fatalf("missing command route error = %v", err)
	}
	if _, err := instance.PublishBatch(t.Context(), event, []processPayload{{}}); !errors.Is(err, messenger.ErrRouteNotFound) {
		t.Fatalf("missing event route error = %v", err)
	}
}

func batchSendNilContext(instance *messenger.Messenger, command messenger.Command[processPayload]) error {
	//nolint:staticcheck // The public contract explicitly rejects a nil context.
	_, err := instance.SendBatch(nil, command, []processPayload{{}})
	return err
}

func batchSendEmpty(
	ctx context.Context,
	instance *messenger.Messenger,
	command messenger.Command[processPayload],
) error {
	_, err := instance.SendBatch(ctx, command, nil)
	return err
}

func batchSendUnknown(
	ctx context.Context,
	instance *messenger.Messenger,
	command messenger.Command[processPayload],
) error {
	_, err := instance.SendBatch(ctx, command, []processPayload{{}})
	return err
}

func batchPublishNilContext(instance *messenger.Messenger, event messenger.Event[processPayload]) error {
	//nolint:staticcheck // The public contract explicitly rejects a nil context.
	_, err := instance.PublishBatch(nil, event, []processPayload{{}})
	return err
}

func batchPublishEmpty(
	ctx context.Context,
	instance *messenger.Messenger,
	event messenger.Event[processPayload],
) error {
	_, err := instance.PublishBatch(ctx, event, nil)
	return err
}

func batchPublishUnknown(
	ctx context.Context,
	instance *messenger.Messenger,
	event messenger.Event[processPayload],
) error {
	_, err := instance.PublishBatch(ctx, event, []processPayload{{}})
	return err
}

type failingBatchCodec struct{}

func (failingBatchCodec) Encode(value processPayload) ([]byte, error) {
	if value.JobID < 0 {
		return nil, errors.New("cannot encode negative job id")
	}
	return []byte(fmt.Sprintf(`{"jobId":%d}`, value.JobID)), nil
}

func (failingBatchCodec) Decode([]byte) (processPayload, error) {
	return processPayload{}, nil
}

func (failingBatchCodec) ContentType() string              { return testContentType }
func (failingBatchCodec) Encoding() messenger.DataEncoding { return messenger.DataJSON }

func TestBatchFacadeCanonicalizesDeliveriesBeforeRouteInvocation(t *testing.T) {
	t.Parallel()

	command := messenger.MustCommand("batch.fail.create", 1, failingBatchCodec{})
	event := messenger.MustEvent("batch.fail.created", 1, failingBatchCodec{})
	route := &recordingBatchRoute{name: outboxBatchRouteName}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.RouteCommand(command, route)
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	// 1. SendMessageBatch with unencodable item fails before route invocation.
	_, err = instance.SendMessageBatch(t.Context(), command, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 10}},
		{Payload: processPayload{JobID: -1}},
	})
	if err == nil {
		t.Fatal("expected error on unencodable command item, got nil")
	}
	if route.batchCalls != 0 {
		t.Fatalf("route was invoked %d times on unencodable command item, want 0", route.batchCalls)
	}

	// 2. PublishMessageBatch with unencodable item fails before route invocation.
	_, err = instance.PublishMessageBatch(t.Context(), event, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 20}},
		{Payload: processPayload{JobID: -2}},
	})
	if err == nil {
		t.Fatal("expected error on unencodable event item, got nil")
	}
	if route.batchCalls != 0 {
		t.Fatalf("route was invoked %d times on unencodable event item, want 0", route.batchCalls)
	}

	// 3. Valid batch succeeds and invokes route once.
	receipts, err := instance.SendMessageBatch(t.Context(), command, []messenger.Outgoing[processPayload]{
		{Payload: processPayload{JobID: 10}},
		{Payload: processPayload{JobID: 20}},
	})
	if err != nil {
		t.Fatalf("valid send message batch failed: %v", err)
	}
	if route.batchCalls != 1 || len(receipts) != 2 {
		t.Fatalf("expected 1 route call with 2 receipts, got calls=%d, receipts=%d", route.batchCalls, len(receipts))
	}
}

var _ messenger.BatchRoute = (*recordingBatchRoute)(nil)
