package messenger

import (
	"context"
	"fmt"
	"reflect"
)

// SendBatch atomically stages command payloads through a BatchRoute.
func (m *Messenger) SendBatch[T any](
	ctx context.Context,
	descriptor Command[T],
	payloads []T,
) ([]Receipt, error) {
	outgoing := make([]Outgoing[T], len(payloads))
	for index := range payloads {
		outgoing[index].Payload = payloads[index]
	}
	return m.SendMessageBatch(ctx, descriptor, outgoing)
}

// SendMessageBatch validates all command messages and atomically stages them
// through the configured BatchRoute.
func (m *Messenger) SendMessageBatch[T any](
	ctx context.Context,
	descriptor Command[T],
	outgoing []Outgoing[T],
) ([]Receipt, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	if len(outgoing) == 0 {
		return nil, fmt.Errorf("%w: empty command batch", ErrInvalidMessage)
	}
	binding, ok := m.commands[keyFor(descriptor.info)]
	if !ok || !sameDescriptor(binding.descriptor, descriptor.info, reflect.TypeFor[T]()) {
		return nil, fmt.Errorf("%w: command %s v%d", ErrDescriptorConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.route == nil {
		return nil, fmt.Errorf("%w: command %s v%d", ErrRouteNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	batchRoute, ok := binding.route.(BatchRoute)
	if !ok {
		return nil, fmt.Errorf("%w: route %s does not support command batches", ErrUnsupportedCapability, binding.route.Name())
	}
	deliveries := make([]Delivery, len(outgoing))
	seen := make(map[MessageID]struct{}, len(outgoing))
	for index := range outgoing {
		metadata, err := m.newMetadata(ctx, descriptor.info, outgoing[index].Metadata)
		if err != nil {
			return nil, fmt.Errorf("command batch item %d: %w", index, err)
		}
		if _, duplicate := seen[metadata.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate message ID %s in command batch", ErrInvalidMessage, metadata.ID)
		}
		seen[metadata.ID] = struct{}{}
		deliveries[index] = binding.delivery(metadata, outgoing[index].Payload)
	}
	return m.deliverBatch(ctx, batchRoute, deliveries)
}

// PublishBatch atomically stages event payloads through a BatchRoute.
func (m *Messenger) PublishBatch[T any](
	ctx context.Context,
	descriptor Event[T],
	payloads []T,
) ([]Receipt, error) {
	outgoing := make([]Outgoing[T], len(payloads))
	for index := range payloads {
		outgoing[index].Payload = payloads[index]
	}
	return m.PublishMessageBatch(ctx, descriptor, outgoing)
}

// PublishMessageBatch validates all event messages and atomically stages them
// through the configured BatchRoute.
func (m *Messenger) PublishMessageBatch[T any](
	ctx context.Context,
	descriptor Event[T],
	outgoing []Outgoing[T],
) ([]Receipt, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	if len(outgoing) == 0 {
		return nil, fmt.Errorf("%w: empty event batch", ErrInvalidMessage)
	}
	binding, ok := m.events[keyFor(descriptor.info)]
	if !ok || !sameDescriptor(binding.descriptor, descriptor.info, reflect.TypeFor[T]()) {
		return nil, fmt.Errorf("%w: event %s v%d", ErrDescriptorConflict,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	if binding.route == nil {
		return nil, fmt.Errorf("%w: event %s v%d", ErrRouteNotFound,
			descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	batchRoute, ok := binding.route.(BatchRoute)
	if !ok {
		return nil, fmt.Errorf("%w: route %s does not support event batches", ErrUnsupportedCapability, binding.route.Name())
	}
	deliveries := make([]Delivery, len(outgoing))
	seen := make(map[MessageID]struct{}, len(outgoing))
	for index := range outgoing {
		metadata, err := m.newMetadata(ctx, descriptor.info, outgoing[index].Metadata)
		if err != nil {
			return nil, fmt.Errorf("event batch item %d: %w", index, err)
		}
		if _, duplicate := seen[metadata.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate message ID %s in event batch", ErrInvalidMessage, metadata.ID)
		}
		seen[metadata.ID] = struct{}{}
		deliveries[index] = binding.delivery(metadata, outgoing[index].Payload)
	}
	return m.deliverBatch(ctx, batchRoute, deliveries)
}

func (m *Messenger) deliverBatch(
	ctx context.Context,
	route BatchRoute,
	deliveries []Delivery,
) ([]Receipt, error) {
	for index, delivery := range deliveries {
		if delivery == nil {
			return nil, fmt.Errorf("%w: nil delivery at index %d", ErrInvalidMessage, index)
		}
		if _, err := delivery.MarshalEnvelope(); err != nil {
			return nil, fmt.Errorf("batch delivery %d: %w", index, err)
		}
	}
	started := m.clock().UTC()
	receipts, err := route.DeliverBatch(ctx, deliveries)
	if err == nil && len(receipts) != len(deliveries) {
		err = fmt.Errorf("%w: batch route returned %d receipts for %d deliveries",
			ErrInvalidMessage, len(receipts), len(deliveries))
	}
	if err == nil {
		for index := range receipts {
			if normalizeErr := normalizeReceipt(
				&receipts[index], deliveries[index].Metadata(), route, m.clock,
			); normalizeErr != nil {
				err = fmt.Errorf("batch receipt %d: %w", index, normalizeErr)
				break
			}
		}
	}
	if m.observer != nil {
		for index, delivery := range deliveries {
			metadata := delivery.Metadata()
			var state ReceiptState
			if index < len(receipts) {
				state = receipts[index].State
			}
			observe(ctx, m.observer, Observation{
				Operation:     OperationDeliver,
				MessageID:     metadata.ID,
				Kind:          metadata.Kind,
				Name:          metadata.Name,
				SchemaVersion: metadata.SchemaVersion,
				Route:         route.Name(),
				State:         state,
				StartedAt:     started,
				Duration:      m.clock().UTC().Sub(started),
				Err:           err,
			})
		}
	}
	return receipts, err
}
