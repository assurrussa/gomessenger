package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// WireMode selects the payload binding used on a NATS subject.
type WireMode string

const (
	// WireNative publishes the canonical gomessenger envelope.
	WireNative WireMode = "native"
	// WireCloudEventsStructured publishes a structured CloudEvents JSON event.
	WireCloudEventsStructured WireMode = "cloudevents-structured"
	// WireCloudEventsBinary publishes CloudEvents attributes as NATS headers.
	WireCloudEventsBinary WireMode = "cloudevents-binary"
)

// RouteConfig declares one direct JetStream producer route.
type RouteConfig struct {
	Name      string
	Namespace string
	WireMode  WireMode
}

// Route synchronously publishes and waits for a JetStream PubAck.
type Route struct {
	name      string
	namespace string
	mode      WireMode
	js        jetstream.JetStream
	clock     func() time.Time
}

// NewRoute constructs a producer from a host-owned NATS connection.
func NewRoute(connection *natsio.Conn, config RouteConfig) (*Route, error) {
	if connection == nil || config.Name == "" || config.Namespace == "" || !config.WireMode.valid() {
		return nil, fmt.Errorf("%w: producer route", ErrInvalidConfig)
	}
	if _, err := Subject(config.Namespace, messenger.DescriptorInfo{
		Kind: messenger.KindEvent, Name: "probe", SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}
	js, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: create JetStream context: %w", err)
	}
	return &Route{
		name: config.Name, namespace: config.Namespace, mode: config.WireMode,
		js: js, clock: time.Now,
	}, nil
}

// Name implements messenger.Route.
func (r *Route) Name() string { return r.name }

// Deliver publishes one message and waits for broker persistence confirmation.
func (r *Route) Deliver(ctx context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
	if delivery == nil {
		return messenger.Receipt{}, fmt.Errorf("%w: nil NATS delivery", messenger.ErrInvalidMessage)
	}
	metadata := delivery.Metadata()
	if metadata.Kind != messenger.KindCommand && metadata.Kind != messenger.KindEvent {
		return messenger.Receipt{}, fmt.Errorf("%w: NATS route only supports command/event delivery", messenger.ErrInvalidMessage)
	}
	native, err := delivery.MarshalEnvelope()
	if err != nil {
		return messenger.Receipt{}, err
	}
	return r.publishEnvelope(ctx, metadata, native)
}

// PublishEnvelope publishes an already encoded native envelope and waits for
// JetStream persistence confirmation. It is intended for durable outbox relay
// jobs that must not rebuild message metadata.
func (r *Route) PublishEnvelope(ctx context.Context, data []byte) (messenger.Receipt, error) {
	canonical, err := messenger.CanonicalizeEnvelope(data)
	if err != nil {
		return messenger.Receipt{}, err
	}
	envelope, err := messenger.UnmarshalEnvelope(canonical)
	if err != nil {
		return messenger.Receipt{}, err
	}
	return r.publishEnvelope(ctx, envelope.Metadata(), canonical)
}

func (r *Route) publishEnvelope(
	ctx context.Context,
	metadata messenger.Metadata,
	native []byte,
) (messenger.Receipt, error) {
	now := r.clock().UTC()
	if !metadata.ExpiresAt.IsZero() && !metadata.ExpiresAt.After(now) {
		return messenger.Receipt{}, messenger.Permanent(ErrMessageExpired)
	}
	if !metadata.NotBefore.IsZero() && metadata.NotBefore.After(now) {
		return messenger.Receipt{}, messenger.RetryAfter(ErrMessageNotReady, metadata.NotBefore.Sub(now))
	}
	if r.mode != WireNative && metadata.Kind != messenger.KindEvent {
		return messenger.Receipt{}, fmt.Errorf("%w: CloudEvents mode only supports events", ErrInvalidConfig)
	}
	subject, err := Subject(r.namespace, messenger.DescriptorInfo{
		Kind: metadata.Kind, Name: metadata.Name, SchemaVersion: metadata.SchemaVersion,
	})
	if err != nil {
		return messenger.Receipt{}, err
	}
	message := &natsio.Msg{Subject: subject, Header: make(natsio.Header)}
	switch r.mode {
	case WireNative:
		message.Header.Set("Content-Type", "application/vnd.gomessenger+json; version=1.0")
		message.Data = native
	case WireCloudEventsStructured:
		message.Data, err = encodeCloudEventStructured(native)
		message.Header.Set("Content-Type", "application/cloudevents+json; charset=utf-8")
	case WireCloudEventsBinary:
		message.Data, err = encodeCloudEventBinary(native, message.Header)
	default:
		err = fmt.Errorf("%w: wire mode %q", ErrInvalidConfig, r.mode)
	}
	if err != nil {
		return messenger.Receipt{}, err
	}
	ack, err := r.js.PublishMsg(ctx, message, jetstream.WithMsgID(metadata.ID.String()))
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("messenger/nats: publish %s: %w", subject, err)
	}
	if ack == nil || ack.Stream == "" {
		return messenger.Receipt{}, errors.New("messenger/nats: broker returned an empty publish acknowledgement")
	}
	return messenger.Receipt{
		MessageID: metadata.ID,
		Route:     r.name,
		State:     messenger.ReceiptBrokerConfirmed,
		At:        now,
	}, nil
}

func (mode WireMode) valid() bool {
	return mode == WireNative || mode == WireCloudEventsStructured || mode == WireCloudEventsBinary
}
