package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RouteConfig declares one native-envelope Kafka producer route.
type RouteConfig struct {
	Name      string
	Namespace string
}

// Route synchronously publishes native envelopes in committed Kafka transactions.
type Route struct {
	transport *Transport
	name      string
	namespace string
	clock     func() time.Time
}

// NewRoute constructs a route backed by the managed transport.
func NewRoute(transport *Transport, config RouteConfig) (*Route, error) {
	if transport == nil || config.Name == "" {
		return nil, fmt.Errorf("%w: producer route", ErrInvalidConfig)
	}
	if err := validateKafkaToken("route name", config.Name); err != nil {
		return nil, err
	}
	if _, err := Topic(config.Namespace, messenger.DescriptorInfo{
		Kind: messenger.KindEvent, Name: "probe", SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}
	return &Route{transport: transport, name: config.Name, namespace: config.Namespace, clock: time.Now}, nil
}

// Name implements messenger.Route.
func (r *Route) Name() string { return r.name }

// ManagedService contributes the shared transport to Builder runtime aggregation.
func (r *Route) ManagedService() (string, messenger.Service) {
	return r.transport.Name(), r.transport
}

// Deliver publishes one command or event and waits for transaction commit.
func (r *Route) Deliver(ctx context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
	if delivery == nil {
		return messenger.Receipt{}, fmt.Errorf("%w: nil Kafka delivery", messenger.ErrInvalidMessage)
	}
	metadata, native, err := decodeDeliveryEnvelope(delivery)
	if err != nil {
		return messenger.Receipt{}, err
	}
	if metadata.Kind != messenger.KindCommand && metadata.Kind != messenger.KindEvent {
		return messenger.Receipt{}, fmt.Errorf("%w: Kafka route only supports command/event delivery",
			messenger.ErrInvalidMessage)
	}
	return r.publishEnvelope(ctx, metadata, native)
}

func decodeDeliveryEnvelope(delivery messenger.Delivery) (messenger.Metadata, []byte, error) {
	native, err := delivery.MarshalEnvelope()
	if err != nil {
		return messenger.Metadata{}, nil, err
	}
	envelope, err := messenger.UnmarshalEnvelope(native)
	if err != nil {
		return messenger.Metadata{}, nil, err
	}
	return envelope.Metadata(), native, nil
}

// PublishEnvelope publishes an already encoded canonical envelope. It is the
// broker-confirmed surface used by transactional Outbox relay jobs.
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
	if err := r.transport.ensureRunning(); err != nil {
		return messenger.Receipt{}, err
	}
	now := r.clock().UTC()
	if !metadata.ExpiresAt.IsZero() && !metadata.ExpiresAt.After(now) {
		return messenger.Receipt{}, messenger.Permanent(ErrMessageExpired)
	}
	if !metadata.NotBefore.IsZero() && metadata.NotBefore.After(now) {
		return messenger.Receipt{}, messenger.RetryAfter(ErrMessageNotReady, metadata.NotBefore.Sub(now))
	}
	topic, err := Topic(r.namespace, messenger.DescriptorInfo{
		Kind: metadata.Kind, Name: metadata.Name, SchemaVersion: metadata.SchemaVersion,
	})
	if err != nil {
		return messenger.Receipt{}, messenger.Permanent(err)
	}
	if ctx == nil {
		return messenger.Receipt{}, fmt.Errorf("%w: nil publish context", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return messenger.Receipt{}, err
	}
	if err := r.transport.acquireTransaction(ctx); err != nil {
		return messenger.Receipt{}, err
	}
	defer r.transport.releaseTransaction()
	now = r.clock().UTC()
	if !metadata.ExpiresAt.IsZero() && !metadata.ExpiresAt.After(now) {
		return messenger.Receipt{}, messenger.Permanent(ErrMessageExpired)
	}
	brokerCtx, cancel := r.transport.brokerContext(ctx)
	defer cancel()
	if err := r.transport.client.BeginTransaction(); err != nil {
		failure := fmt.Errorf("messenger/kafka: begin publish transaction: %w", err)
		r.transport.logFailure(ctx, messenger.LogError, "Kafka transaction failed", "publish_begin", failure,
			messenger.LogAttr{Key: logAttrRoute, Value: r.name},
			messenger.LogAttr{Key: logAttrTopic, Value: topic})
		return messenger.Receipt{}, failure
	}
	record := newRouteRecord(topic, metadata, native)
	if err := r.transport.client.ProduceSync(brokerCtx, record).FirstErr(); err != nil {
		abortErr := abortTransaction(ctx, r.transport.config.OperationTimeout, r.transport.client)
		failure := fmt.Errorf("messenger/kafka: publish %s: %w", topic, err)
		r.transport.logTransactionFailure(ctx, "publish_produce", failure, abortErr,
			messenger.LogAttr{Key: logAttrRoute, Value: r.name},
			messenger.LogAttr{Key: logAttrTopic, Value: topic})
		return messenger.Receipt{}, errors.Join(failure, abortErr)
	}
	if err := r.transport.client.EndTransaction(brokerCtx, kgo.TryCommit); err != nil {
		abortErr := abortTransaction(ctx, r.transport.config.OperationTimeout, r.transport.client)
		failure := fmt.Errorf("messenger/kafka: commit publish %s: %w", topic, err)
		r.transport.logTransactionFailure(ctx, "publish_commit", failure, abortErr,
			messenger.LogAttr{Key: logAttrRoute, Value: r.name},
			messenger.LogAttr{Key: logAttrTopic, Value: topic})
		return messenger.Receipt{}, errors.Join(failure, abortErr)
	}
	return messenger.Receipt{
		MessageID: metadata.ID,
		Route:     r.name,
		State:     messenger.ReceiptBrokerConfirmed,
		At:        r.clock().UTC(),
	}, nil
}

func newRouteRecord(topic string, metadata messenger.Metadata, native []byte) *kgo.Record {
	// Leave Timestamp zero so franz-go assigns broker publication time. The
	// immutable logical creation time remains inside the canonical envelope.
	return &kgo.Record{
		Topic: topic,
		Key:   expectedRecordKey(metadata),
		Value: append([]byte(nil), native...),
	}
}

var (
	_ messenger.Route           = (*Route)(nil)
	_ messenger.ServiceProvider = (*Route)(nil)
)
