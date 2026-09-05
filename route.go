package messenger

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// ReceiptState describes what a successful route call has guaranteed.
type ReceiptState string

const (
	// ReceiptCompleted means local synchronous handlers completed.
	ReceiptCompleted ReceiptState = "completed"
	// ReceiptAccepted means bounded in-process async admission succeeded.
	ReceiptAccepted ReceiptState = "accepted"
	// ReceiptStaged means an outbox write succeeded in the current transaction.
	ReceiptStaged ReceiptState = "staged"
	// ReceiptBrokerConfirmed means the broker confirmed persistence.
	ReceiptBrokerConfirmed ReceiptState = "broker_confirmed"
	// ReceiptNoop means a local event had no subscribers.
	ReceiptNoop ReceiptState = "noop"
)

// Receipt describes the guarantee reached by one primary route.
type Receipt struct {
	MessageID MessageID    `json:"messageId"`
	Route     string       `json:"route"`
	State     ReceiptState `json:"state"`
	At        time.Time    `json:"at"`
}

// Delivery is the transport-neutral route input. MarshalEnvelope is lazy, so
// local routes do not serialize payloads. Invoke is intended for local and
// terminal durable adapters.
type Delivery interface {
	Metadata() Metadata
	HandlerCount() int
	MarshalEnvelope() ([]byte, error)
	Fingerprint() ([sha256.Size]byte, error)
	Invoke(ctx context.Context) error
}

// Route is one static primary delivery route.
type Route interface {
	Name() string
	Deliver(ctx context.Context, delivery Delivery) (Receipt, error)
}

// BatchRoute atomically delivers an ordered set of command/event deliveries.
// The durable Outbox route is the supported implementation; direct broker and
// local routes intentionally do not implement this capability.
type BatchRoute interface {
	Route
	DeliverBatch(ctx context.Context, deliveries []Delivery) ([]Receipt, error)
}

// LocalQueryRoute is the sealed local request/reply route contract. The built-in
// LocalSyncRoute and LocalAsyncRoute are its only implementations.
type LocalQueryRoute interface {
	Name() string
	query(ctx context.Context, call localQueryCall) (localQueryResult, error)
	requiresLocalQueryRoute()
}

type delivery struct {
	onExpire func(context.Context, error)
	metadata Metadata
	encode   func() ([]byte, DataEncoding, error)
	invoke   func(context.Context) error
	handlers int

	once        sync.Once
	envelope    []byte
	envelopeErr error
}

func (d *delivery) Metadata() Metadata { return cloneMetadata(d.metadata) }
func (d *delivery) HandlerCount() int  { return d.handlers }

func (d *delivery) MarshalEnvelope() ([]byte, error) {
	d.once.Do(func() {
		payload, encoding, err := d.encode()
		if err != nil {
			d.envelopeErr = fmt.Errorf("messenger: encode %s %s v%d: %w",
				d.metadata.Kind, d.metadata.Name, d.metadata.SchemaVersion, err)
			return
		}
		d.envelope, d.envelopeErr = MarshalEnvelope(d.metadata, payload, encoding)
	})
	return append([]byte(nil), d.envelope...), d.envelopeErr
}

func (d *delivery) Fingerprint() ([sha256.Size]byte, error) {
	data, err := d.MarshalEnvelope()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return EnvelopeFingerprint(data), nil
}

func (d *delivery) Invoke(ctx context.Context) error {
	if ctx == nil || d.invoke == nil {
		return fmt.Errorf("%w: invalid delivery invocation", ErrInvalidMessage)
	}
	return d.invoke(contextWithMetadata(ctx, d.metadata))
}

func validateImmediateTiming(metadata Metadata, now time.Time) error {
	now = now.UTC()
	if !metadata.ExpiresAt.IsZero() && !metadata.ExpiresAt.After(now) {
		return Permanent(ErrMessageExpired)
	}
	if !metadata.NotBefore.IsZero() && metadata.NotBefore.After(now) {
		return fmt.Errorf("%w: local routes cannot schedule delivery for %s: %w",
			ErrUnsupportedCapability, metadata.NotBefore.UTC().Format(time.RFC3339Nano), ErrMessageNotReady)
	}
	return nil
}

type localRoute interface {
	Route
	requiresLocalHandler()
}

func (d *delivery) reportExpiry(ctx context.Context, err error) {
	if d.onExpire != nil {
		d.onExpire(ctx, err)
	}
}

func expiryObserver(observer Observer, metadata Metadata, route string) func(context.Context, error) {
	if observer == nil {
		return nil
	}
	return func(ctx context.Context, err error) {
		observe(ctx, observer, Observation{
			Operation: OperationExpire,
			MessageID: metadata.ID, Kind: metadata.Kind, Name: metadata.Name, SchemaVersion: metadata.SchemaVersion,
			Route: route, StartedAt: time.Now().UTC(), Err: err,
		})
	}
}
