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

// LocalQueryRoute is the sealed local request/reply route contract. The built-in
// LocalSyncRoute and LocalAsyncRoute are its only implementations.
type LocalQueryRoute interface {
	Name() string
	query(ctx context.Context, call localQueryCall) (localQueryResult, error)
	requiresLocalQueryRoute()
}

type delivery struct {
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
	return d.invoke(contextWithMetadata(ctx, d.metadata))
}

type localRoute interface {
	Route
	requiresLocalHandler()
}
