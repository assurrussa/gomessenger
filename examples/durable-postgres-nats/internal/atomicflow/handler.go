// Package atomicflow demonstrates a PostgreSQL Inbox handler that commits its
// business projection and one outgoing Outbox event in the same SQL transaction.
package atomicflow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsrepo"
)

const (
	consumerID   = "atomic-order-projection-v1"
	inputSource  = "urn:service:atomic-orders"
	outputSource = "urn:service:atomic-projection"
)

// OrderCreated is the incoming business event.
type OrderCreated struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

// OrderProjected is the outgoing event staged alongside the projection.
type OrderProjected struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

func createdEvent() messenger.Event[OrderCreated] {
	return messenger.MustEvent("orders.created", 1, messenger.JSON[OrderCreated]())
}

func projectedEvent() messenger.Event[OrderProjected] {
	return messenger.MustEvent("orders.projected", 1, messenger.JSON[OrderProjected]())
}

// Handle runs inside the PostgreSQL Inbox callback. It never opens a connection,
// starts another transaction, commits, or falls back to broker publication.
// Its staged receipt remains provisional until ProcessAttempt commits.
func Handle(ctx context.Context, message messenger.Message[OrderCreated]) (messenger.Receipt, error) {
	if ctx == nil {
		return messenger.Receipt{}, errors.New("atomic example: nil handler context")
	}
	active, ok := inbox.SQLTxFromContext(ctx)
	if !ok {
		return messenger.Receipt{}, errors.New("atomic example: missing Inbox transaction")
	}
	tx, ok := active.(*sql.Tx)
	if !ok || tx == nil {
		return messenger.Receipt{}, errors.New("atomic example: PostgreSQL Inbox requires *sql.Tx")
	}
	if message.Metadata.ID.IsZero() || message.Metadata.Source == "" || message.Metadata.Time.IsZero() ||
		message.Payload.OrderID == "" || message.Payload.Amount <= 0 {
		return messenger.Receipt{}, errors.New("atomic example: invalid incoming order")
	}

	// The official backend adapter binds to this exact transaction, including
	// the Inbox savepoint and the host-configured PostgreSQL search_path.
	putter, err := jobsrepo.NewSQLTxPutter(tx)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("bind Outbox transaction: %w", err)
	}
	producer, err := outboxadapter.NewProducer(putter, outboxadapter.ProducerConfig{
		Name: "outbox.atomic.projected",
	})
	if err != nil {
		return messenger.Receipt{}, err
	}
	event := projectedEvent()
	builder := messenger.NewBuilder(messenger.WithSource(outputSource))
	builder.RouteEvent(event, producer)
	bus, _, err := builder.Build()
	if err != nil {
		return messenger.Receipt{}, err
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO atomic_order_projection (order_id, message_id, amount)
		VALUES ($1, $2, $3)`, message.Payload.OrderID, message.Metadata.ID.String(), message.Payload.Amount); err != nil {
		return messenger.Receipt{}, fmt.Errorf("write atomic order projection: %w", err)
	}
	correlationID := message.Metadata.CorrelationID
	if correlationID.IsZero() {
		correlationID = message.Metadata.ID
	}
	return bus.PublishMessage(ctx, event, messenger.Outgoing[OrderProjected]{
		Payload: OrderProjected{OrderID: message.Payload.OrderID, Amount: message.Payload.Amount},
		Metadata: messenger.OutgoingMetadata{
			ID: projectedID(message.Metadata), Time: message.Metadata.Time,
			CorrelationID: correlationID, CausationID: message.Metadata.ID,
			Key: message.Payload.OrderID,
		},
	})
}

// One deterministic UUIDv8 identifies this consumer's one output event for the
// logical input identity. Retry generations and wall-clock time do not enter it.
func projectedID(metadata messenger.Metadata) messenger.MessageID {
	hash := sha256.New()
	_, _ = hash.Write([]byte("gomessenger/atomic-order-projection-v1/orders.projected/v1\x00"))
	_, _ = hash.Write([]byte(metadata.Source))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(metadata.ID[:])
	var id messenger.MessageID
	copy(id[:], hash.Sum(nil))
	id[6] = (id[6] & 0x0f) | 0x80
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}
