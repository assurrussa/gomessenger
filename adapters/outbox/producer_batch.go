package outboxadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
)

// BatchProducer is the transactional-Outbox-only atomic batch route.
type BatchProducer struct {
	putter    coreoutbox.UniqueBatchVersionedPutter
	name      string
	jobName   string
	jobSchema coreoutbox.SchemaVersion
}

// NewBatchProducer constructs an Outbox route whose singleton and multi-item
// calls both use the same atomic batch staging path.
func NewBatchProducer(
	putter coreoutbox.UniqueBatchVersionedPutter,
	config ProducerConfig,
) (*BatchProducer, error) {
	if putter == nil || config.Name == "" {
		return nil, errors.New("messenger/outbox: invalid batch producer configuration")
	}
	if config.JobName == "" {
		config.JobName = DefaultRelayJobName
	}
	if config.JobSchema == 0 {
		config.JobSchema = RelayJobSchemaVersion
	}
	if config.JobSchema < 1 {
		return nil, errors.New("messenger/outbox: job schema must be positive")
	}
	return &BatchProducer{
		putter: putter, name: config.Name, jobName: config.JobName, jobSchema: config.JobSchema,
	}, nil
}

func (p *BatchProducer) Name() string { return p.name }

// Deliver stages one delivery through the batch path.
func (p *BatchProducer) Deliver(
	ctx context.Context,
	delivery messenger.Delivery,
) (messenger.Receipt, error) {
	receipts, err := p.DeliverBatch(ctx, []messenger.Delivery{delivery})
	if err != nil {
		return messenger.Receipt{}, err
	}
	return receipts[0], nil
}

// DeliverBatch validates and atomically stages every canonical envelope.
func (p *BatchProducer) DeliverBatch(
	ctx context.Context,
	deliveries []messenger.Delivery,
) ([]messenger.Receipt, error) {
	if len(deliveries) == 0 {
		return nil, errors.New("messenger/outbox: empty delivery batch")
	}
	puts := make([]coreoutbox.UniqueBatchPut, len(deliveries))
	messageIDs := make([]messenger.MessageID, len(deliveries))
	seen := make(map[messenger.MessageID]struct{}, len(deliveries))
	for index, delivery := range deliveries {
		if delivery == nil {
			return nil, fmt.Errorf("messenger/outbox: nil delivery at index %d", index)
		}
		metadata := delivery.Metadata()
		if metadata.Kind != messenger.KindCommand && metadata.Kind != messenger.KindEvent {
			return nil, fmt.Errorf("%w: outbox only supports command/event delivery", messenger.ErrInvalidMessage)
		}
		if _, duplicate := seen[metadata.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate message ID %s", messenger.ErrInvalidMessage, metadata.ID)
		}
		seen[metadata.ID] = struct{}{}
		envelope, err := delivery.MarshalEnvelope()
		if err != nil {
			return nil, fmt.Errorf("messenger/outbox: encode batch item %d: %w", index, err)
		}
		availableAt := metadata.NotBefore
		if availableAt.IsZero() {
			availableAt = metadata.Time
		}
		puts[index] = coreoutbox.UniqueBatchPut{
			DeduplicationKey: metadata.ID.String(),
			Name:             p.jobName,
			SchemaVersion:    p.jobSchema,
			Payload:          string(envelope),
			AvailableAt:      availableAt.UTC(),
		}
		messageIDs[index] = metadata.ID
	}

	results, err := p.putter.PutVersionedUniqueBatch(ctx, puts)
	if err != nil {
		return nil, fmt.Errorf("messenger/outbox: stage batch: %w", err)
	}
	if len(results) != len(puts) {
		return nil, fmt.Errorf("messenger/outbox: outbox returned %d results for %d items", len(results), len(puts))
	}
	receipts := make([]messenger.Receipt, len(results))
	now := time.Now().UTC()
	for index, result := range results {
		if result.JobID.IsZero() {
			return nil, fmt.Errorf("messenger/outbox: outbox returned an empty job ID at index %d", index)
		}
		receipts[index] = messenger.Receipt{
			MessageID: messageIDs[index],
			Route:     p.name,
			State:     messenger.ReceiptStaged,
			At:        now,
		}
	}
	return receipts, nil
}

var (
	_ messenger.Route      = (*BatchProducer)(nil)
	_ messenger.BatchRoute = (*BatchProducer)(nil)
)
