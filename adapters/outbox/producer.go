package outboxadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
)

const (
	// DefaultRelayJobName is the stable capability consumed by RelayJob.
	DefaultRelayJobName = "gomessenger.relay"
	// RelayJobSchemaVersion is the native envelope relay payload schema.
	RelayJobSchemaVersion coreoutbox.SchemaVersion = 1
)

// ProducerConfig declares one durable staging route.
type ProducerConfig struct {
	Name      string
	JobName   string
	JobSchema coreoutbox.SchemaVersion
}

// Producer stages canonical envelopes with MessageID as the immutable outbox
// deduplication key. Transaction atomicity is supplied by the host's outbox
// repository through ctx.
type Producer struct {
	putter    coreoutbox.UniqueVersionedPutter
	name      string
	jobName   string
	jobSchema coreoutbox.SchemaVersion
}

// NewProducer constructs a durable outbox route.
func NewProducer(
	putter coreoutbox.UniqueVersionedPutter,
	config ProducerConfig,
) (*Producer, error) {
	if putter == nil || config.Name == "" {
		return nil, errors.New("messenger/outbox: invalid producer configuration")
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
	return &Producer{
		putter: putter, name: config.Name, jobName: config.JobName, jobSchema: config.JobSchema,
	}, nil
}

// Name implements messenger.Route.
func (p *Producer) Name() string { return p.name }

// Deliver stages the canonical envelope in the caller's transaction.
func (p *Producer) Deliver(
	ctx context.Context,
	delivery messenger.Delivery,
) (messenger.Receipt, error) {
	if delivery == nil {
		return messenger.Receipt{}, errors.New("messenger/outbox: nil delivery")
	}
	metadata := delivery.Metadata()
	envelope, err := delivery.MarshalEnvelope()
	if err != nil {
		return messenger.Receipt{}, err
	}
	availableAt := metadata.NotBefore
	if availableAt.IsZero() {
		// Message time is immutable, unlike the relay call's wall clock, so an
		// idempotent retry cannot drift the outbox fingerprint.
		availableAt = metadata.Time
	}
	result, err := p.putter.PutVersionedUnique(
		ctx,
		metadata.ID.String(),
		p.jobName,
		p.jobSchema,
		string(envelope),
		availableAt.UTC(),
	)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("messenger/outbox: stage %s: %w", metadata.ID, err)
	}
	if result.JobID.IsZero() {
		return messenger.Receipt{}, errors.New("messenger/outbox: outbox returned an empty job ID")
	}
	return messenger.Receipt{
		MessageID: metadata.ID,
		Route:     p.name,
		State:     messenger.ReceiptStaged,
		At:        time.Now().UTC(),
	}, nil
}

var _ messenger.Route = (*Producer)(nil)
