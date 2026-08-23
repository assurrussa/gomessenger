package outboxadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
)

// EnvelopePublisher is the narrow broker-confirmed surface used by RelayJob.
type EnvelopePublisher interface {
	PublishEnvelope(ctx context.Context, payload []byte) (messenger.Receipt, error)
}

// RelayJobConfig controls the outbox worker capability and retry bounds.
type RelayJobConfig struct {
	Name             string
	SchemaVersion    coreoutbox.SchemaVersion
	ExecutionTimeout time.Duration
	MaxAttempts      int
	Clock            func() time.Time
}

// RelayJob publishes one persisted native envelope without changing its
// identity or lineage.
type RelayJob struct {
	publisher EnvelopePublisher
	config    RelayJobConfig
}

// NewRelayJob constructs an outbox capability handler.
func NewRelayJob(publisher EnvelopePublisher, config RelayJobConfig) (*RelayJob, error) {
	if publisher == nil {
		return nil, errors.New("messenger/outbox: nil envelope publisher")
	}
	if config.Name == "" {
		config.Name = DefaultRelayJobName
	}
	if config.SchemaVersion == 0 {
		config.SchemaVersion = RelayJobSchemaVersion
	}
	if config.ExecutionTimeout == 0 {
		config.ExecutionTimeout = 30 * time.Second
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 10
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.SchemaVersion < 1 || config.ExecutionTimeout <= 0 || config.MaxAttempts < 1 {
		return nil, errors.New("messenger/outbox: invalid relay job configuration")
	}
	return &RelayJob{publisher: publisher, config: config}, nil
}

func (j *RelayJob) Name() string { return j.config.Name }

func (j *RelayJob) SchemaVersion() coreoutbox.SchemaVersion { return j.config.SchemaVersion }

func (j *RelayJob) ExecutionTimeout() time.Duration { return j.config.ExecutionTimeout }

func (j *RelayJob) MaxAttempts() int { return j.config.MaxAttempts }

func (j *RelayJob) Handle(ctx context.Context, payload string) error {
	envelope, err := messenger.UnmarshalEnvelope([]byte(payload))
	if err != nil {
		return coreoutbox.Permanent(fmt.Errorf("messenger/outbox: decode relay envelope: %w", err))
	}
	ctx = messenger.ContextWithMetadata(ctx, envelope.Metadata())
	_, err = j.publisher.PublishEnvelope(ctx, []byte(payload))
	if err == nil {
		return nil
	}
	if messenger.IsPermanent(err) || errors.Is(err, messenger.ErrInvalidMessage) ||
		errors.Is(err, messenger.ErrDescriptorConflict) {
		return coreoutbox.Permanent(err)
	}
	if delay, ok := messenger.RetryDelay(err); ok {
		return coreoutbox.RetryAt(err, j.config.Clock().UTC().Add(delay))
	}
	return err
}

var (
	_ coreoutbox.Job          = (*RelayJob)(nil)
	_ coreoutbox.VersionedJob = (*RelayJob)(nil)
)
