package outboxadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
)

// BatchEnvelopePublisher publishes a real broker batch and reports one result
// for every payload in input order.
type BatchEnvelopePublisher interface {
	PublishEnvelopeBatch(ctx context.Context, payloads [][]byte) ([]messenger.Receipt, []error, error)
}

// BatchRelayJob publishes canonical envelopes through one real batch call.
type BatchRelayJob struct {
	publisher BatchEnvelopePublisher
	config    RelayJobConfig
}

// NewBatchRelayJob constructs a real Outbox batch capability.
func NewBatchRelayJob(
	publisher BatchEnvelopePublisher,
	config RelayJobConfig,
) (*BatchRelayJob, error) {
	if publisher == nil {
		return nil, errors.New("messenger/outbox: nil batch envelope publisher")
	}
	validated, err := NewRelayJob(noopEnvelopePublisher{}, config)
	if err != nil {
		return nil, err
	}
	return &BatchRelayJob{publisher: publisher, config: validated.config}, nil
}

type noopEnvelopePublisher struct{}

func (noopEnvelopePublisher) PublishEnvelope(context.Context, []byte) (messenger.Receipt, error) {
	return messenger.Receipt{}, nil
}

func (j *BatchRelayJob) Name() string { return j.config.Name }

func (j *BatchRelayJob) SchemaVersion() coreoutbox.SchemaVersion { return j.config.SchemaVersion }

func (j *BatchRelayJob) ExecutionTimeout() time.Duration { return j.config.ExecutionTimeout }

func (j *BatchRelayJob) MaxAttempts() int { return j.config.MaxAttempts }

func (j *BatchRelayJob) HandleBatch(
	ctx context.Context,
	items []coreoutbox.BatchJobItem,
) (coreoutbox.BatchResult, error) {
	results := make([]coreoutbox.BatchItemResult, len(items))
	payloads := make([][]byte, 0, len(items))
	positions := make([]int, 0, len(items))
	for index, item := range items {
		results[index].JobID = item.JobID
		envelope, err := messenger.UnmarshalEnvelope([]byte(item.Payload))
		if err != nil {
			results[index].Err = coreoutbox.Permanent(fmt.Errorf("messenger/outbox: decode relay envelope: %w", err))
			continue
		}
		_ = envelope
		payloads = append(payloads, []byte(item.Payload))
		positions = append(positions, index)
	}
	if len(payloads) == 0 {
		return coreoutbox.BatchResult{Items: results}, nil
	}

	receipts, publishErrors, err := j.publisher.PublishEnvelopeBatch(ctx, payloads)
	if err != nil {
		for _, position := range positions {
			results[position].Err = relayBatchError(j.config, err)
		}
		return coreoutbox.BatchResult{Items: results}, nil
	}
	if len(receipts) != len(payloads) || len(publishErrors) != len(payloads) {
		return coreoutbox.BatchResult{}, coreoutbox.Permanent(fmt.Errorf(
			"messenger/outbox: batch publisher returned %d receipts and %d errors for %d payloads",
			len(receipts), len(publishErrors), len(payloads),
		))
	}
	for index, publicationErr := range publishErrors {
		position := positions[index]
		if publicationErr != nil {
			results[position].Err = relayBatchError(j.config, publicationErr)
		}
	}
	return coreoutbox.BatchResult{Items: results}, nil
}

func relayBatchError(config RelayJobConfig, err error) error {
	if messenger.IsPermanent(err) || errors.Is(err, messenger.ErrInvalidMessage) ||
		errors.Is(err, messenger.ErrDescriptorConflict) {
		return coreoutbox.Permanent(err)
	}
	if delay, ok := messenger.RetryDelay(err); ok {
		return coreoutbox.DeferAt(err, config.Clock().UTC().Add(delay))
	}
	return err
}

var (
	_ coreoutbox.BatchJob          = (*BatchRelayJob)(nil)
	_ coreoutbox.VersionedBatchJob = (*BatchRelayJob)(nil)
)
