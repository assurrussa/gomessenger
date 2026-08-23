package outboxadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"

	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
)

type fakeEnvelopePublisher struct {
	err      error
	payload  []byte
	metadata messenger.Metadata
}

func (p *fakeEnvelopePublisher) PublishEnvelope(
	ctx context.Context,
	payload []byte,
) (messenger.Receipt, error) {
	p.payload = append([]byte(nil), payload...)
	p.metadata, _ = messenger.MetadataFromContext(ctx)
	return messenger.Receipt{}, p.err
}

func TestRelayJobPublishesPersistedEnvelopeWithoutChangingLineage(t *testing.T) {
	metadata, envelope := testEnvelope(t)
	publisher := &fakeEnvelopePublisher{}
	job, err := outboxadapter.NewRelayJob(publisher, outboxadapter.RelayJobConfig{})
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if err := job.Handle(t.Context(), string(envelope)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if string(publisher.payload) != string(envelope) || publisher.metadata.ID != metadata.ID ||
		publisher.metadata.CorrelationID != metadata.CorrelationID {
		t.Fatalf("publisher payload/metadata changed: %#v", publisher.metadata)
	}
	if job.Name() != outboxadapter.DefaultRelayJobName || job.SchemaVersion() != outboxadapter.RelayJobSchemaVersion {
		t.Fatalf("job capability = %s v%d", job.Name(), job.SchemaVersion())
	}
}

func TestRelayJobMapsPermanentAndScheduledRetryDispositions(t *testing.T) {
	_, envelope := testEnvelope(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	publisher := &fakeEnvelopePublisher{err: messenger.RetryAfter(errors.New("busy"), 5*time.Second)}
	job, err := outboxadapter.NewRelayJob(publisher, outboxadapter.RelayJobConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	err = job.Handle(t.Context(), string(envelope))
	retryAt, ok := coreoutbox.RetryTime(err)
	if !ok || !retryAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("retry disposition = %s, %v", retryAt, err)
	}

	publisher.err = messenger.Permanent(errors.New("rejected"))
	err = job.Handle(t.Context(), string(envelope))
	if !coreoutbox.IsPermanent(err) {
		t.Fatalf("permanent disposition = %v", err)
	}

	err = job.Handle(t.Context(), `{"invalid":true}`)
	if !coreoutbox.IsPermanent(err) {
		t.Fatalf("invalid envelope disposition = %v", err)
	}
}

var _ outboxadapter.EnvelopePublisher = (*fakeEnvelopePublisher)(nil)
