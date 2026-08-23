package outboxadapter_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/shared/types"

	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
)

const (
	testEventName   = "media.processed"
	testSource      = "urn:service:test"
	testContentType = "application/json"
)

type transactionKey struct{}

type stagedCall struct {
	key         string
	name        string
	version     coreoutbox.SchemaVersion
	payload     string
	availableAt time.Time
	transaction bool
}

type fakeUniquePutter struct {
	calls []stagedCall
	seen  map[string]coreoutbox.UniquePutResult
}

func (p *fakeUniquePutter) PutVersionedUnique(
	ctx context.Context,
	key string,
	name string,
	version coreoutbox.SchemaVersion,
	payload string,
	availableAt time.Time,
) (coreoutbox.UniquePutResult, error) {
	p.calls = append(p.calls, stagedCall{
		key: key, name: name, version: version, payload: payload, availableAt: availableAt,
		transaction: ctx.Value(transactionKey{}) == "tx-1",
	})
	if result, ok := p.seen[key]; ok {
		result.Created = false
		return result, nil
	}
	result := coreoutbox.UniquePutResult{JobID: types.NewJobID(), Created: true}
	p.seen[key] = result
	return result, nil
}

type staticDelivery struct {
	metadata messenger.Metadata
	envelope []byte
}

func (d staticDelivery) Metadata() messenger.Metadata { return d.metadata }
func (d staticDelivery) HandlerCount() int            { return 0 }
func (d staticDelivery) MarshalEnvelope() ([]byte, error) {
	return append([]byte(nil), d.envelope...), nil
}

func (d staticDelivery) Fingerprint() ([sha256.Size]byte, error) {
	return sha256.Sum256(d.envelope), nil
}
func (d staticDelivery) Invoke(context.Context) error { return errors.New("unexpected invoke") }

func TestProducerStagesCanonicalEnvelopeWithStableIdempotency(t *testing.T) {
	metadata, envelope := testEnvelope(t)
	putter := &fakeUniquePutter{seen: make(map[string]coreoutbox.UniquePutResult)}
	producer, err := outboxadapter.NewProducer(putter, outboxadapter.ProducerConfig{Name: "outbox.events"})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	ctx := context.WithValue(t.Context(), transactionKey{}, "tx-1")
	delivery := staticDelivery{metadata: metadata, envelope: envelope}
	for range 2 {
		receipt, deliverErr := producer.Deliver(ctx, delivery)
		if deliverErr != nil {
			t.Fatalf("deliver: %v", deliverErr)
		}
		if receipt.State != messenger.ReceiptStaged || receipt.MessageID != metadata.ID {
			t.Fatalf("receipt = %#v", receipt)
		}
	}
	if len(putter.calls) != 2 {
		t.Fatalf("calls = %d", len(putter.calls))
	}
	for _, call := range putter.calls {
		if call.key != metadata.ID.String() || call.name != outboxadapter.DefaultRelayJobName ||
			call.version != outboxadapter.RelayJobSchemaVersion || call.payload != string(envelope) ||
			!call.availableAt.Equal(metadata.NotBefore) || !call.transaction {
			t.Fatalf("staged call = %#v", call)
		}
	}
}

func TestProducerUsesImmutableMessageTimeForImmediateDelivery(t *testing.T) {
	metadata, envelope := testEnvelope(t)
	metadata.NotBefore = time.Time{}
	putter := &fakeUniquePutter{seen: make(map[string]coreoutbox.UniquePutResult)}
	producer, err := outboxadapter.NewProducer(putter, outboxadapter.ProducerConfig{Name: "outbox.events"})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	_, err = producer.Deliver(t.Context(), staticDelivery{metadata: metadata, envelope: envelope})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !putter.calls[0].availableAt.Equal(metadata.Time) {
		t.Fatalf("availableAt = %s, want %s", putter.calls[0].availableAt, metadata.Time)
	}
}

func testEnvelope(t *testing.T) (messenger.Metadata, []byte) {
	t.Helper()
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("parse id: %v", err)
	}
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
		Source: testSource, Time: time.Unix(1_700_000_000, 0).UTC(),
		CorrelationID: id, ContentType: testContentType,
		NotBefore: time.Unix(1_700_000_100, 0).UTC(),
	}
	envelope, err := messenger.MarshalEnvelope(metadata, []byte(`{"jobId":42}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return metadata, envelope
}

var _ coreoutbox.UniqueVersionedPutter = (*fakeUniquePutter)(nil)
