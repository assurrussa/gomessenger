package outboxadapter_test

import (
	"context"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/shared/types"

	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
)

type fakeUniqueBatchPutter struct {
	calls [][]coreoutbox.UniqueBatchPut
}

func (p *fakeUniqueBatchPutter) PutVersionedUniqueBatch(
	_ context.Context,
	items []coreoutbox.UniqueBatchPut,
) ([]coreoutbox.UniquePutResult, error) {
	p.calls = append(p.calls, append([]coreoutbox.UniqueBatchPut(nil), items...))
	results := make([]coreoutbox.UniquePutResult, len(items))
	for index := range results {
		results[index] = coreoutbox.UniquePutResult{JobID: types.NewJobID(), Created: true}
	}
	return results, nil
}

func TestBatchProducerStagesAllEnvelopesInOneOrderedCall(t *testing.T) {
	firstMetadata, firstEnvelope := testEnvelope(t)
	secondMetadata := firstMetadata
	secondMetadata.ID, _ = messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000002")
	secondMetadata.CorrelationID = secondMetadata.ID
	secondEnvelope, err := messenger.MarshalEnvelope(secondMetadata, []byte(`{"jobId":43}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("second envelope: %v", err)
	}
	putter := &fakeUniqueBatchPutter{}
	producer, err := outboxadapter.NewBatchProducer(putter, outboxadapter.ProducerConfig{Name: testProducerName})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	receipts, err := producer.DeliverBatch(t.Context(), []messenger.Delivery{
		staticDelivery{metadata: firstMetadata, envelope: firstEnvelope},
		staticDelivery{metadata: secondMetadata, envelope: secondEnvelope},
	})
	if err != nil {
		t.Fatalf("deliver batch: %v", err)
	}
	if len(putter.calls) != 1 || len(putter.calls[0]) != 2 || len(receipts) != 2 {
		t.Fatalf("calls/items/receipts = %d/%d/%d", len(putter.calls), len(putter.calls[0]), len(receipts))
	}
	if putter.calls[0][0].DeduplicationKey != firstMetadata.ID.String() ||
		putter.calls[0][1].DeduplicationKey != secondMetadata.ID.String() ||
		receipts[0].MessageID != firstMetadata.ID || receipts[1].MessageID != secondMetadata.ID {
		t.Fatalf("batch order changed: %#v %#v", putter.calls[0], receipts)
	}
}

var _ coreoutbox.UniqueBatchVersionedPutter = (*fakeUniqueBatchPutter)(nil)
