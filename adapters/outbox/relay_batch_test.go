package outboxadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/shared/types"

	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
)

type fakeBatchEnvelopePublisher struct {
	payloads [][]byte
	errs     []error
	topErr   error
}

func (p *fakeBatchEnvelopePublisher) PublishEnvelopeBatch(
	_ context.Context,
	payloads [][]byte,
) ([]messenger.Receipt, []error, error) {
	p.payloads = append([][]byte(nil), payloads...)
	return make([]messenger.Receipt, len(payloads)), append([]error(nil), p.errs...), p.topErr
}

func TestBatchRelayClassifiesInvalidAndDeferredPublicationsPerItem(t *testing.T) {
	_, envelope := testEnvelope(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	publisher := &fakeBatchEnvelopePublisher{errs: []error{messenger.RetryAfter(errors.New("down"), time.Second)}}
	job, err := outboxadapter.NewBatchRelayJob(publisher, outboxadapter.RelayJobConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	firstID, secondID := types.NewJobID(), types.NewJobID()
	result, err := job.HandleBatch(t.Context(), []coreoutbox.BatchJobItem{
		{JobID: firstID, Payload: string(envelope), Attempt: 1},
		{JobID: secondID, Payload: `{"invalid":true}`, Attempt: 1},
	})
	if err != nil || len(result.Items) != 2 || len(publisher.payloads) != 1 {
		t.Fatalf("result/payloads/error = %#v/%d/%v", result, len(publisher.payloads), err)
	}
	if deferAt, ok := coreoutbox.DeferTime(result.Items[0].Err); !ok || !deferAt.Equal(now.Add(time.Second)) {
		t.Fatalf("first disposition = %v", result.Items[0].Err)
	}
	if !coreoutbox.IsPermanent(result.Items[1].Err) {
		t.Fatalf("second disposition = %v", result.Items[1].Err)
	}
}

var _ outboxadapter.BatchEnvelopePublisher = (*fakeBatchEnvelopePublisher)(nil)
