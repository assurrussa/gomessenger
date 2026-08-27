//nolint:testpackage // Tests exercise the package-local transactional measurement decorators.
package demo

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	testMessageID = "018f22e2-7e9c-7a5b-8c2d-001122334455"
	testRunID     = "run-1"
	testStageID   = "stage-1"
)

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

type routeStub struct {
	err    error
	called bool
}

func (r *routeStub) Name() string { return "stub" }
func (r *routeStub) Deliver(context.Context, messenger.Delivery) (messenger.Receipt, error) {
	r.called = true
	return messenger.Receipt{State: messenger.ReceiptStaged}, r.err
}

func TestMeasurementRouteRecordsExactEnvelopeBeforeDelegate(t *testing.T) {
	t.Parallel()
	messageID := mustMessageID(t)
	delegate := &routeStub{}
	var got envelopeMeasurement
	route, err := newMeasurementRouteWithRecorder(delegate, func(_ context.Context, measurement envelopeMeasurement) error {
		got = measurement
		return nil
	})
	if err != nil {
		t.Fatalf("newMeasurementRouteWithRecorder() error = %v", err)
	}
	envelope := []byte("canonical-envelope")
	receipt, err := route.Deliver(context.Background(), staticDelivery{
		metadata: messenger.Metadata{
			ID:      messageID,
			Headers: map[string]string{BenchmarkRunHeader: testRunID, BenchmarkStageHeader: testStageID},
		},
		envelope: envelope,
	})
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if !delegate.called || receipt.State != messenger.ReceiptStaged {
		t.Fatalf("delegate result was not preserved: called=%v receipt=%#v", delegate.called, receipt)
	}
	wantDigest := sha256.Sum256(envelope)
	if got.MessageID != messageID.String() || got.EnvelopeBytes != int64(len(envelope)) ||
		got.SHA256 != fmtDigest(wantDigest) {
		t.Fatalf("measurement = %#v", got)
	}
}

func TestMeasurementRouteStopsBeforeDelegateWhenRecordingFails(t *testing.T) {
	t.Parallel()
	messageID := mustMessageID(t)
	delegate := &routeStub{}
	recordErr := errors.New("record failed")
	route, err := newMeasurementRouteWithRecorder(delegate, func(context.Context, envelopeMeasurement) error {
		return recordErr
	})
	if err != nil {
		t.Fatalf("newMeasurementRouteWithRecorder() error = %v", err)
	}
	_, err = route.Deliver(context.Background(), staticDelivery{
		metadata: messenger.Metadata{
			ID:      messageID,
			Headers: map[string]string{BenchmarkRunHeader: testRunID, BenchmarkStageHeader: testStageID},
		},
		envelope: []byte("payload"),
	})
	if !errors.Is(err, recordErr) || delegate.called {
		t.Fatalf("Deliver() error=%v delegate.called=%v", err, delegate.called)
	}
}

func TestMeasurementRoutePropagatesDelegateFailureToRollbackTransaction(t *testing.T) {
	t.Parallel()
	messageID := mustMessageID(t)
	delegateErr := errors.New("stage Outbox job failed")
	delegate := &routeStub{err: delegateErr}
	recorded := false
	route, err := newMeasurementRouteWithRecorder(delegate, func(context.Context, envelopeMeasurement) error {
		recorded = true
		return nil
	})
	if err != nil {
		t.Fatalf("newMeasurementRouteWithRecorder() error = %v", err)
	}
	_, err = route.Deliver(context.Background(), staticDelivery{
		metadata: messenger.Metadata{
			ID:      messageID,
			Headers: map[string]string{BenchmarkRunHeader: testRunID, BenchmarkStageHeader: testStageID},
		},
		envelope: []byte("payload"),
	})
	if !recorded || !delegate.called || !errors.Is(err, delegateErr) {
		t.Fatalf("recorded=%v delegate.called=%v error=%v", recorded, delegate.called, err)
	}
}

type publisherStub struct {
	err error
}

func (p publisherStub) PublishEnvelope(context.Context, []byte) (messenger.Receipt, error) {
	return messenger.Receipt{State: messenger.ReceiptBrokerConfirmed}, p.err
}

func TestMeasurementPublisherMarksExactConfirmedEnvelope(t *testing.T) {
	t.Parallel()
	messageID := mustMessageID(t)
	metadata := messenger.Metadata{
		ID: messageID, Kind: messenger.KindEvent, Name: "orders.created", SchemaVersion: 1,
		Source: "urn:test", Time: time.Unix(1_700_000_000, 0).UTC(), CorrelationID: messageID,
		ContentType: "application/json",
		Headers:     map[string]string{BenchmarkRunHeader: testRunID, BenchmarkStageHeader: testStageID},
	}
	envelope, err := messenger.MarshalEnvelope(metadata, []byte(`{"orderId":"one"}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("MarshalEnvelope() error = %v", err)
	}
	var got envelopeMeasurement
	publisher, err := newMeasurementPublisher(publisherStub{}, func(_ context.Context, measurement envelopeMeasurement) error {
		got = measurement
		return nil
	})
	if err != nil {
		t.Fatalf("newMeasurementPublisher() error = %v", err)
	}
	if _, err := publisher.PublishEnvelope(context.Background(), envelope); err != nil {
		t.Fatalf("PublishEnvelope() error = %v", err)
	}
	digest := sha256.Sum256(envelope)
	if got.MessageID != messageID.String() || got.EnvelopeBytes != int64(len(envelope)) ||
		got.SHA256 != fmtDigest(digest) {
		t.Fatalf("measurement = %#v", got)
	}
}

func mustMessageID(t *testing.T) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID(testMessageID)
	if err != nil {
		t.Fatalf("ParseMessageID() error = %v", err)
	}
	return id
}

func fmtDigest(value [sha256.Size]byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, 0, sha256.Size*2)
	for _, item := range value {
		result = append(result, digits[item>>4], digits[item&0x0f])
	}
	return string(result)
}
