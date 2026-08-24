package nats_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

type queryDelivery struct {
	metadata     messenger.Metadata
	marshalCalls atomic.Int32
}

func (d *queryDelivery) Metadata() messenger.Metadata { return d.metadata }
func (*queryDelivery) HandlerCount() int              { return 1 }
func (d *queryDelivery) MarshalEnvelope() ([]byte, error) {
	d.marshalCalls.Add(1)
	return nil, errors.New("query must not be serialized")
}

func (*queryDelivery) Fingerprint() ([sha256.Size]byte, error) {
	return [sha256.Size]byte{}, errors.New("query must not be fingerprinted")
}

func (*queryDelivery) Invoke(context.Context) error {
	return errors.New("query must not be invoked by NATS")
}

func TestRouteRejectsQueryBeforeWireEncoding(t *testing.T) {
	connection := startJetStream(t)
	route, err := nats.NewRoute(connection, nats.RouteConfig{
		Name: "nats.query-rejection", Namespace: testNamespace, WireMode: nats.WireNative,
	})
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000031")
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	delivery := &queryDelivery{metadata: messenger.Metadata{
		ID: id, Kind: messenger.KindQuery, Name: "article.find", SchemaVersion: 1,
		Source: testSource, Time: time.Now().UTC(), CorrelationID: id, ContentType: testContentType,
	}}
	if _, err := route.Deliver(t.Context(), delivery); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("query delivery = %v", err)
	}
	if delivery.marshalCalls.Load() != 0 {
		t.Fatalf("query marshal calls = %d", delivery.marshalCalls.Load())
	}
	if _, err := nats.Subject(testNamespace, messenger.DescriptorInfo{
		Kind: messenger.KindQuery, Name: "article.find", SchemaVersion: 1,
	}); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("query subject = %v", err)
	}
}
