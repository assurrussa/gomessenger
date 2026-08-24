package messenger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type fixedHeaderPropagator struct{}

const testTenantValue = "test"

func (fixedHeaderPropagator) Inject(_ context.Context, carrier map[string]string) {
	carrier["traceparent"] = testTraceParent
	carrier["tracestate"] = "vendor=value"
}

func (fixedHeaderPropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

func TestOutgoingPropagationIsImmutableAndIncludedInEnvelope(t *testing.T) {
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	originalHeaders := map[string]string{"tenant": testTenantValue}
	var captured messenger.Envelope
	route := &routeFunc{
		name: "capture",
		deliver: func(_ context.Context, delivery messenger.Delivery) (messenger.Receipt, error) {
			data, err := delivery.MarshalEnvelope()
			if err != nil {
				return messenger.Receipt{}, err
			}
			captured, err = messenger.UnmarshalEnvelope(data)
			return messenger.Receipt{State: messenger.ReceiptBrokerConfirmed}, err
		},
	}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithContextPropagator(fixedHeaderPropagator{}),
	)
	builder.RouteEvent(event, route)
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.PublishMessage(t.Context(), event, messenger.Outgoing[processedPayload]{
		Payload:  processedPayload{JobID: 42},
		Metadata: messenger.OutgoingMetadata{Headers: originalHeaders},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if captured.Headers["traceparent"] == "" || captured.Headers["tracestate"] != "vendor=value" ||
		captured.Headers["tenant"] != testTenantValue {
		t.Fatalf("captured headers = %#v", captured.Headers)
	}
	if len(originalHeaders) != 1 || originalHeaders["tenant"] != testTenantValue {
		t.Fatalf("caller headers mutated = %#v", originalHeaders)
	}
}

func TestCanonicalEnvelopeNormalizesEquivalentTimeZones(t *testing.T) {
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	base := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: event.Info().Name, SchemaVersion: 1,
		Source: testSource, Time: time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC),
		CorrelationID: id, ContentType: event.Info().ContentType,
	}
	first := base
	first.NotBefore = time.Date(2026, 8, 24, 15, 0, 0, 0, time.FixedZone("UTC+5", 5*60*60))
	first.ExpiresAt = first.NotBefore.Add(time.Hour)
	second := base
	second.NotBefore = time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	second.ExpiresAt = second.NotBefore.Add(time.Hour)
	firstData, err := messenger.EncodeEventEnvelope(event, first, processedPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode first: %v", err)
	}
	secondData, err := messenger.EncodeEventEnvelope(event, second, processedPayload{JobID: 42})
	if err != nil {
		t.Fatalf("encode second: %v", err)
	}
	if string(firstData) != string(secondData) ||
		messenger.EnvelopeFingerprint(firstData) != messenger.EnvelopeFingerprint(secondData) {
		t.Fatalf("equivalent timestamps produced different canonical envelopes\n%s\n%s", firstData, secondData)
	}
}

func TestBuilderRejectsNilMiddlewareAndPropagator(t *testing.T) {
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithContextPropagator(nil),
	)
	builder.UseMiddleware(nil)
	_, _, err := builder.Build()
	if !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("build error = %v", err)
	}
}
