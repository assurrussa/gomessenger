package nats_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

func TestCloudEventsStructuredRoundTrip(t *testing.T) {
	native, metadata := testNativeEnvelope(t)
	structured, err := nats.EncodeCloudEventStructured(native)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := nats.DecodeCloudEventStructured(structured)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertCloudEnvelope(t, decoded, metadata, messenger.DataJSON, []byte(`{"jobId":42}`))
	var encoded map[string]any
	if err := json.Unmarshal(structured, &encoded); err != nil {
		t.Fatalf("decode structured JSON: %v", err)
	}
	if encoded["traceparent"] != metadata.Headers["traceparent"] {
		t.Fatalf("traceparent extension = %#v", encoded["traceparent"])
	}
	legacyHeaders, err := json.Marshal(map[string]string{
		"tenant":      testNamespace,
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if err != nil {
		t.Fatalf("marshal duplicate headers: %v", err)
	}
	encoded["gmheaders"] = base64.RawURLEncoding.EncodeToString(legacyHeaders)
	structured, err = json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal structured JSON: %v", err)
	}
	decoded, err = nats.DecodeCloudEventStructured(structured)
	if err != nil {
		t.Fatalf("decode duplicate trace context: %v", err)
	}
	if got := nats.CloudEventEnvelopeMetadata(decoded).Headers["traceparent"]; got != metadata.Headers["traceparent"] {
		t.Fatalf("CloudEvents extension did not win: %q", got)
	}
}

func TestCloudEventsMissingTimeUsesStableUUIDv7Timestamp(t *testing.T) {
	native, _ := testNativeEnvelope(t)
	structured, err := nats.EncodeCloudEventStructured(native)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var encoded map[string]any
	if err := json.Unmarshal(structured, &encoded); err != nil {
		t.Fatalf("decode structured JSON: %v", err)
	}
	delete(encoded, "time")
	structured, err = json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal without time: %v", err)
	}
	first, err := nats.DecodeCloudEventStructured(structured)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}
	second, err := nats.DecodeCloudEventStructured(structured)
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	firstTime := nats.CloudEventEnvelopeMetadata(first).Time
	secondTime := nats.CloudEventEnvelopeMetadata(second).Time
	want := time.UnixMilli(0x018f4f2c4a00).UTC()
	if !firstTime.Equal(want) || !secondTime.Equal(want) {
		t.Fatalf("derived times = %s / %s, want %s", firstTime, secondTime, want)
	}

	encoded["id"] = "018f4f2c-4a00-4000-8000-000000000001"
	structured, err = json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal non-v7 event: %v", err)
	}
	if _, err := nats.DecodeCloudEventStructured(structured); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("non-v7 missing-time error = %v", err)
	}
}

func TestCloudEventsStructuredRejectsFractionalSchemaVersion(t *testing.T) {
	native, _ := testNativeEnvelope(t)
	structured, err := nats.EncodeCloudEventStructured(native)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var encoded map[string]any
	if err := json.Unmarshal(structured, &encoded); err != nil {
		t.Fatalf("decode structured JSON: %v", err)
	}
	encoded["schemaversion"] = 1.5
	structured, err = json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal fractional version: %v", err)
	}
	if _, err := nats.DecodeCloudEventStructured(structured); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("fractional schema version error = %v", err)
	}
}

func TestCloudEventsRequireExplicitDataEncoding(t *testing.T) {
	native, _ := testNativeEnvelope(t)
	structured, err := nats.EncodeCloudEventStructured(native)
	if err != nil {
		t.Fatalf("encode structured: %v", err)
	}
	var encoded map[string]any
	if err := json.Unmarshal(structured, &encoded); err != nil {
		t.Fatalf("decode structured JSON: %v", err)
	}
	delete(encoded, "dataencoding")
	structured, err = json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal without encoding: %v", err)
	}
	if _, err := nats.DecodeCloudEventStructured(structured); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("missing structured encoding error = %v", err)
	}

	headers := make(natsio.Header)
	payload, err := nats.EncodeCloudEventBinary(native, headers)
	if err != nil {
		t.Fatalf("encode binary: %v", err)
	}
	headers.Del("Ce-dataencoding")
	if _, err := nats.DecodeCloudEventBinary(payload, headers); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("missing binary encoding error = %v", err)
	}
}

func TestRouteHonorsNotBeforeAndExpiresAtBeforeBrokerIO(t *testing.T) {
	_, metadata := testNativeEnvelope(t)
	route := nats.NewTestRoute(testNamespace, testNamespace, nats.WireNative, func() time.Time {
		return metadata.Time
	})
	metadata.NotBefore = metadata.Time.Add(time.Minute)
	native, err := messenger.MarshalEnvelope(metadata, []byte(`{"jobId":42}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal delayed: %v", err)
	}
	_, err = route.PublishEnvelope(t.Context(), native)
	delay, ok := messenger.RetryDelay(err)
	if !ok || !errors.Is(err, nats.ErrMessageNotReady) || delay != time.Minute {
		t.Fatalf("not-before error = %v, delay=%s", err, delay)
	}

	metadata.NotBefore = time.Time{}
	metadata.ExpiresAt = metadata.Time
	native, err = messenger.MarshalEnvelope(metadata, []byte(`{"jobId":42}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal expired: %v", err)
	}
	_, err = route.PublishEnvelope(t.Context(), native)
	if !messenger.IsPermanent(err) || !errors.Is(err, nats.ErrMessageExpired) {
		t.Fatalf("expired error = %v", err)
	}
}

func TestCloudEventsBinaryRoundTrip(t *testing.T) {
	native, metadata := testNativeEnvelope(t)
	headers := make(natsio.Header)
	payload, err := nats.EncodeCloudEventBinary(native, headers)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := nats.DecodeCloudEventBinary(payload, headers)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertCloudEnvelope(t, decoded, metadata, messenger.DataJSON, []byte(`{"jobId":42}`))
}

func TestCloudEventsPreserveTextPayloadWithCustomMediaType(t *testing.T) {
	_, metadata := testNativeEnvelope(t)
	metadata.ContentType = "application/vnd.example.text"
	payload := []byte("job 42 done")
	native, err := messenger.MarshalEnvelope(metadata, payload, messenger.DataText)
	if err != nil {
		t.Fatalf("marshal native: %v", err)
	}

	structured, err := nats.EncodeCloudEventStructured(native)
	if err != nil {
		t.Fatalf("encode structured: %v", err)
	}
	structuredEnvelope, err := nats.DecodeCloudEventStructured(structured)
	if err != nil {
		t.Fatalf("decode structured: %v", err)
	}
	assertCloudEnvelope(t, structuredEnvelope, metadata, messenger.DataText, payload)

	headers := make(natsio.Header)
	binaryPayload, err := nats.EncodeCloudEventBinary(native, headers)
	if err != nil {
		t.Fatalf("encode binary: %v", err)
	}
	binaryEnvelope, err := nats.DecodeCloudEventBinary(binaryPayload, headers)
	if err != nil {
		t.Fatalf("decode binary: %v", err)
	}
	assertCloudEnvelope(t, binaryEnvelope, metadata, messenger.DataText, payload)
}

func testNativeEnvelope(t *testing.T) ([]byte, messenger.Metadata) {
	t.Helper()
	id := mustID(t, "018f4f2c-4a00-7000-8000-000000000001")
	causationID := mustID(t, "018f4f2c-49ff-7000-8000-000000000002")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
		Source: "urn:service:media", Subject: "job/42", Time: time.Unix(1_700_000_000, 123).UTC(),
		CorrelationID: id, CausationID: causationID, Key: "42", ContentType: testContentType,
		Schema: "urn:schema:media.processed:1", Headers: map[string]string{
			"tenant":      testNamespace,
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"tracestate":  testTraceState,
		},
		NotBefore: time.Unix(1_700_000_001, 0).UTC(), ExpiresAt: time.Unix(1_700_000_100, 0).UTC(),
	}
	data, err := messenger.MarshalEnvelope(metadata, []byte(`{"jobId":42}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal native: %v", err)
	}
	return data, metadata
}

func assertCloudEnvelope(
	t *testing.T,
	got nats.CloudEventEnvelope,
	want messenger.Metadata,
	wantEncoding messenger.DataEncoding,
	payload []byte,
) {
	t.Helper()
	gotMeta := nats.CloudEventEnvelopeMetadata(got)
	gotData := nats.CloudEventEnvelopeData(got)
	if gotMeta.ID != want.ID || gotMeta.CorrelationID != want.CorrelationID ||
		gotMeta.CausationID != want.CausationID || gotMeta.Name != want.Name ||
		gotMeta.SchemaVersion != want.SchemaVersion || gotMeta.Headers["tenant"] != testNamespace ||
		gotMeta.Headers["traceparent"] != want.Headers["traceparent"] ||
		gotMeta.Headers["tracestate"] != want.Headers["tracestate"] ||
		!gotMeta.NotBefore.Equal(want.NotBefore) || !gotMeta.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("metadata = %#v, want %#v", gotMeta, want)
	}
	if !bytes.Equal(gotData, payload) {
		t.Fatalf("payload = %s, want %s", gotData, payload)
	}
	if gotEncoding := nats.CloudEventEnvelopeDataEncoding(got); gotEncoding != wantEncoding {
		t.Fatalf("data encoding = %s, want %s", gotEncoding, wantEncoding)
	}
}
