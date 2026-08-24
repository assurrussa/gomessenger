package messenger_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestEnvelopeValidationAndPayloadFailures(t *testing.T) {
	metadata := validMetadata(t)
	data, err := messenger.MarshalEnvelope(metadata, nil, messenger.DataJSON)
	if err != nil || !bytes.Contains(data, []byte(`"data":null`)) {
		t.Fatalf("nil JSON = %s, %v", data, err)
	}
	canonical, err := messenger.CanonicalizeEnvelope(data)
	if err != nil || !bytes.Equal(canonical, data) {
		t.Fatalf("canonical = %s, %v", canonical, err)
	}
	if messenger.EnvelopeFingerprint(data) != messenger.EnvelopeFingerprint(canonical) {
		t.Fatal("canonical fingerprint changed")
	}

	invalidMetadata := metadata
	invalidMetadata.ExpiresAt = time.Unix(10, 0)
	invalidMetadata.NotBefore = time.Unix(20, 0)
	if _, err := messenger.MarshalEnvelope(
		invalidMetadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid dates = %v", err)
	}
	if _, err := messenger.MarshalEnvelope(metadata, []byte(`{`), messenger.DataJSON); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid JSON codec bytes = %v", err)
	}
	if _, err := messenger.MarshalEnvelope(metadata, nil, 0); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("unknown encoding = %v", err)
	}

	envelope, err := messenger.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("binary"))
	envelope.DataBase64 = &encoded
	if err := envelope.Validate(); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("dual data = %v", err)
	}
	envelope.Data = nil
	badBase64 := "!"
	envelope.DataBase64 = &badBase64
	envelope.DataEncoding = messenger.DataBinary
	if _, _, err := envelope.Payload(); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("bad base64 = %v", err)
	}
	envelope.DataBase64 = nil
	envelope.DataEncoding = messenger.DataText
	envelope.Data = json.RawMessage(`42`)
	if _, _, err := envelope.Payload(); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("bad text = %v", err)
	}
	envelope.DataEncoding = messenger.DataJSON
	envelope.Data = json.RawMessage(`{`)
	if _, _, err := envelope.Payload(); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("bad JSON payload = %v", err)
	}
}

func TestEnvelopeBoundsAndStrictIdentity(t *testing.T) {
	metadata := validMetadata(t)
	metadata.Headers = map[string]string{"bad\nkey": "value"}
	if _, err := messenger.MarshalEnvelope(
		metadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid header key = %v", err)
	}
	metadata.Headers = map[string]string{"key": "bad\rvalue"}
	if _, err := messenger.MarshalEnvelope(
		metadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid header value = %v", err)
	}
	metadata.Headers = map[string]string{
		"traceparent": testTraceParent,
		"TraceParent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	}
	if _, err := messenger.MarshalEnvelope(
		metadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("duplicate trace header = %v", err)
	}
	metadata.Headers = make(map[string]string)
	for index := range messenger.DefaultMaxHeaders + 1 {
		metadata.Headers[string(rune('a'+index%26))+strings.Repeat("x", index/26)] = "v"
	}
	if _, err := messenger.MarshalEnvelope(
		metadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("header count = %v", err)
	}
	metadata.Headers = map[string]string{"key": strings.Repeat("v", messenger.DefaultMaxHeaderBytes)}
	if _, err := messenger.MarshalEnvelope(
		metadata, []byte(`null`), messenger.DataJSON,
	); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("header bytes = %v", err)
	}

	oversized := bytes.Repeat([]byte("x"), messenger.DefaultMaxEnvelopeBytes+1)
	if _, err := messenger.UnmarshalEnvelope(oversized); !errors.Is(err, messenger.ErrEnvelopeTooLarge) {
		t.Fatalf("unmarshal oversized = %v", err)
	}
	metadata = validMetadata(t)
	maxPayload := bytes.Repeat([]byte("x"), messenger.DefaultMaxEnvelopeBytes)
	if _, err := messenger.MarshalEnvelope(
		metadata, maxPayload, messenger.DataBinary,
	); !errors.Is(err, messenger.ErrEnvelopeTooLarge) {
		t.Fatalf("marshal oversized = %v", err)
	}
	if _, err := messenger.UnmarshalEnvelope([]byte(`not-json`)); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid envelope JSON = %v", err)
	}
}

func TestTypedEnvelopeEncodeDecodeAndMismatch(t *testing.T) {
	metadata := validMetadata(t)
	command := messenger.MustCommand(testEventName, 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent(testEventName, 1, messenger.JSON[processPayload]())
	metadata.Kind = messenger.KindCommand
	data, err := messenger.EncodeCommandEnvelope(command, metadata, processPayload{JobID: 7})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	message, err := messenger.DecodeCommand(command, data)
	if err != nil || message.Payload.JobID != 7 {
		t.Fatalf("decode command = %#v, %v", message, err)
	}
	payload, err := messenger.DecodeCommandPayload(command, []byte(`{"jobId":8}`))
	if err != nil || payload.JobID != 8 {
		t.Fatalf("decode command payload = %#v, %v", payload, err)
	}
	if _, err := messenger.DecodeEvent(event, data); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("kind mismatch = %v", err)
	}

	metadata.Kind = messenger.KindEvent
	eventData, err := messenger.EncodeEventEnvelope(event, metadata, processPayload{JobID: 9})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	eventMessage, err := messenger.DecodeEvent(event, eventData)
	if err != nil || eventMessage.Payload.JobID != 9 {
		t.Fatalf("decode event = %#v, %v", eventMessage, err)
	}
	eventPayload, err := messenger.DecodeEventPayload(event, []byte(`{"jobId":10}`))
	if err != nil || eventPayload.JobID != 10 {
		t.Fatalf("decode event payload = %#v, %v", eventPayload, err)
	}

	wrongMetadata := metadata
	wrongMetadata.SchemaVersion = 2
	if _, err := messenger.EncodeEventEnvelope(
		event, wrongMetadata, processPayload{},
	); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("encode mismatch = %v", err)
	}
	binaryEvent := messenger.MustEvent(testEventName, 1, messenger.Bytes())
	if _, err := messenger.DecodeEvent(binaryEvent, eventData); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("encoding mismatch = %v", err)
	}
}

func validMetadata(t *testing.T) messenger.Metadata {
	t.Helper()
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	return messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
		Source: testSource, Time: time.Unix(1_700_000_000, 0).UTC(),
		CorrelationID: id, ContentType: testContentType,
	}
}
