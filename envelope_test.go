package messenger_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestEnvelope_RoundTripPayloadEncodings(t *testing.T) {
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	metadata := messenger.Metadata{
		ID:            id,
		Kind:          messenger.KindEvent,
		Name:          testEventName,
		SchemaVersion: 1,
		Source:        testSource,
		Time:          time.Unix(1_700_000_000, 123).UTC(),
		CorrelationID: id,
		ContentType:   testContentType,
	}
	tests := []struct {
		name        string
		payload     []byte
		encoding    messenger.DataEncoding
		contentType string
	}{
		{name: "json", payload: []byte(`{"jobId":42}`), encoding: messenger.DataJSON, contentType: testContentType},
		{name: "text", payload: []byte("done"), encoding: messenger.DataText, contentType: "application/vnd.example.text"},
		{name: "binary", payload: []byte{0, 1, 2, 255}, encoding: messenger.DataBinary, contentType: "application/octet-stream"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := metadata
			current.ContentType = test.contentType
			data, err := messenger.MarshalEnvelope(current, test.payload, test.encoding)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			envelope, err := messenger.UnmarshalEnvelope(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			payload, encoding, err := envelope.Payload()
			if err != nil {
				t.Fatalf("payload: %v", err)
			}
			if !bytes.Equal(payload, test.payload) || encoding != test.encoding {
				t.Fatalf("payload = %v/%v, want %v/%v", payload, encoding, test.payload, test.encoding)
			}
			if !json.Valid(data) {
				t.Fatalf("invalid envelope JSON: %s", data)
			}
		})
	}
}

func TestEnvelopeRejectsUnknownFieldsAndTrailingValues(t *testing.T) {
	data, err := messenger.MarshalEnvelope(validMetadata(t), []byte(`null`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	unknown := append([]byte(nil), data[:len(data)-1]...)
	unknown = append(unknown, []byte(`,"unknown":true}`)...)
	if _, err := messenger.UnmarshalEnvelope(unknown); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := messenger.UnmarshalEnvelope(append(data, []byte(` {}`)...)); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("trailing value error = %v", err)
	}
}

func TestDecodeCommand_VerifiesDescriptor(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindCommand, Name: "media.processor", SchemaVersion: 1,
		Source: testSource, Time: time.Now().UTC(), CorrelationID: id,
		ContentType: testContentType,
	}
	data, err := messenger.MarshalEnvelope(metadata, []byte(`{"jobId":11}`), messenger.DataJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	message, err := messenger.DecodeCommand(command, data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Payload.JobID != 11 || message.Metadata.ID != id {
		t.Fatalf("message = %#v", message)
	}
}

func FuzzUnmarshalEnvelope(f *testing.F) {
	f.Add([]byte(`{"specVersion":"1.0"}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = messenger.UnmarshalEnvelope(data)
	})
}
