package messenger_test

import (
	"encoding/json"
	"errors"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

func TestUUIDv7Generator(t *testing.T) {
	id, err := messenger.UUIDv7Generator().New()
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if id[6]>>4 != 7 || id[8]>>6 != 2 {
		t.Fatalf("id %s has version=%d variant=%d", id, id[6]>>4, id[8]>>6)
	}
	parsed, err := messenger.ParseMessageID(id.String())
	if err != nil || parsed != id {
		t.Fatalf("parse = %s, %v", parsed, err)
	}
	data, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded messenger.MessageID
	if err := json.Unmarshal(data, &decoded); err != nil || decoded != id {
		t.Fatalf("unmarshal = %s, %v", decoded, err)
	}
}

func TestMessageIDTextJSONAndValidationErrors(t *testing.T) {
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000001")
	text, err := id.MarshalText()
	if err != nil || string(text) != id.String() {
		t.Fatalf("marshal text = %q, %v", text, err)
	}
	var decoded messenger.MessageID
	if err := decoded.UnmarshalText(text); err != nil || decoded != id {
		t.Fatalf("unmarshal text = %s, %v", decoded, err)
	}
	invalid := []string{"", "018f4f2c4a0070008000000000000001", "zzzzzzzz-4a00-7000-8000-000000000001"}
	for _, value := range invalid {
		if _, err := messenger.ParseMessageID(value); !errors.Is(err, messenger.ErrInvalidMessage) {
			t.Fatalf("parse %q = %v", value, err)
		}
	}
	if err := json.Unmarshal([]byte(`42`), &decoded); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("numeric JSON ID = %v", err)
	}
}
