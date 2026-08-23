package messenger_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

func TestBuiltInAndCustomCodecs(t *testing.T) {
	jsonCodec := messenger.JSON[processPayload]()
	encoded, err := jsonCodec.Encode(processPayload{JobID: 42})
	if err != nil || string(encoded) != `{"jobId":42}` {
		t.Fatalf("JSON encode = %s, %v", encoded, err)
	}
	decoded, err := jsonCodec.Decode(encoded)
	if err != nil || decoded.JobID != 42 {
		t.Fatalf("JSON decode = %#v, %v", decoded, err)
	}
	if _, err := jsonCodec.Decode([]byte(`{`)); err == nil {
		t.Fatal("invalid JSON decoded successfully")
	}
	if _, err := messenger.JSON[func()]().Encode(func() {}); err == nil {
		t.Fatal("unsupported JSON type encoded successfully")
	}

	binary := messenger.Bytes()
	original := []byte{1, 2, 3}
	encoded, err = binary.Encode(original)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	encoded[0] = 9
	if original[0] != 1 || binary.ContentType() != "application/octet-stream" ||
		binary.Encoding() != messenger.DataBinary {
		t.Fatal("binary codec did not copy or report its contract")
	}
	decodedBytes, err := binary.Decode(encoded)
	if err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	decodedBytes[1] = 8
	if encoded[1] != 2 {
		t.Fatal("binary decode did not copy input")
	}

	text := messenger.Text()
	encoded, err = text.Encode("hello")
	if err != nil || !bytes.Equal(encoded, []byte("hello")) {
		t.Fatalf("text encode = %q, %v", encoded, err)
	}
	decodedText, err := text.Decode(encoded)
	if err != nil || decodedText != "hello" || text.Encoding() != messenger.DataText ||
		text.ContentType() == "" {
		t.Fatalf("text decode = %q, %v", decodedText, err)
	}

	cause := errors.New("codec")
	custom, err := messenger.CustomCodec(
		"application/custom", messenger.DataBinary,
		func(int) ([]byte, error) { return nil, cause },
		func([]byte) (int, error) { return 0, cause },
	)
	if err != nil {
		t.Fatalf("custom: %v", err)
	}
	if _, err := custom.Encode(1); !errors.Is(err, cause) {
		t.Fatalf("custom encode = %v", err)
	}
	if _, err := custom.Decode(nil); !errors.Is(err, cause) {
		t.Fatalf("custom decode = %v", err)
	}
	if custom.ContentType() != "application/custom" || custom.Encoding() != messenger.DataBinary {
		t.Fatal("custom codec contract changed")
	}
	invalidCases := []func() error{
		func() error {
			_, err := messenger.CustomCodec(
				"", messenger.DataJSON,
				func(int) ([]byte, error) { return nil, nil },
				func([]byte) (int, error) { return 0, nil },
			)
			return err
		},
		func() error {
			_, err := messenger.CustomCodec(
				"x", 0,
				func(int) ([]byte, error) { return nil, nil },
				func([]byte) (int, error) { return 0, nil },
			)
			return err
		},
		func() error {
			_, err := messenger.CustomCodec[int]("x", messenger.DataJSON, nil, func([]byte) (int, error) { return 0, nil })
			return err
		},
		func() error {
			_, err := messenger.CustomCodec[int]("x", messenger.DataJSON, func(int) ([]byte, error) { return nil, nil }, nil)
			return err
		},
	}
	for index, invalid := range invalidCases {
		if err := invalid(); !errors.Is(err, messenger.ErrInvalidDescriptor) {
			t.Fatalf("invalid custom codec %d = %v", index, err)
		}
	}
}

func TestDescriptorValidationSchemaAndInfo(t *testing.T) {
	command := messenger.MustCommand("media.processor", 2, messenger.JSON[processPayload]()).WithSchema("urn:schema:media:2")
	event := messenger.MustEvent("media.processed", 3, messenger.Text()).WithSchema("urn:schema:event:3")
	if command.Info().Schema != "urn:schema:media:2" || command.Info().Kind != messenger.KindCommand ||
		command.Info().DataEncoding != messenger.DataJSON || event.Info().Schema != "urn:schema:event:3" ||
		event.Info().Kind != messenger.KindEvent || event.Info().DataEncoding != messenger.DataText {
		t.Fatalf("descriptor info = %#v / %#v", command.Info(), event.Info())
	}
	invalidNames := []string{"", "Upper", "space value", "-prefix"}
	for _, name := range invalidNames {
		if _, err := messenger.NewCommand(name, 1, messenger.JSON[int]()); !errors.Is(err, messenger.ErrInvalidDescriptor) {
			t.Fatalf("invalid name %q = %v", name, err)
		}
	}
	if _, err := messenger.NewEvent("valid", 0, messenger.JSON[int]()); !errors.Is(err, messenger.ErrInvalidDescriptor) {
		t.Fatalf("invalid version = %v", err)
	}
	var nilCodec *pointerCodec
	if _, err := messenger.NewEvent("valid", 1, nilCodec); !errors.Is(err, messenger.ErrInvalidDescriptor) {
		t.Fatalf("typed nil codec = %v", err)
	}
	assertPanics(t, func() { messenger.MustCommand("BAD", 1, messenger.JSON[int]()) })
	assertPanics(t, func() { messenger.MustEvent("BAD", 1, messenger.JSON[int]()) })
}

func TestBuilderRejectsDescriptorsThatDifferOnlyByDataEncoding(t *testing.T) {
	jsonCodec, err := messenger.CustomCodec(
		"application/custom", messenger.DataJSON,
		func(int) ([]byte, error) { return []byte(`1`), nil },
		func([]byte) (int, error) { return 1, nil },
	)
	if err != nil {
		t.Fatalf("JSON codec: %v", err)
	}
	textCodec, err := messenger.CustomCodec(
		"application/custom", messenger.DataText,
		func(int) ([]byte, error) { return []byte("1"), nil },
		func([]byte) (int, error) { return 1, nil },
	)
	if err != nil {
		t.Fatalf("text codec: %v", err)
	}
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.Subscribe(messenger.MustEvent(testEventName, 1, jsonCodec), "json", func(context.Context, messenger.Message[int]) error {
		return nil
	})
	builder.Subscribe(messenger.MustEvent(testEventName, 1, textCodec), "text", func(context.Context, messenger.Message[int]) error {
		return nil
	})
	if _, _, err := builder.Build(); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("build error = %v", err)
	}
}

type pointerCodec struct{}

func (*pointerCodec) Encode(int) ([]byte, error)       { return nil, nil }
func (*pointerCodec) Decode([]byte) (int, error)       { return 0, nil }
func (*pointerCodec) ContentType() string              { return "application/test" }
func (*pointerCodec) Encoding() messenger.DataEncoding { return messenger.DataJSON }

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
