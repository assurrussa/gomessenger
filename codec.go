package messenger

import (
	"encoding/json"
	"fmt"
)

// DataEncoding selects how encoded payload bytes appear in envelope JSON.
type DataEncoding uint8

const (
	// DataJSON stores codec output directly in the envelope data field.
	DataJSON DataEncoding = iota + 1
	// DataText stores codec output as a JSON string in the envelope data field.
	DataText
	// DataBinary stores codec output in the envelope dataBase64 field.
	DataBinary
)

// Codec encodes and decodes one descriptor payload type.
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
	ContentType() string
	Encoding() DataEncoding
}

type jsonCodec[T any] struct{}

// JSON returns the standard JSON codec for T.
func JSON[T any]() Codec[T] {
	return jsonCodec[T]{}
}

func (jsonCodec[T]) Encode(value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("messenger: encode JSON: %w", err)
	}
	return data, nil
}

func (jsonCodec[T]) Decode(data []byte) (T, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("messenger: decode JSON: %w", err)
	}
	return value, nil
}

func (jsonCodec[T]) ContentType() string    { return "application/json" }
func (jsonCodec[T]) Encoding() DataEncoding { return DataJSON }

type bytesCodec struct{}

// Bytes returns a binary codec that copies byte slices at the boundary.
func Bytes() Codec[[]byte] {
	return bytesCodec{}
}

func (bytesCodec) Encode(value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (bytesCodec) Decode(data []byte) ([]byte, error) {
	return append([]byte(nil), data...), nil
}

func (bytesCodec) ContentType() string    { return "application/octet-stream" }
func (bytesCodec) Encoding() DataEncoding { return DataBinary }

type textCodec struct{}

// Text returns a UTF-8 text codec.
func Text() Codec[string] {
	return textCodec{}
}

func (textCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }
func (textCodec) Decode(data []byte) (string, error)  { return string(data), nil }
func (textCodec) ContentType() string                 { return "text/plain; charset=utf-8" }
func (textCodec) Encoding() DataEncoding              { return DataText }

type customCodec[T any] struct {
	contentType string
	encoding    DataEncoding
	encode      func(T) ([]byte, error)
	decode      func([]byte) (T, error)
}

// CustomCodec constructs a codec with an explicit content type and wire encoding.
func CustomCodec[T any](
	contentType string,
	encoding DataEncoding,
	encode func(T) ([]byte, error),
	decode func([]byte) (T, error),
) (Codec[T], error) {
	if contentType == "" || encode == nil || decode == nil || !encoding.valid() {
		return nil, fmt.Errorf("%w: invalid custom codec", ErrInvalidDescriptor)
	}
	return customCodec[T]{
		contentType: contentType,
		encoding:    encoding,
		encode:      encode,
		decode:      decode,
	}, nil
}

func (c customCodec[T]) Encode(value T) ([]byte, error) { return c.encode(value) }
func (c customCodec[T]) Decode(data []byte) (T, error)  { return c.decode(data) }
func (c customCodec[T]) ContentType() string            { return c.contentType }
func (c customCodec[T]) Encoding() DataEncoding         { return c.encoding }

// String returns the stable wire name of the data encoding.
func (e DataEncoding) String() string {
	switch e {
	case DataJSON:
		return "json"
	case DataText:
		return "text"
	case DataBinary:
		return "binary"
	default:
		return ""
	}
}

// MarshalJSON encodes a data encoding as its stable wire name.
func (e DataEncoding) MarshalJSON() ([]byte, error) {
	if !e.valid() {
		return nil, fmt.Errorf("%w: invalid data encoding", ErrInvalidDescriptor)
	}
	return json.Marshal(e.String())
}

// UnmarshalJSON decodes a stable data-encoding wire name.
func (e *DataEncoding) UnmarshalJSON(data []byte) error {
	if e == nil {
		return fmt.Errorf("%w: nil data encoding", ErrInvalidDescriptor)
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("%w: decode data encoding: %w", ErrInvalidDescriptor, err)
	}
	switch value {
	case DataJSON.String():
		*e = DataJSON
	case DataText.String():
		*e = DataText
	case DataBinary.String():
		*e = DataBinary
	default:
		return fmt.Errorf("%w: unknown data encoding %q", ErrInvalidDescriptor, value)
	}
	return nil
}

func (e DataEncoding) valid() bool {
	return e == DataJSON || e == DataText || e == DataBinary
}
