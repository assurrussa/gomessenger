package messenger

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"time"
)

const (
	// EnvelopeSpecVersion is the current native envelope contract.
	EnvelopeSpecVersion = "1.0"
	// DefaultMaxEnvelopeBytes is the default encoded envelope limit.
	DefaultMaxEnvelopeBytes = 1 << 20
	// DefaultMaxHeaders is the default number of application headers.
	DefaultMaxHeaders = 64
	// DefaultMaxHeaderBytes is the default aggregate application-header limit.
	DefaultMaxHeaderBytes = 16 << 10
)

// Envelope is the canonical native wire representation.
type Envelope struct {
	SpecVersion   string            `json:"specVersion"`
	ID            MessageID         `json:"id"`
	Kind          Kind              `json:"kind"`
	Name          string            `json:"name"`
	SchemaVersion int               `json:"schemaVersion"`
	Source        string            `json:"source"`
	Subject       string            `json:"subject,omitempty"`
	Time          time.Time         `json:"time"`
	CorrelationID MessageID         `json:"correlationId"`
	CausationID   MessageID         `json:"causationId,omitzero"`
	Key           string            `json:"key,omitempty"`
	ContentType   string            `json:"contentType"`
	DataEncoding  DataEncoding      `json:"dataEncoding"`
	Schema        string            `json:"schema,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	NotBefore     time.Time         `json:"notBefore,omitzero"`
	ExpiresAt     time.Time         `json:"expiresAt,omitzero"`
	Data          json.RawMessage   `json:"data,omitempty"`
	DataBase64    *string           `json:"dataBase64,omitempty"`
}

// Metadata returns a defensive copy of the envelope metadata.
func (e Envelope) Metadata() Metadata {
	return Metadata{
		ID:            e.ID,
		Kind:          e.Kind,
		Name:          e.Name,
		SchemaVersion: e.SchemaVersion,
		Source:        e.Source,
		Subject:       e.Subject,
		Time:          e.Time,
		CorrelationID: e.CorrelationID,
		CausationID:   e.CausationID,
		Key:           e.Key,
		ContentType:   e.ContentType,
		Schema:        e.Schema,
		Headers:       maps.Clone(e.Headers),
		NotBefore:     e.NotBefore,
		ExpiresAt:     e.ExpiresAt,
	}
}

// Validate checks the native envelope invariants and configured default bounds.
func (e Envelope) Validate() error {
	if e.SpecVersion != EnvelopeSpecVersion || e.ID.IsZero() || !e.Kind.validWire() ||
		len(e.Name) == 0 || len(e.Name) > maxDescriptorNameLength ||
		!descriptorNamePattern.MatchString(e.Name) || e.SchemaVersion <= 0 ||
		e.Time.IsZero() || e.CorrelationID.IsZero() || e.ContentType == "" || !e.DataEncoding.valid() {
		return fmt.Errorf("%w: incomplete envelope", ErrInvalidMessage)
	}
	if err := validateSource(e.Source); err != nil {
		return err
	}
	if _, err := e.payload(); err != nil {
		return err
	}
	if !e.NotBefore.IsZero() && !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(e.NotBefore) {
		return fmt.Errorf("%w: expiresAt must follow notBefore", ErrInvalidMessage)
	}
	return validateHeaders(e.Headers)
}

// Payload returns decoded codec bytes and their envelope representation.
func (e Envelope) Payload() ([]byte, DataEncoding, error) {
	payload, err := e.payload()
	return payload, e.DataEncoding, err
}

func (e Envelope) payload() ([]byte, error) {
	switch e.DataEncoding {
	case DataBinary:
		if e.Data != nil || e.DataBase64 == nil {
			return nil, fmt.Errorf("%w: binary data requires dataBase64", ErrInvalidMessage)
		}
		data, err := base64.StdEncoding.DecodeString(*e.DataBase64)
		if err != nil {
			return nil, fmt.Errorf("%w: decode dataBase64: %w", ErrInvalidMessage, err)
		}
		return data, nil
	case DataText:
		if e.Data == nil || e.DataBase64 != nil {
			return nil, fmt.Errorf("%w: text data requires data", ErrInvalidMessage)
		}
		var value string
		if err := json.Unmarshal(e.Data, &value); err != nil {
			return nil, fmt.Errorf("%w: decode text data: %w", ErrInvalidMessage, err)
		}
		return []byte(value), nil
	case DataJSON:
		if e.Data == nil || e.DataBase64 != nil {
			return nil, fmt.Errorf("%w: JSON data requires data", ErrInvalidMessage)
		}
		if !json.Valid(e.Data) {
			return nil, fmt.Errorf("%w: invalid JSON data", ErrInvalidMessage)
		}
		return append([]byte(nil), e.Data...), nil
	default:
		return nil, fmt.Errorf("%w: unknown data encoding", ErrInvalidMessage)
	}
}

// MarshalEnvelope validates and encodes an envelope with a codec payload.
func MarshalEnvelope(metadata Metadata, payload []byte, encoding DataEncoding) ([]byte, error) {
	envelope, err := envelopeFrom(metadata, payload, encoding)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("messenger: marshal envelope: %w", err)
	}
	if len(data) > DefaultMaxEnvelopeBytes {
		return nil, fmt.Errorf("%w: got %d bytes", ErrEnvelopeTooLarge, len(data))
	}
	return data, nil
}

// UnmarshalEnvelope parses and validates a native envelope.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	if len(data) > DefaultMaxEnvelopeBytes {
		return Envelope{}, fmt.Errorf("%w: got %d bytes", ErrEnvelopeTooLarge, len(data))
	}
	var envelope Envelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("%w: decode envelope: %w", ErrInvalidMessage, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Envelope{}, fmt.Errorf("%w: decode envelope: %w", ErrInvalidMessage, err)
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

// CanonicalizeEnvelope parses and re-encodes an envelope using the native
// deterministic field order. Delivery metadata is never included.
func CanonicalizeEnvelope(data []byte) ([]byte, error) {
	envelope, err := UnmarshalEnvelope(data)
	if err != nil {
		return nil, err
	}
	payload, encoding, err := envelope.Payload()
	if err != nil {
		return nil, err
	}
	return MarshalEnvelope(envelope.Metadata(), payload, encoding)
}

// EnvelopeFingerprint returns SHA-256 over canonical encoded envelope bytes.
func EnvelopeFingerprint(data []byte) [sha256.Size]byte { return sha256.Sum256(data) }

func envelopeFrom(metadata Metadata, payload []byte, encoding DataEncoding) (Envelope, error) {
	envelope := Envelope{
		SpecVersion:   EnvelopeSpecVersion,
		ID:            metadata.ID,
		Kind:          metadata.Kind,
		Name:          metadata.Name,
		SchemaVersion: metadata.SchemaVersion,
		Source:        metadata.Source,
		Subject:       metadata.Subject,
		Time:          metadata.Time.UTC(),
		CorrelationID: metadata.CorrelationID,
		CausationID:   metadata.CausationID,
		Key:           metadata.Key,
		ContentType:   metadata.ContentType,
		DataEncoding:  encoding,
		Schema:        metadata.Schema,
		Headers:       maps.Clone(metadata.Headers),
		NotBefore:     utcOrZero(metadata.NotBefore),
		ExpiresAt:     utcOrZero(metadata.ExpiresAt),
	}
	switch encoding {
	case DataJSON:
		if len(payload) == 0 {
			payload = []byte("null")
		}
		if !json.Valid(payload) {
			return Envelope{}, fmt.Errorf("%w: codec returned invalid JSON", ErrInvalidMessage)
		}
		envelope.Data = append(json.RawMessage(nil), payload...)
	case DataText:
		data, err := json.Marshal(string(payload))
		if err != nil {
			return Envelope{}, fmt.Errorf("messenger: encode text payload: %w", err)
		}
		envelope.Data = data
	case DataBinary:
		encoded := base64.StdEncoding.EncodeToString(payload)
		envelope.DataBase64 = &encoded
	default:
		return Envelope{}, fmt.Errorf("%w: unknown data encoding", ErrInvalidMessage)
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func validateHeaders(headers map[string]string) error {
	if len(headers) > DefaultMaxHeaders {
		return fmt.Errorf("%w: too many headers", ErrInvalidMessage)
	}
	total := 0
	traceParentSeen := false
	traceStateSeen := false
	for key, value := range headers {
		if key == "" || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\r\n") ||
			strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("%w: invalid header", ErrInvalidMessage)
		}
		switch {
		case strings.EqualFold(key, "traceparent"):
			if traceParentSeen {
				return fmt.Errorf("%w: duplicate trace header", ErrInvalidMessage)
			}
			traceParentSeen = true
		case strings.EqualFold(key, "tracestate"):
			if traceStateSeen {
				return fmt.Errorf("%w: duplicate trace header", ErrInvalidMessage)
			}
			traceStateSeen = true
		}
		total += len(key) + len(value)
	}
	if total > DefaultMaxHeaderBytes {
		return fmt.Errorf("%w: headers exceed byte limit", ErrInvalidMessage)
	}
	return nil
}
