package messenger

import (
	"encoding/json"
	"fmt"
	"strconv"
	"uuid"
)

// MessageID is a UUID-compatible 128-bit message identity.
type MessageID [16]byte

// IDGenerator creates stable message identities.
type IDGenerator interface {
	New() (MessageID, error)
}

type uuidV7Generator struct{}

// UUIDv7Generator returns the default cryptographically random UUIDv7 generator.
func UUIDv7Generator() IDGenerator {
	return uuidV7Generator{}
}

func (uuidV7Generator) New() (MessageID, error) {
	return MessageID(uuid.NewV7()), nil
}

// ParseMessageID parses the canonical UUID text form.
func ParseMessageID(value string) (MessageID, error) {
	var id MessageID
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return id, fmt.Errorf("%w: malformed message id", ErrInvalidMessage)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return MessageID{}, fmt.Errorf("%w: malformed message id: %w", ErrInvalidMessage, err)
	}
	if parsed.String() != value {
		return MessageID{}, fmt.Errorf("%w: message id is not canonical", ErrInvalidMessage)
	}
	return MessageID(parsed), nil
}

// IsZero reports whether no message identity is set.
func (id MessageID) IsZero() bool { return id == MessageID{} }

// String returns the canonical lowercase UUID text form.
func (id MessageID) String() string {
	return uuid.UUID(id).String()
}

// MarshalText implements encoding.TextMarshaler.
func (id MessageID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *MessageID) UnmarshalText(text []byte) error {
	parsed, err := ParseMessageID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// MarshalJSON implements json.Marshaler.
func (id MessageID) MarshalJSON() ([]byte, error) { return json.Marshal(id.String()) }

// UnmarshalJSON implements json.Unmarshaler.
func (id *MessageID) UnmarshalJSON(data []byte) error {
	value, err := strconv.Unquote(string(data))
	if err != nil {
		return fmt.Errorf("%w: message id must be a string", ErrInvalidMessage)
	}
	return id.UnmarshalText([]byte(value))
}
