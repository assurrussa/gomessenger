package messenger

import (
	"context"
	"maps"
	"time"
)

// Metadata is canonical message metadata independent of a transport attempt.
type Metadata struct {
	ID            MessageID
	Kind          Kind
	Name          string
	SchemaVersion int
	Source        string
	Subject       string
	Time          time.Time
	CorrelationID MessageID
	CausationID   MessageID
	Key           string
	ContentType   string
	Schema        string
	Headers       map[string]string
	NotBefore     time.Time
	ExpiresAt     time.Time
}

// Message is the typed value passed to a handler.
type Message[T any] struct {
	Metadata Metadata
	Payload  T
}

// OutgoingMetadata customizes metadata generated for a new outgoing message.
// Source, kind, name, schema version, content type, and schema come from the
// builder and descriptor and cannot be overridden per call.
type OutgoingMetadata struct {
	ID            MessageID
	Subject       string
	Time          time.Time
	CorrelationID MessageID
	CausationID   MessageID
	Key           string
	Headers       map[string]string
	NotBefore     time.Time
	ExpiresAt     time.Time
}

// Outgoing combines a typed payload with optional explicit metadata.
type Outgoing[T any] struct {
	Payload  T
	Metadata OutgoingMetadata
}

type messageContextKey struct{}

// MetadataFromContext returns the currently handled message metadata.
func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	if ctx == nil {
		return Metadata{}, false
	}
	metadata, ok := ctx.Value(messageContextKey{}).(Metadata)
	if !ok {
		return Metadata{}, false
	}
	metadata.Headers = maps.Clone(metadata.Headers)
	return metadata, true
}

// ContextWithMetadata installs immutable message lineage for an adapter or
// terminal handler entering the typed messenger boundary. As with standard
// context helpers, ctx must be non-nil.
func ContextWithMetadata(ctx context.Context, metadata Metadata) context.Context {
	metadata.Headers = maps.Clone(metadata.Headers)
	return context.WithValue(ctx, messageContextKey{}, metadata)
}

func contextWithMetadata(ctx context.Context, metadata Metadata) context.Context {
	return ContextWithMetadata(ctx, metadata)
}

func cloneMetadata(metadata Metadata) Metadata {
	metadata.Headers = maps.Clone(metadata.Headers)
	return metadata
}
