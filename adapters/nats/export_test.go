package nats

import (
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

// Exported for testing.
var (
	EncodeCloudEventStructured = encodeCloudEventStructured
	DecodeCloudEventStructured = decodeCloudEventStructured
	EncodeCloudEventBinary     = encodeCloudEventBinary
	DecodeCloudEventBinary     = decodeCloudEventBinary
)

type CloudEventEnvelope = cloudEventEnvelope

func NewTestRoute(name, namespace string, mode WireMode, clock func() time.Time) *Route {
	return &Route{name: name, namespace: namespace, mode: mode, clock: clock}
}

func CloudEventEnvelopeMetadata(e cloudEventEnvelope) messenger.Metadata {
	return e.metadata
}

func CloudEventEnvelopeData(e cloudEventEnvelope) []byte {
	return e.data
}

func CloudEventEnvelopeDataEncoding(e cloudEventEnvelope) messenger.DataEncoding {
	return e.encoding
}
