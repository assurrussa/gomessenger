package nats

import "errors"

var (
	// ErrInvalidConfig reports an invalid route, consumer, or topology declaration.
	ErrInvalidConfig = errors.New("messenger/nats: invalid configuration")
	// ErrTopologyDrift reports an unsafe difference from declared topology.
	ErrTopologyDrift = errors.New("messenger/nats: topology drift")
	// ErrConsumerClosed reports use after consumer shutdown.
	ErrConsumerClosed = errors.New("messenger/nats: consumer closed")
	// ErrMessageNotReady reports an envelope whose NotBefore instant is still in the future.
	ErrMessageNotReady = errors.New("messenger/nats: message is not ready")
	// ErrMessageExpired reports an envelope whose ExpiresAt instant has elapsed.
	ErrMessageExpired = errors.New("messenger/nats: message expired")
)
