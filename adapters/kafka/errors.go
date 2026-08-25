package kafka

import "errors"

var (
	// ErrInvalidConfig reports an invalid Kafka adapter declaration.
	ErrInvalidConfig = errors.New("messenger/kafka: invalid configuration")
	// ErrTransportClosed reports use after the managed transport was closed.
	ErrTransportClosed = errors.New("messenger/kafka: transport closed")
	// ErrConsumerClosed reports use after a consumer was closed.
	ErrConsumerClosed = errors.New("messenger/kafka: consumer closed")
	// ErrTopologyDrift reports an unsafe difference from declared Kafka topology.
	ErrTopologyDrift = errors.New("messenger/kafka: topology drift")
	// ErrMessageExpired reports an envelope whose expiry has passed.
	ErrMessageExpired = errors.New("messenger/kafka: message expired")
	// ErrMessageNotReady reports an envelope whose not-before time is in the future.
	ErrMessageNotReady = errors.New("messenger/kafka: message is not ready")
)
