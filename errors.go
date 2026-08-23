package messenger

import "errors"

var (
	// ErrInvalidDescriptor reports an invalid command or event descriptor.
	ErrInvalidDescriptor = errors.New("messenger: invalid descriptor")
	// ErrInvalidMessage reports invalid outgoing metadata or envelope data.
	ErrInvalidMessage = errors.New("messenger: invalid message")
	// ErrDescriptorConflict reports two incompatible descriptors with one wire identity.
	ErrDescriptorConflict = errors.New("messenger: descriptor conflict")
	// ErrHandlerConflict reports a duplicate command handler or subscription ID.
	ErrHandlerConflict = errors.New("messenger: handler conflict")
	// ErrHandlerNotFound reports a missing local command handler.
	ErrHandlerNotFound = errors.New("messenger: handler not found")
	// ErrRouteConflict reports more than one primary route for a descriptor.
	ErrRouteConflict = errors.New("messenger: route conflict")
	// ErrRouteNotFound reports that a descriptor has no outbound route.
	ErrRouteNotFound = errors.New("messenger: route not found")
	// ErrServiceConflict reports a duplicate managed service ID.
	ErrServiceConflict = errors.New("messenger: service conflict")
	// ErrRuntimeNotRunning reports an operation that requires a running runtime.
	ErrRuntimeNotRunning = errors.New("messenger: runtime not running")
	// ErrRuntimeRunning reports a second concurrent call to Runtime.Run.
	ErrRuntimeRunning = errors.New("messenger: runtime already running")
	// ErrRuntimeClosed reports use after a runtime has shut down.
	ErrRuntimeClosed = errors.New("messenger: runtime closed")
	// ErrEnvelopeTooLarge reports an envelope beyond the configured wire limit.
	ErrEnvelopeTooLarge = errors.New("messenger: envelope too large")
)
