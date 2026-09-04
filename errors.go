package messenger

import "errors"

var (
	// ErrInvalidDescriptor reports an invalid command, event, or query descriptor.
	ErrInvalidDescriptor = errors.New("messenger: invalid descriptor")
	// ErrInvalidMessage reports invalid outgoing metadata or envelope data.
	ErrInvalidMessage = errors.New("messenger: invalid message")
	// ErrDescriptorConflict reports two incompatible descriptors with one wire identity.
	ErrDescriptorConflict = errors.New("messenger: descriptor conflict")
	// ErrHandlerConflict reports a duplicate command handler or subscription ID.
	ErrHandlerConflict = errors.New("messenger: handler conflict")
	// ErrHandlerNotFound reports a missing required local handler.
	ErrHandlerNotFound = errors.New("messenger: handler not found")
	// ErrQueryResultMissing reports successful global middleware completion without a query result.
	ErrQueryResultMissing = errors.New("messenger: query result missing")
	// ErrRouteConflict reports more than one primary route for a descriptor.
	ErrRouteConflict = errors.New("messenger: route conflict")
	// ErrRouteNotFound reports that a descriptor has no outbound route.
	ErrRouteNotFound = errors.New("messenger: route not found")
	// ErrUnsupportedCapability reports a requested semantic guarantee that a route cannot provide.
	ErrUnsupportedCapability = errors.New("messenger: unsupported route capability")
	// ErrMessageExpired reports a message whose expiration boundary has been reached.
	ErrMessageExpired = errors.New("messenger: message expired")
	// ErrMessageNotReady reports a message whose not-before boundary is still in the future.
	ErrMessageNotReady = errors.New("messenger: message not ready")
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
	// ErrInvalidBatchResult reports a missing, duplicate, unknown, or otherwise
	// inconsistent item in a batch handler result.
	ErrInvalidBatchResult = errors.New("messenger: invalid batch result")
)
