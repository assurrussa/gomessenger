package messenger

import (
	"context"
	"time"
)

// Operation identifies an observable messaging boundary.
type Operation string

const (
	// OperationDeliver covers outbound route delivery.
	OperationDeliver Operation = "deliver"
	// OperationHandle covers local or durable handler execution.
	OperationHandle Operation = "handle"
	// OperationQuery covers a complete local request/reply call.
	OperationQuery Operation = "query"
	// OperationService covers managed service completion.
	OperationService Operation = "service"
	// OperationBrokerAck covers broker-confirmed acknowledgement of a consumed message.
	OperationBrokerAck Operation = "broker_ack"
	// OperationOffsetCommit covers transactional Kafka offset finalization.
	OperationOffsetCommit Operation = "offset_commit"
	// OperationRetryHandoff covers durable retry scheduling or broker hand-off.
	OperationRetryHandoff Operation = "retry_handoff"
	// OperationDLQHandoff covers durable terminal hand-off to a dead-letter destination.
	OperationDLQHandoff Operation = "dlq_handoff"
)

// Observation contains bounded operational data. Observers decide which
// fields are safe for low-cardinality metric labels.
type Observation struct {
	Operation     Operation
	MessageID     MessageID
	Kind          Kind
	Name          string
	SchemaVersion int
	Route         string
	HandlerID     string
	ConsumerID    string
	ServiceID     string
	Attempt       uint64
	Duplicate     bool
	RetryDelay    time.Duration
	State         ReceiptState
	StartedAt     time.Time
	Duration      time.Duration
	Err           error
}

// Observer receives messaging lifecycle observations. Implementations must not
// retain ctx and should return quickly.
type Observer interface {
	Observe(ctx context.Context, observation Observation)
}

func observe(ctx context.Context, observer Observer, observation Observation) {
	if observer == nil {
		return
	}
	observer.Observe(ctx, observation)
}

type observerSet struct {
	logger    Logger
	observers []Observer
}

func newObserverSet(logger Logger, observers []Observer) Observer {
	if len(observers) == 0 {
		return nil
	}
	return observerSet{logger: logger, observers: append([]Observer(nil), observers...)}
}

func (set observerSet) Observe(ctx context.Context, observation Observation) {
	for _, observer := range set.observers {
		callObserver(ctx, set.logger, observer, observation)
	}
}

func callObserver(ctx context.Context, logger Logger, observer Observer, observation Observation) {
	defer func() {
		if recovered := recover(); recovered != nil {
			attrs := []LogAttr{{Key: "operation", Value: observation.Operation}}
			if !observation.MessageID.IsZero() {
				attrs = append(attrs, LogAttr{Key: "message_id", Value: observation.MessageID.String()})
			}
			attrs = append(attrs, LogAttr{Key: "observer_panic", Value: true})
			safeLog(ctx, logger, LogError, "messenger observer panicked", attrs...)
		}
	}()
	observer.Observe(ctx, observation)
}

type loggingObserver struct {
	logger    Logger
	sanitizer FailureSanitizer
}

// NewLoggingObserver reports sanitized observations through logger. Successful
// operations use Debug and failed operations use Error. A nil logger creates a
// no-op observer.
func NewLoggingObserver(logger Logger) Observer {
	return NewSanitizedLoggingObserver(logger, DefaultFailureSanitizer())
}

// NewSanitizedLoggingObserver reports observations with an explicit failure
// sanitizer. A nil sanitizer uses DefaultFailureSanitizer.
func NewSanitizedLoggingObserver(logger Logger, sanitizer FailureSanitizer) Observer {
	if nilInterface(logger) {
		logger = noopLogger{}
	}
	if nilInterface(sanitizer) {
		sanitizer = DefaultFailureSanitizer()
	}
	return loggingObserver{logger: logger, sanitizer: sanitizer}
}

func (observer loggingObserver) Observe(ctx context.Context, observation Observation) {
	level := LogDebug
	message := "messenger operation completed"
	attrs := observationLogAttrs(observation)
	if observation.Err != nil {
		level = LogError
		message = "messenger operation failed"
		attrs = append(attrs, LogAttr{Key: logAttrErrorKey, Value: SanitizeError(observer.sanitizer, observation.Err)})
	}
	safeLog(ctx, observer.logger, level, message, attrs...)
}

func observationLogAttrs(observation Observation) []LogAttr {
	attrs := make([]LogAttr, 0, 14)
	attrs = append(attrs, LogAttr{Key: "operation", Value: observation.Operation})
	if !observation.MessageID.IsZero() {
		attrs = append(attrs, LogAttr{Key: "message_id", Value: observation.MessageID.String()})
	}
	if observation.Kind != "" {
		attrs = append(attrs, LogAttr{Key: "kind", Value: observation.Kind})
	}
	if observation.Name != "" {
		attrs = append(attrs, LogAttr{Key: "name", Value: observation.Name})
	}
	if observation.SchemaVersion != 0 {
		attrs = append(attrs, LogAttr{Key: "schema_version", Value: observation.SchemaVersion})
	}
	for _, attr := range []LogAttr{
		{Key: "route", Value: observation.Route},
		{Key: "handler_id", Value: observation.HandlerID},
		{Key: "consumer_id", Value: observation.ConsumerID},
		{Key: logAttrServiceIDKey, Value: observation.ServiceID},
	} {
		if value, ok := attr.Value.(string); ok && value != "" {
			attrs = append(attrs, attr)
		}
	}
	if observation.State != "" {
		attrs = append(attrs, LogAttr{Key: "receipt_state", Value: observation.State})
	}
	if observation.Attempt != 0 {
		attrs = append(attrs, LogAttr{Key: "attempt", Value: observation.Attempt})
	}
	if observation.Duplicate {
		attrs = append(attrs, LogAttr{Key: "duplicate", Value: true})
	}
	if observation.RetryDelay != 0 {
		attrs = append(attrs, LogAttr{Key: "retry_delay", Value: observation.RetryDelay})
	}
	attrs = append(attrs, LogAttr{Key: "duration", Value: observation.Duration})
	return attrs
}
