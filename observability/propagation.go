package observability

import (
	"context"
	"strings"

	messenger "github.com/assurrussa/gomessenger"
	"go.opentelemetry.io/otel/propagation"
)

// TraceContextPropagator adapts W3C Trace Context to GoMessenger metadata.
// It intentionally does not propagate baggage.
type TraceContextPropagator struct {
	propagator propagation.TraceContext
}

// NewTraceContextPropagator constructs a W3C Trace Context adapter.
func NewTraceContextPropagator() *TraceContextPropagator {
	return &TraceContextPropagator{}
}

// Inject implements messenger.ContextPropagator.
func (propagator *TraceContextPropagator) Inject(ctx context.Context, carrier map[string]string) {
	if propagator == nil {
		return
	}
	removeTraceHeaders(carrier)
	propagator.propagator.Inject(ctx, propagation.MapCarrier(carrier))
}

// Extract implements messenger.ContextPropagator.
func (propagator *TraceContextPropagator) Extract(
	ctx context.Context,
	carrier map[string]string,
) context.Context {
	if propagator == nil {
		return ctx
	}
	return propagator.propagator.Extract(ctx, traceCarrier(carrier))
}

type traceCarrier map[string]string

func (carrier traceCarrier) Get(key string) string {
	if value, exists := carrier[key]; exists {
		return value
	}
	for candidate, value := range carrier {
		if strings.EqualFold(candidate, key) {
			return value
		}
	}
	return ""
}

func (carrier traceCarrier) Set(key, value string) { carrier[key] = value }

func (carrier traceCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier))
	for key := range carrier {
		keys = append(keys, key)
	}
	return keys
}

func removeTraceHeaders(carrier map[string]string) {
	for key := range carrier {
		if strings.EqualFold(key, "traceparent") || strings.EqualFold(key, "tracestate") {
			delete(carrier, key)
		}
	}
}

var _ messenger.ContextPropagator = (*TraceContextPropagator)(nil)
