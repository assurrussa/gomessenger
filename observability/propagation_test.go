package observability_test

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/assurrussa/gomessenger/observability"
)

const testTraceState = "vendor=value"

func TestTraceContextPropagatorInjectsExtractsAndOmitsBaggage(t *testing.T) {
	traceID, err := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("trace ID: %v", err)
	}
	spanID, err := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("span ID: %v", err)
	}
	traceState, err := oteltrace.ParseTraceState(testTraceState)
	if err != nil {
		t.Fatalf("trace state: %v", err)
	}
	spanContext := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: oteltrace.FlagsSampled, TraceState: traceState,
	})
	ctx := oteltrace.ContextWithSpanContext(context.Background(), spanContext)
	member, err := baggage.NewMember("tenant", "secret")
	if err != nil {
		t.Fatalf("baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage: %v", err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)

	carrier := map[string]string{
		"TraceParent": "stale",
		"TRACESTATE":  "stale",
		"tenant":      "public",
	}
	propagator := observability.NewTraceContextPropagator()
	propagator.Inject(ctx, carrier)
	if carrier["traceparent"] != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" ||
		carrier["tracestate"] != testTraceState || carrier["tenant"] != "public" {
		t.Fatalf("injected carrier = %#v", carrier)
	}
	for key := range carrier {
		if strings.EqualFold(key, "baggage") {
			t.Fatalf("baggage was propagated: %#v", carrier)
		}
		if (strings.EqualFold(key, "traceparent") && key != "traceparent") ||
			(strings.EqualFold(key, "tracestate") && key != "tracestate") {
			t.Fatalf("stale trace header remains: %#v", carrier)
		}
	}

	extracted := oteltrace.SpanContextFromContext(propagator.Extract(context.Background(), carrier))
	if extracted.TraceID() != traceID || extracted.SpanID() != spanID ||
		extracted.TraceState().String() != testTraceState || !extracted.IsRemote() {
		t.Fatalf("extracted span context = %#v", extracted)
	}
}

func TestTraceContextPropagatorExtractsMixedCaseHeaders(t *testing.T) {
	carrier := map[string]string{
		"TraceParent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"TraceState":  testTraceState,
	}
	extracted := oteltrace.SpanContextFromContext(
		observability.NewTraceContextPropagator().Extract(context.Background(), carrier),
	)
	if extracted.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" ||
		extracted.SpanID().String() != "00f067aa0ba902b7" ||
		extracted.TraceState().String() != testTraceState || !extracted.IsRemote() {
		t.Fatalf("mixed-case extracted span context = %#v", extracted)
	}
}
