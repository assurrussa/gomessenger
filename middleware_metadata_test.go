package messenger_test

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	metadataTestPayload   = "metadata-payload"
	metadataForgedContext = "forged"
)

type middlewareMetadataKey struct{}

type metadataObservation struct {
	message messenger.Metadata
	context messenger.Metadata
	present bool
	value   any
	child   messenger.Metadata
	err     error
}

func TestLocalMiddlewareReplacementPreservesCanonicalMetadata(t *testing.T) {
	for _, asynchronous := range []bool{false, true} {
		name := "sync"
		if asynchronous {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			for _, replacement := range []string{"derived", "background", metadataForgedContext} {
				t.Run(replacement, func(t *testing.T) {
					testLocalMiddlewareMetadata(t, asynchronous, replacement)
				})
			}
		})
	}
}

func testLocalMiddlewareMetadata(t *testing.T, asynchronous bool, replacement string) {
	t.Helper()
	command := messenger.MustCommand("metadata.command", 1, messenger.JSON[string]())
	event := messenger.MustEvent("metadata.event", 1, messenger.JSON[string]())
	query := messenger.MustQuery[string, string]("metadata.query", 1, messenger.JSON[string]())
	childQuery := messenger.MustQuery[string, messenger.Metadata]("metadata.child", 1, messenger.JSON[string]())
	childBuilder := messenger.NewBuilder(messenger.WithSource(testSource))
	childBuilder.HandleQuery(childQuery, "child", func(
		_ context.Context, message messenger.Message[string],
	) (messenger.Metadata, error) {
		return message.Metadata, nil
	})
	childBuilder.RouteQuery(childQuery, messenger.NewLocalSyncRoute())
	child, _, err := childBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}

	canonical := make(chan messenger.Metadata, 4)
	observed := make(chan metadataObservation, 4)
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	//nolint:contextcheck // Exercise replacement contexts instead of only inherited middleware contexts.
	builder.UseMiddleware(func(ctx context.Context, metadata messenger.Metadata, _ string, next messenger.HandlerFunc) error {
		original := metadata
		original.Headers = maps.Clone(metadata.Headers)
		canonical <- original
		if replacement != "derived" {
			ctx = context.Background() // Exercise a middleware that intentionally replaces the parent context.
		}
		if replacement == metadataForgedContext {
			forged := original
			forged.ID[15] ^= 1
			forged.CorrelationID[15] ^= 1
			forged.Name = metadataForgedContext
			forged.Source = "urn:forged"
			forged.Headers = map[string]string{"forged-header": "true"}
			ctx = messenger.ContextWithMetadata(ctx, forged)
		}
		if metadata.Headers != nil {
			metadata.Headers["application"] = "mutated middleware copy"
		}
		return next(context.WithValue(ctx, middlewareMetadataKey{}, replacement))
	})
	handler := func(ctx context.Context, message messenger.Message[string]) error {
		metadata, present := messenger.MetadataFromContext(ctx)
		childMetadata, childErr := child.Query(ctx, childQuery, "child")
		messageSnapshot := message.Metadata
		messageSnapshot.Headers = maps.Clone(message.Metadata.Headers)
		observed <- metadataObservation{
			message: messageSnapshot, context: metadata, present: present,
			value: ctx.Value(middlewareMetadataKey{}), child: childMetadata, err: childErr,
		}
		// Mutation by one subscription must not affect another subscription or the context snapshot.
		if message.Metadata.Headers != nil {
			message.Metadata.Headers["application"] = "mutated handler copy"
		}
		return childErr
	}
	builder.HandleCommand(command, "command", handler)
	builder.Subscribe(event, "event-one", handler)
	builder.Subscribe(event, "event-two", handler)
	builder.HandleQuery(query, "query", func(ctx context.Context, message messenger.Message[string]) (string, error) {
		return message.Payload, handler(ctx, message)
	})
	if asynchronous {
		route, routeErr := messenger.NewLocalAsyncRoute("metadata.async", messenger.LocalAsyncConfig{Capacity: 4, Workers: 1})
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		builder.RouteCommand(command, route)
		builder.RouteEvent(event, route)
		builder.RouteQuery(query, route)
	} else {
		route := messenger.NewLocalSyncRoute()
		builder.RouteCommand(command, route)
		builder.RouteEvent(event, route)
		builder.RouteQuery(query, route)
	}
	instance, runtime, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if asynchronous {
		startMetadataRuntime(t, runtime)
	}
	outgoing := messenger.Outgoing[string]{Payload: metadataTestPayload, Metadata: messenger.OutgoingMetadata{
		Subject: "subject", Key: "message-key", Headers: map[string]string{"application": "original"},
	}}
	if _, err := instance.SendMessage(t.Context(), command, outgoing); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.PublishMessage(t.Context(), event, outgoing); err != nil {
		t.Fatal(err)
	}
	if result, err := instance.Query(t.Context(), query, metadataTestPayload); err != nil || result != metadataTestPayload {
		t.Fatalf("query = %q, %v", result, err)
	}
	for range 4 {
		select {
		case got := <-observed:
			want := <-canonical
			assertMetadataObservation(t, got, want, replacement)
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not complete")
		}
	}
}

func startMetadataRuntime(t *testing.T, runtime *messenger.Runtime) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("runtime: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("runtime did not stop")
		}
	})
	waitRuntimeReady(t, runtime)
}

func assertMetadataObservation(t *testing.T, got metadataObservation, want messenger.Metadata, replacement string) {
	t.Helper()
	if !got.present || !reflect.DeepEqual(got.context, want) || !reflect.DeepEqual(got.message, want) {
		t.Errorf("canonical metadata changed: context=%+v, message=%+v, want=%+v", got.context, got.message, want)
	}
	if got.value != replacement {
		t.Errorf("replacement context value = %v, want %s", got.value, replacement)
	}
	if got.err != nil || got.child.CausationID != want.ID || got.child.CorrelationID != want.CorrelationID {
		t.Errorf("child lineage = %+v, error=%v; parent=%+v", got.child, got.err, want)
	}
}

func TestLocalMiddlewareReplacementPreservesDeadlineAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "deadline"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			deadline := time.Now().Add(time.Minute)
			replacement, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			if canceled {
				cancel()
			}
			command := messenger.MustCommand("metadata.cancel", 1, messenger.JSON[string]())
			builder := messenger.NewBuilder(messenger.WithSource(testSource))
			builder.UseMiddleware(func(_ context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
				return next(replacement)
			})
			builder.HandleCommand(command, "handler", func(ctx context.Context, message messenger.Message[string]) error {
				actual, ok := ctx.Deadline()
				if !ok || !actual.Equal(deadline) || ctx.Done() != replacement.Done() || !errors.Is(ctx.Err(), replacement.Err()) {
					t.Error("replacement deadline or cancellation changed")
				}
				metadata, present := messenger.MetadataFromContext(ctx)
				if !present || metadata.ID.IsZero() || !reflect.DeepEqual(metadata, message.Metadata) {
					t.Error("canonical metadata missing from replacement context")
				}
				return nil
			})
			builder.RouteCommand(command, messenger.NewLocalSyncRoute())
			instance, _, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := instance.Send(t.Context(), command, metadataTestPayload); err != nil {
				t.Fatal(err)
			}
		})
	}
}
