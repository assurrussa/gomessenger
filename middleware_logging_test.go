package messenger_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type recordedLog struct {
	level messenger.LogLevel
	text  string
	attrs []messenger.LogAttr
}

const testAdapterMessage = "adapter record"

type testLogger struct {
	mu      sync.Mutex
	records []recordedLog
}

func (logger *testLogger) Log(
	_ context.Context,
	level messenger.LogLevel,
	message string,
	attrs ...messenger.LogAttr,
) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.records = append(logger.records, recordedLog{
		level: level,
		text:  message,
		attrs: append([]messenger.LogAttr(nil), attrs...),
	})
}

func (logger *testLogger) snapshot() []recordedLog {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return append([]recordedLog(nil), logger.records...)
}

func TestBuilderMiddlewareOrderContextAndPayloadHelpers(t *testing.T) {
	type contextKey struct{}
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[processedPayload]())
	var order []string
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.UseMiddleware(
		func(
			ctx context.Context,
			_ messenger.Metadata,
			handlerID string,
			next messenger.HandlerFunc,
		) error {
			order = append(order, "first.before:"+handlerID)
			err := next(context.WithValue(ctx, contextKey{}, "installed"))
			order = append(order, "first.after:"+handlerID)
			return err
		},
		func(ctx context.Context, _ messenger.Metadata, handlerID string, next messenger.HandlerFunc) error {
			order = append(order, "second.before:"+handlerID)
			err := next(ctx)
			order = append(order, "second.after:"+handlerID)
			return err
		},
	)
	builder.HandleCommandFunc(command, "command-handler", func(ctx context.Context, payload processPayload) error {
		if ctx.Value(contextKey{}) != "installed" || payload.JobID != 42 {
			return errors.New("middleware context or payload was not preserved")
		}
		order = append(order, "handler:command-handler")
		return nil
	})
	builder.SubscribeFunc(event, "event-subscriber", func(_ context.Context, payload processedPayload) error {
		if payload.JobID != 42 {
			return errors.New("event payload was not preserved")
		}
		return nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	builder.RouteEvent(event, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.Send(t.Context(), command, processPayload{JobID: 42}); err != nil {
		t.Fatalf("send: %v", err)
	}
	want := []string{
		"first.before:command-handler",
		"second.before:command-handler",
		"handler:command-handler",
		"second.after:command-handler",
		"first.after:command-handler",
	}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("middleware order = %v, want %v", order, want)
	}
	if _, err := instance.Publish(t.Context(), event, processedPayload{JobID: 42}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestMiddlewareShortCircuitAndNextAtMostOnce(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	shortCircuit := errors.New("blocked")
	var handled atomic.Int32
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.UseMiddleware(func(
		context.Context,
		messenger.Metadata,
		string,
		messenger.HandlerFunc,
	) error {
		return shortCircuit
	})
	builder.HandleCommandFunc(command, "handler", func(context.Context, processPayload) error {
		handled.Add(1)
		return nil
	})
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.Send(t.Context(), command, processPayload{}); !errors.Is(err, shortCircuit) {
		t.Fatalf("short circuit error = %v", err)
	}
	if handled.Load() != 0 {
		t.Fatalf("short-circuited handler calls = %d", handled.Load())
	}

	doubleBuilder := messenger.NewBuilder(messenger.WithSource(testSource))
	doubleBuilder.UseMiddleware(func(
		ctx context.Context,
		_ messenger.Metadata,
		_ string,
		next messenger.HandlerFunc,
	) error {
		if err := next(ctx); err != nil {
			return err
		}
		return next(ctx)
	})
	doubleBuilder.HandleCommandFunc(command, "handler", func(context.Context, processPayload) error {
		handled.Add(1)
		return nil
	})
	doubleBuilder.RouteCommand(command, messenger.NewLocalSyncRoute())
	double, _, err := doubleBuilder.Build()
	if err != nil {
		t.Fatalf("build double-next: %v", err)
	}
	if _, err := double.Send(t.Context(), command, processPayload{}); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("double-next error = %v", err)
	}
	if handled.Load() != 1 {
		t.Fatalf("double-next handler calls = %d", handled.Load())
	}
}

func TestTypedMiddlewareAndAsyncExecution(t *testing.T) {
	type payload struct{ Value int }
	command := messenger.MustCommand("jobs.run", 1, messenger.JSON[payload]())
	var order []string
	handler := messenger.ChainHandler(
		func(_ context.Context, message messenger.Message[payload]) error {
			order = append(order, fmt.Sprintf("handler:%d", message.Payload.Value))
			return nil
		},
		func(next messenger.Handler[payload]) messenger.Handler[payload] {
			return func(ctx context.Context, message messenger.Message[payload]) error {
				order = append(order, "typed.before")
				err := next(ctx, message)
				order = append(order, "typed.after")
				return err
			}
		},
	)
	route, err := messenger.NewLocalAsyncRoute("local.async.test", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1})
	if err != nil {
		t.Fatalf("new async route: %v", err)
	}
	done := make(chan struct{})
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.UseMiddleware(func(
		ctx context.Context,
		_ messenger.Metadata,
		_ string,
		next messenger.HandlerFunc,
	) error {
		defer close(done)
		return next(ctx)
	})
	builder.HandleCommand(command, "async-handler", handler)
	builder.RouteCommand(command, route)
	instance, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	eventuallyRuntimeReady(t, runtime)
	if _, err := instance.Send(t.Context(), command, payload{Value: 7}); err != nil {
		t.Fatalf("send async: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("async middleware did not finish")
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"typed.before", "handler:7", "typed.after"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("typed middleware order = %v, want %v", order, want)
	}
}

func TestObserverFanoutLogsPanicAndContinues(t *testing.T) {
	command := messenger.MustCommand("media.processor", 1, messenger.JSON[processPayload]())
	logger := &testLogger{}
	panicking := &recordingObserver{panic: true}
	recording := &recordingObserver{}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithLogger(logger),
		messenger.WithObserver(panicking),
		messenger.WithObserver(recording),
		messenger.WithObserver(messenger.NewLoggingObserver(logger)),
	)
	builder.HandleCommandFunc(command, "handler", func(context.Context, processPayload) error { return nil })
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.Send(t.Context(), command, processPayload{}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(recording.observations) != 2 {
		t.Fatalf("observations after panic = %d, want 2", len(recording.observations))
	}
	records := logger.snapshot()
	var panicLogs, debugLogs int
	for _, record := range records {
		switch {
		case record.text == "messenger observer panicked" && record.level == messenger.LogError:
			panicLogs++
		case record.text == "messenger operation completed" && record.level == messenger.LogDebug:
			debugLogs++
		}
		for _, attr := range record.attrs {
			if attr.Key == "payload" || attr.Key == "headers" {
				t.Fatalf("sensitive log attribute %q", attr.Key)
			}
		}
	}
	if panicLogs != 2 || debugLogs != 2 {
		t.Fatalf("panic logs=%d debug logs=%d records=%#v", panicLogs, debugLogs, records)
	}
	messenger.AdaptSlog(nil).Log(t.Context(), messenger.LogInfo, "discarded")
}

func TestSlogAdapterLevelsNoopPropagationAndFailedObservation(t *testing.T) {
	var output bytes.Buffer
	adapted := messenger.AdaptSlog(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	for _, level := range []messenger.LogLevel{
		messenger.LogDebug,
		messenger.LogInfo,
		messenger.LogWarn,
		messenger.LogError,
		messenger.LogLevel(99),
	} {
		adapted.Log(t.Context(), level, testAdapterMessage,
			messenger.LogAttr{}, messenger.LogAttr{Key: "component", Value: "test"})
	}
	if strings.Count(output.String(), "msg=\""+testAdapterMessage+"\"") != 5 ||
		strings.Count(output.String(), "component=test") != 5 {
		t.Fatalf("slog output = %q", output.String())
	}

	propagator := messenger.NoopContextPropagator()
	carrier := map[string]string{"tenant": testTenantValue}
	propagator.Inject(t.Context(), carrier)
	if extracted := propagator.Extract(t.Context(), carrier); extracted != t.Context() ||
		carrier["tenant"] != testTenantValue {
		t.Fatalf("no-op propagation changed state: context=%v carrier=%v", extracted, carrier)
	}

	logger := &testLogger{}
	failure := errors.New("broker rejected")
	messenger.NewLoggingObserver(logger).Observe(t.Context(), messenger.Observation{
		Operation: messenger.OperationDeliver,
		State:     messenger.ReceiptBrokerConfirmed,
		Err:       failure,
	})
	records := logger.snapshot()
	if len(records) != 1 || records[0].level != messenger.LogError ||
		records[0].text != "messenger operation failed" {
		t.Fatalf("failed observation logs = %#v", records)
	}
	messenger.NewLoggingObserver(nil).Observe(t.Context(), messenger.Observation{})
}

func eventuallyRuntimeReady(t *testing.T, runtime *messenger.Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if err := runtime.Readiness(t.Context()); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
}
