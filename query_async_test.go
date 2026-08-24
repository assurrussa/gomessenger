package messenger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestAsyncQueryRequiresRunningRuntimeAndDrainsAcceptedWork(t *testing.T) {
	query := messenger.MustQuery[int, int]("async.query", 1, messenger.JSON[int]())
	route, err := messenger.NewLocalAsyncRoute(
		"local.query", messenger.LocalAsyncConfig{Capacity: 2, Workers: 1},
	)
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(query, "async.query", func(ctx context.Context, value int) (int, error) {
		close(started)
		select {
		case <-release:
			return value * 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	builder.RouteQuery(query, route)
	instance, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("query before Run = %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	waitRuntimeReady(t, runtime)
	queryDone := make(chan struct {
		result int
		err    error
	}, 1)
	go func() {
		result, queryErr := instance.Query(t.Context(), query, 21)
		queryDone <- struct {
			result int
			err    error
		}{result: result, err: queryErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query handler did not start")
	}
	runtime.BeginDrain()
	if _, err := instance.Query(t.Context(), query, 22); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
		t.Fatalf("query during drain = %v", err)
	}
	close(release)
	select {
	case outcome := <-queryDone:
		if outcome.err != nil || outcome.result != 42 {
			t.Fatalf("accepted query = %d, %v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted query did not finish during drain")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runtime run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish graceful drain")
	}
	if _, err := instance.Query(t.Context(), query, 23); !errors.Is(err, messenger.ErrRuntimeClosed) {
		t.Fatalf("query after drain = %v", err)
	}
}

func TestAsyncQueryCallerCancellationControlsExecutionAndDoesNotBlockDrain(t *testing.T) {
	query := messenger.MustQuery[int, int]("async.cancel", 1, messenger.JSON[int]())
	route, err := messenger.NewLocalAsyncRoute(
		"local.query.cancel", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
	)
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	started := make(chan struct{})
	handlerDone := make(chan error, 1)
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(query, "async.cancel", func(ctx context.Context, _ int) (int, error) {
		close(started)
		<-ctx.Done()
		handlerDone <- ctx.Err()
		return 0, ctx.Err()
	})
	builder.RouteQuery(query, route)
	instance, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(runContext) }()
	waitRuntimeReady(t, runtime)

	queryContext, cancelQuery := context.WithCancel(t.Context())
	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := instance.Query(queryContext, query, 1)
		queryDone <- queryErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancelQuery()
	select {
	case err := <-queryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("query cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not return after cancellation")
	}
	select {
	case err := <-handlerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not observe caller cancellation")
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not drain unread buffered result")
	}
}

func TestAsyncQueryBoundedAdmissionHonorsDeadline(t *testing.T) {
	query := messenger.MustQuery[int, int]("async.backpressure", 1, messenger.JSON[int]())
	route, err := messenger.NewLocalAsyncRoute(
		"local.query.backpressure", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
	)
	if err != nil {
		t.Fatalf("new route: %v", err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(query, "async.backpressure", func(ctx context.Context, value int) (int, error) {
		if value == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return value, nil
	})
	builder.RouteQuery(query, route)
	instance, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(t.Context()) }()
	waitRuntimeReady(t, runtime)

	firstDone := make(chan error, 1)
	go func() {
		_, queryErr := instance.Query(t.Context(), query, 1)
		firstDone <- queryErr
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first handler did not start")
	}
	secondDone := make(chan error, 1)
	go func() {
		result, queryErr := instance.Query(t.Context(), query, 2)
		if queryErr == nil && result != 2 {
			queryErr = errors.New("unexpected second result")
		}
		secondDone <- queryErr
	}()
	// Give the earlier submitter a scheduling window to occupy the one queued slot.
	time.Sleep(20 * time.Millisecond)
	blockedContext, cancelBlocked := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancelBlocked()
	if _, err := instance.Query(blockedContext, query, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded admission = %v", err)
	}
	close(releaseFirst)
	for name, result := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s query: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s query did not finish", name)
		}
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestAsyncQueryDrainBeforeRunAndForcedShutdown(t *testing.T) {
	t.Run("drain before run", func(t *testing.T) {
		query := messenger.MustQuery[int, int]("async.predrain", 1, messenger.JSON[int]())
		route, err := messenger.NewLocalAsyncRoute(
			"local.query.predrain", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
		)
		if err != nil {
			t.Fatalf("new route: %v", err)
		}
		builder := messenger.NewBuilder(messenger.WithSource(testSource))
		builder.HandleQueryFunc(query, "async.predrain", func(context.Context, int) (int, error) { return 1, nil })
		builder.RouteQuery(query, route)
		instance, _, err := builder.Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		route.BeginDrain()
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
			t.Fatalf("query while pre-draining = %v", err)
		}
		if err := route.Run(t.Context()); err != nil {
			t.Fatalf("run pre-drained route: %v", err)
		}
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrRuntimeClosed) {
			t.Fatalf("query after pre-drain = %v", err)
		}
	})

	t.Run("forced shutdown", func(t *testing.T) {
		query := messenger.MustQuery[int, int]("async.forced", 1, messenger.JSON[int]())
		route, err := messenger.NewLocalAsyncRoute(
			"local.query.forced", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1},
		)
		if err != nil {
			t.Fatalf("new route: %v", err)
		}
		started := make(chan struct{})
		builder := messenger.NewBuilder(messenger.WithSource(testSource))
		builder.HandleQueryFunc(query, "async.forced", func(ctx context.Context, _ int) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		})
		builder.RouteQuery(query, route)
		instance, runtime, err := builder.Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- runtime.Run(t.Context()) }()
		waitRuntimeReady(t, runtime)
		queryDone := make(chan error, 1)
		go func() {
			_, queryErr := instance.Query(t.Context(), query, 1)
			queryDone <- queryErr
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("forced-shutdown handler did not start")
		}
		shutdownContext, cancelShutdown := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancelShutdown()
		if err := runtime.Shutdown(shutdownContext); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("forced shutdown = %v", err)
		}
		select {
		case err := <-queryDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("forced query error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("forced query did not finish")
		}
		select {
		case err := <-runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("runtime did not stop after forced shutdown")
		}
	})
}
