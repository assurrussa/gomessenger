package messenger_test

import (
	"context"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func ExampleMessenger_Send() {
	type resizeMedia struct {
		JobID int64 `json:"jobId"`
	}

	resize := messenger.MustCommand("media.resize", 1, messenger.JSON[resizeMedia]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:media-resizer"))
	builder.HandleCommandFunc(resize, "media-worker", func(_ context.Context, payload resizeMedia) error {
		fmt.Println("handler", payload.JobID)
		return nil
	})
	builder.RouteCommand(resize, messenger.NewLocalSyncRoute())
	bus, _, err := builder.Build()
	if err != nil {
		panic(err)
	}
	resizeSender := messenger.BindSender(bus, resize)
	receipt, err := resizeSender.Send(context.Background(), resizeMedia{JobID: 42})
	if err != nil {
		panic(err)
	}
	fmt.Println(receipt.State)

	// Output:
	// handler 42
	// completed
}

func ExampleMessenger_Query() {
	type findArticle struct{ ID int64 }
	type articleView struct {
		ID    int64
		Title string
	}

	find := messenger.MustQuery[findArticle, articleView]("article.find", 1, messenger.JSON[findArticle]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:catalog"))
	builder.HandleQueryFunc(find, "article-reader", func(_ context.Context, query findArticle) (articleView, error) {
		return articleView{ID: query.ID, Title: "CQRS in Go"}, nil
	})
	builder.RouteQuery(find, messenger.NewLocalSyncRoute())
	bus, _, err := builder.Build()
	if err != nil {
		panic(err)
	}

	reader := messenger.BindQuerier(bus, find)
	article, err := reader.Query(context.Background(), findArticle{ID: 42})
	if err != nil {
		panic(err)
	}
	fmt.Println(article.ID, article.Title)

	// Output:
	// 42 CQRS in Go
}

func ExampleLocalAsyncRoute_payloadOwnership() {
	type increment struct{ Values []int }

	command := messenger.MustCommand("counter.increment", 1, messenger.JSON[increment]())
	route, err := messenger.NewLocalAsyncRoute("local.counter", messenger.LocalAsyncConfig{Capacity: 1, Workers: 1})
	if err != nil {
		panic(err)
	}
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:counter"))
	builder.HandleCommandFunc(command, "counter-worker", func(_ context.Context, payload increment) error {
		// The caller gives this handler exclusive access until drain completes.
		// A copied struct still shares the Values backing array.
		payload.Values[0]++
		return nil
	})
	builder.RouteCommand(command, route)
	bus, runtime, err := builder.Build()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(ctx) }()
	for runtime.Readiness(ctx) != nil {
		select {
		case err := <-runDone:
			panic(fmt.Sprintf("runtime stopped before readiness: %v", err))
		case <-ctx.Done():
			panic(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}

	payload := increment{Values: []int{42}}
	receipt, err := bus.Send(ctx, command, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(receipt.State)
	// Do not read or mutate payload.Values here: acceptance is not completion.
	// Neither caller cancellation nor BeginDrain alone releases shared storage.
	if err := runtime.Shutdown(ctx); err != nil {
		panic(err) // A shutdown timeout does not prove the handler has stopped.
	}
	if err := <-runDone; err != nil {
		panic(err)
	}

	// Graceful drain waited for the handler, which retained no references and
	// started no goroutines. The caller can now read and reuse the same slice.
	fmt.Println(payload.Values[0])
	payload.Values[0] = 100
	fmt.Println(payload.Values[0])

	// Output:
	// accepted
	// 43
	// 100
}
