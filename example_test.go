package messenger_test

import (
	"context"
	"fmt"

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
