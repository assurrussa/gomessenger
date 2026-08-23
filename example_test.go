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
	receipt, err := bus.Send(context.Background(), resize, resizeMedia{JobID: 42})
	if err != nil {
		panic(err)
	}
	fmt.Println(receipt.State)

	// Output:
	// handler 42
	// completed
}
