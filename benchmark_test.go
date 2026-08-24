package messenger_test

import (
	"context"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

func BenchmarkMessengerLocalCommand(b *testing.B) {
	command := messenger.MustCommand("benchmark.command", 1, messenger.JSON[int]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:benchmark"))
	builder.HandleCommand(command, "handler", func(context.Context, messenger.Message[int]) error { return nil })
	builder.RouteCommand(command, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		b.Fatalf("build: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := m.Send(context.Background(), command, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessengerLocalQuery(b *testing.B) {
	query := messenger.MustQuery[int, int]("benchmark.query", 1, messenger.JSON[int]())
	builder := messenger.NewBuilder(messenger.WithSource("urn:benchmark"))
	builder.HandleQueryFunc(query, "handler", func(_ context.Context, value int) (int, error) { return value + 1, nil })
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	m, _, err := builder.Build()
	if err != nil {
		b.Fatalf("build: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if result, err := m.Query(context.Background(), query, 1); err != nil || result != 2 {
			b.Fatalf("query = %d, %v", result, err)
		}
	}
}
