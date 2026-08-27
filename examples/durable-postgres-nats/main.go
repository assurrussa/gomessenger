package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := demo.RunCorrectness(ctx, log); err != nil {
		log.Error("durable demo failed", "error", err)
		return 1
	}
	log.Info("durable demo passed")
	return 0
}
