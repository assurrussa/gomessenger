package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"example.com/gomessenger-durable-postgres-nats/internal/capacity"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config, err := capacity.FromEnvironment()
	if err != nil {
		log.Error("invalid capacity configuration", "error", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := capacity.Run(ctx, config, log)
	if err != nil {
		log.Error("capacity run failed", "error", err, "results", config.ResultDir())
		return 1
	}
	log.Info("capacity run complete",
		"result", report.CapacityStatement,
		"integrity_passed", report.IntegrityPassed,
		"results", config.ResultDir(),
	)
	return 0
}
