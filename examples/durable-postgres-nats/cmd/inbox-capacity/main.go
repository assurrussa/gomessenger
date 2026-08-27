package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"example.com/gomessenger-durable-postgres-nats/internal/inboxcapacity"
)

func main() { os.Exit(realMain()) }

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config, err := inboxcapacity.FromEnvironment()
	if err != nil {
		log.Error("invalid PostgreSQL Inbox capacity configuration", "error", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := inboxcapacity.Run(ctx, config, log)
	if err != nil {
		log.Error("PostgreSQL Inbox capacity run failed", "error", err, "results", config.ResultDir())
		return 1
	}
	log.Info("PostgreSQL Inbox capacity run complete",
		"cases", len(report.Cases), "integrity_passed", report.IntegrityPassed, "results", config.ResultDir())
	return 0
}
