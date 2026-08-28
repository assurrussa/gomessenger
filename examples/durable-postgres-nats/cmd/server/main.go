package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config := demo.CapacityConfig(log)
	var err error
	config.OutboxWorkers, err = envInt("OUTBOX_WORKERS", config.OutboxWorkers)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}
	config.OutboxReservationBatchSize, err = envInt(
		"OUTBOX_RESERVATION_BATCH_SIZE", config.OutboxReservationBatchSize,
	)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}
	config.OutboxProducerMaxConns, err = envInt(
		"OUTBOX_PRODUCER_MAX_CONNS", config.OutboxProducerMaxConns,
	)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}
	config.OutboxRelayMaxConns, err = envInt("OUTBOX_RELAY_MAX_CONNS", config.OutboxRelayMaxConns)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}
	config.ConsumerConcurrency, err = envInt("NATS_CONSUMER_CONCURRENCY", config.ConsumerConcurrency)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}
	config.DBMaxOpenConns, err = envInt("DB_MAX_OPEN_CONNS", config.DBMaxOpenConns)
	if err != nil {
		log.Error("invalid capacity service configuration", "error", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startupCtx, startupCancel := context.WithTimeout(ctx, 60*time.Second)
	application, err := demo.Open(startupCtx, config)
	startupCancel()
	if err != nil {
		log.Error("start capacity application", "error", err)
		return 1
	}
	serveErr := demo.RunHTTPServer(ctx, demo.EnvOr("HTTP_ADDR", ":8080"), application, log)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	shutdownErr := application.Close(shutdownCtx)
	shutdownCancel()
	if serveErr != nil {
		log.Error("capacity HTTP service failed", "error", serveErr)
	}
	if shutdownErr != nil {
		log.Error("close capacity application", "error", shutdownErr)
	}
	return serviceExitCode(serveErr, shutdownErr)
}

func serviceExitCode(serveErr, shutdownErr error) int {
	if serveErr != nil || shutdownErr != nil {
		return 1
	}
	return 0
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, value)
	}
	return parsed, nil
}
