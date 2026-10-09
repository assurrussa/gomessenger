package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"example.com/gomessenger-durable-postgres-nats/internal/atomicflow"
)

func main() { os.Exit(realMain()) }

func realMain() int {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	dsn := os.Getenv("GOMESSENGER_POSTGRES_DSN")
	if dsn == "" {
		log.Error("GOMESSENGER_POSTGRES_DSN is required; use a disposable PostgreSQL database")
		return 2
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		// Do not echo connection credentials through a parser error.
		log.Error("invalid PostgreSQL connection configuration")
		return 2
	}
	// The host selects one schema for Inbox, business, and official Outbox
	// tables. The stager neither changes search_path nor opens another pool.
	config.RuntimeParams["search_path"] = "gomessenger_atomic_example"
	db := stdlib.OpenDB(*config)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Error("close PostgreSQL", "error", closeErr)
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS gomessenger_atomic_example"); err != nil {
		log.Error("create example schema", "error", err)
		return 1
	}
	if err := atomicflow.Migrate(ctx, db); err != nil {
		log.Error("migrate example", "error", err)
		return 1
	}
	report, err := atomicflow.Run(ctx, db)
	if err != nil {
		log.Error("atomic Inbox/business/Outbox proof failed", "error", err)
		return 1
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		log.Error("write proof report", "error", err)
		return 1
	}
	log.Info("atomic Inbox/business/Outbox proof passed")
	return 0
}
