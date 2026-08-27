package demo

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	pgsql "github.com/assurrussa/outbox/backends/pgsql"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsfailedrepo"
	"github.com/assurrussa/outbox/backends/pgsql/repositories/jobsrepo"
	"github.com/assurrussa/outbox/backends/pgsql/storage/pgsqlclient"
	"github.com/assurrussa/outbox/backends/pgsql/storage/pgsqlinit"
	pgsqltx "github.com/assurrussa/outbox/backends/pgsql/storage/transaction"
	coreoutbox "github.com/assurrussa/outbox/outbox"
	"github.com/assurrussa/outbox/outbox/logger"
)

const (
	producerApplicationName = "gomessenger-outbox-producer"
	relayApplicationName    = "gomessenger-outbox-relay"
)

// splitOutboxRuntime deliberately gives producer staging and relay execution
// different pools. Repositories remain bound to the relay client, but honor a
// pgx transaction carried in context by the producer transactor.
type splitOutboxRuntime struct {
	producerClient     *pgsqlclient.Client
	relayClient        *pgsqlclient.Client
	producerTransactor *pgsqltx.Manager
	service            *coreoutbox.Service

	closeRelayOnce    sync.Once
	closeProducerOnce sync.Once
	closeRelayErr     error
	closeProducerErr  error
}

func openSplitOutboxRuntime(ctx context.Context, config Config) (*splitOutboxRuntime, error) {
	producerDSN, err := withApplicationName(config.PostgresDSN, producerApplicationName)
	if err != nil {
		return nil, fmt.Errorf("build Outbox producer DSN: %w", err)
	}
	relayDSN, err := withApplicationName(config.PostgresDSN, relayApplicationName)
	if err != nil {
		return nil, fmt.Errorf("build Outbox relay DSN: %w", err)
	}

	producerClient, err := pgsqlinit.Create(
		ctx,
		producerDSN,
		pgsqlclient.WithMinConnectionsCount(1),
		// normalizeConfig caps the host setting at 1,024 before this constructor runs.
		//nolint:gosec // The validated value cannot overflow int32.
		pgsqlclient.WithMaxConnectionsCount(int32(config.OutboxProducerMaxConns)),
		pgsqlclient.WithLogger(logger.Discard()),
	)
	if err != nil {
		return nil, fmt.Errorf("open Outbox producer pool: %w", err)
	}
	closeProducer := func() { _ = producerClient.Close() }

	relayClient, err := pgsqlinit.Create(
		ctx,
		relayDSN,
		pgsqlclient.WithMinConnectionsCount(1),
		// normalizeConfig caps the host setting at 1,024 before this constructor runs.
		//nolint:gosec // The validated value cannot overflow int32.
		pgsqlclient.WithMaxConnectionsCount(int32(config.OutboxRelayMaxConns)),
		pgsqlclient.WithLogger(logger.Discard()),
	)
	if err != nil {
		closeProducer()
		return nil, fmt.Errorf("open Outbox relay pool: %w", err)
	}
	closeRelay := func() { _ = relayClient.Close() }

	jobs, err := jobsrepo.New(jobsrepo.NewOptions(relayClient))
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox jobs repository: %w", err)
	}
	failed, err := jobsfailedrepo.New(jobsfailedrepo.NewOptions(relayClient))
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox failed jobs repository: %w", err)
	}
	relayTransactor := pgsqltx.New(relayClient.DB())
	service, err := coreoutbox.New(
		coreoutbox.WithWorkers(config.OutboxWorkers),
		coreoutbox.WithIdleTime(100*time.Millisecond),
		coreoutbox.WithReserveFor(5*time.Second),
		coreoutbox.WithJobsRepo(jobs),
		coreoutbox.WithCapabilityJobsRepo(jobs),
		coreoutbox.WithFanoutJobsRepo(jobs),
		coreoutbox.WithJobsStatRepo(jobs),
		coreoutbox.WithJobsFailedRepo(failed),
		coreoutbox.WithCapabilityJobsFailedRepo(failed),
		coreoutbox.WithTransactor(relayTransactor),
		coreoutbox.WithLogger(logger.Discard()),
	)
	if err != nil {
		closeRelay()
		closeProducer()
		return nil, fmt.Errorf("create Outbox service: %w", err)
	}

	return &splitOutboxRuntime{
		producerClient:     producerClient,
		relayClient:        relayClient,
		producerTransactor: pgsqltx.New(producerClient.DB()),
		service:            service,
	}, nil
}

func (r *splitOutboxRuntime) Run(ctx context.Context) error { return r.service.Run(ctx) }

func (r *splitOutboxRuntime) Readiness(ctx context.Context) error {
	if r == nil || r.producerClient == nil || r.relayClient == nil || r.service == nil {
		return errors.New("split Outbox runtime is not initialized")
	}
	if err := r.producerClient.DB().Ping(ctx); err != nil {
		return fmt.Errorf("producer pool: %w", err)
	}
	if err := r.relayClient.DB().Ping(ctx); err != nil {
		return fmt.Errorf("relay pool: %w", err)
	}
	return r.service.Readiness(ctx)
}

func (r *splitOutboxRuntime) BeginDrain() {
	if r != nil && r.service != nil {
		r.service.BeginDrain()
	}
}

func (r *splitOutboxRuntime) Service() *coreoutbox.Service { return r.service }

func (r *splitOutboxRuntime) ProducerClient() pgsql.Client { return r.producerClient }

func (r *splitOutboxRuntime) RelayClient() pgsql.Client { return r.relayClient }

func (r *splitOutboxRuntime) ProducerTransactor() *pgsqltx.Manager { return r.producerTransactor }

func (r *splitOutboxRuntime) CloseRelay() error {
	if r == nil || r.relayClient == nil {
		return nil
	}
	r.closeRelayOnce.Do(func() { r.closeRelayErr = r.relayClient.Close() })
	return r.closeRelayErr
}

func (r *splitOutboxRuntime) CloseProducer() error {
	if r == nil || r.producerClient == nil {
		return nil
	}
	r.closeProducerOnce.Do(func() { r.closeProducerErr = r.producerClient.Close() })
	return r.closeProducerErr
}

func withApplicationName(dsn, applicationName string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("PostgreSQL DSN must include scheme and host")
	}
	query := parsed.Query()
	query.Set("application_name", applicationName)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
