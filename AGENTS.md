# Repository Guidelines

## Project contract

`gomessenger` is a Go 1.27 multi-module messaging suite. The root module is the
typed public facade and may depend only on the standard library and
`github.com/assurrussa/gobus`. Broker, storage, telemetry, and CLI dependencies
belong in their nested modules.

The public contract contains commands and events only. Keep in-process queries
in GoBus; any distributed query API requires a separate request/reply contract
and ADR as defined in `docs/decisions/0001-query-boundary.md`.

Supported modules:

- `github.com/assurrussa/gomessenger`
- `github.com/assurrussa/gomessenger/adapters/outbox`
- `github.com/assurrussa/gomessenger/adapters/nats`
- `github.com/assurrussa/gomessenger/adapters/inbox`
- `github.com/assurrussa/gomessenger/observability`
- `github.com/assurrussa/gomessenger/tools/gomessengerctl`

The host owns database and NATS connections, process supervision, transaction
boundaries, configuration, and deployment. Keep reflection-derived wire names,
driver dependencies, and transport types out of the root facade.

Repository gates prove controlled contracts, not operational production
readiness. Do not describe GoMessenger as production-proven until the pilot in
`docs/decisions/0002-real-project-pilot.md` meets its exit criteria.

## Source order

Read `README.md`, `docs/contracts.md`, relevant `docs/decisions/*.md`,
`implementation-notes.md`, `go.work`, the affected module's `go.mod`, code,
migrations, and tests before editing.
Use `$project-context-router` for shared context. Do not put machine-local paths
in public documentation.

## Commands

- `make test` runs all module unit tests.
- `make test-race` runs race tests for every module.
- `make test-e2e` runs the isolated transactional
  Outbox-to-JetStream-to-Inbox pipeline under the race detector.
- `make check` is the maximal source-read-only gate: formatting check, build,
  vet, lint, tests, race, checkptr, coverage threshold, clean consumer probes,
  and the durable pipeline E2E.
- `make prepare` runs mutating formatting and `go mod tidy` for all modules.
- `make test-integration` reruns adapter and embedded-JetStream/SQLite E2E
  suites. Infrastructure-specific Outbox backend suites remain in that repo.
- `GOMESSENGER_POSTGRES_DSN='postgres://...' make test-postgres` runs the
  PostgreSQL migration, conflict, rollback/retry, concurrency, and prune gate.
- `make bench-all` records allocation-aware local dispatch benchmarks.

Use a narrow package test while iterating. After a coherent change run one
matching aggregate gate and do not repeat nested checks on an unchanged tree.

## Coding and compatibility

Use explicit descriptors and stable handler/subscription IDs. Commands have one
logical handler; events may have zero or more subscriptions. Delivery is
at-least-once outside the process, so ACK must happen only after a committed
handler/inbox transaction. Never claim exactly-once effects.

Run `gofmt` on touched Go files. Document exported identifiers. Add race,
fuzz, lifecycle, redelivery, and clean-consumer coverage when changing public or
concurrent behavior. Preserve immutable release tags and never publish, push,
or create a remote without explicit authorization.

Pin Outbox root and backend dependencies together at `v0.11.0`. Local Outbox
overrides belong only in `go.work`; clean consumer and E2E modules must resolve
the published tags with `GOWORK=off`.

Keep `implementation-notes.md` current during large implementation work.
