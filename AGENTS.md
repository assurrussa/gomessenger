# Repository Guidelines

## Project contract

`gomessenger` is a Go 1.27 multi-module messaging suite. The root module is the
typed public facade and may depend only on the standard library and
`github.com/assurrussa/gobus`. Broker, storage, telemetry, and CLI dependencies
belong in their nested modules.

The public contract contains commands, typed local queries, and events. Local
queries delegate to GoBus result dispatch and never enter `Delivery` or a wire
adapter. Any distributed query API requires the separate request/reply contract
in `docs/decisions/0003-distributed-queries.md`.

Supported modules:

- `github.com/assurrussa/gomessenger`
- `github.com/assurrussa/gomessenger/adapters/outbox`
- `github.com/assurrussa/gomessenger/adapters/kafka`
- `github.com/assurrussa/gomessenger/adapters/nats`
- `github.com/assurrussa/gomessenger/adapters/inbox`
- `github.com/assurrussa/gomessenger/observability`
- `github.com/assurrussa/gomessenger/tools/gomessengerctl`

The host owns database and NATS connections, Kafka broker/TLS/SASL input and
stable replica identity, process supervision, transaction boundaries,
configuration, and deployment. The Kafka adapter owns its franz-go clients and
mandatory transactional options. Keep reflection-derived wire names, driver
dependencies, and transport types out of the root facade.

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
- `make test-kafka` runs the opt-in local transactional pipeline against official
  Kafka 4.1.2 and 4.3.1 Docker images; it is not part of hosted CI.
- `GOMESSENGER_POSTGRES_DSN='postgres://...' make test-postgres` runs the
  PostgreSQL migration, conflict, rollback/retry, concurrency, and prune gate.
- `make bench-all` records allocation-aware local dispatch benchmarks.

Use a narrow package test while iterating. After a coherent change run one
matching aggregate gate and do not repeat nested checks on an unchanged tree.

## Coding and compatibility

Use explicit descriptors and stable handler/subscription IDs. Commands have one
logical handler; local queries require exactly one handler and local route;
events may have zero or more subscriptions. Delivery is
at-least-once outside the process, so ACK must happen only after a committed
handler/inbox transaction. Never claim exactly-once effects.

Run `gofmt` on touched Go files. Document exported identifiers. Add race,
fuzz, lifecycle, redelivery, and clean-consumer coverage when changing public or
concurrent behavior. Preserve immutable release tags and never publish, push,
or create a remote without explicit authorization.

Pin Outbox root and backend dependencies together at `v0.15.0`. Local Outbox
overrides belong only in `go.work`; clean consumer and E2E modules must resolve
the published tags with `GOWORK=off`.

Keep `implementation-notes.md` current during large implementation work.

## Code Review Rules

### Lifecycle startup and shutdown

- Flag readiness paths that can succeed before every resource and worker or pull
  loop required to accept work has started, or after drain begins. The safe path
  uses an explicit startup-ready condition and deterministic coverage of the
  startup window and startup failure.
- Flag recovered lifecycle panics or errors that are only logged while a service
  or peer can keep waiting. `BeginDrain` and `Shutdown` failures must propagate
  or force-cancel affected run contexts so bounded shutdown cannot be preceded
  by an unbounded wait.
- Changes to `Run`, `Readiness`, `Liveness`, `DeepHealth`, `BeginDrain`, or
  `Shutdown` must trace every state transition and add deterministic lifecycle
  coverage for new startup, failure, drain, and cancellation paths.

## Pull request and release review gate

When Codex Code Review is enabled for the connected GitHub repository, do not
merge a pull request or include it in a release until Codex has completed Code
Review for the pull request's current head commit, evidenced by a posted review
or the connector's no-findings reaction. If automatic review did not run,
request it with `@codex review` and wait for the in-progress reaction to resolve
into a completed result. A later push requires a review of the new head.

Address actionable findings and resolve their review conversations before
merge. A rejected finding needs a concise rationale in the conversation. Do not
create or publish release tags while an included pull request has a pending
Codex review or unresolved actionable finding. If Code Review is unavailable or
disabled, report that explicitly and use the normal human and repository gates;
do not imply that Codex reviewed the change.
