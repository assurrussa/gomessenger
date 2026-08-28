# Release process

GoMessenger is a multi-module repository. Tags are immutable and every optional module must be published before the
clean release consumer can resolve the complete facade.

## `v0.1.0` scope

The first release includes typed commands, typed local `Query[Q,R]` request/reply, typed events, local sync/async GoBus
routes, canonical command/event envelopes, Outbox/JetStream/Kafka/Inbox durability, middleware, lifecycle,
observability, topology, and DLQ tooling. The query request codec is local descriptor identity; result `R` is not
serialized.

Distributed query transports are explicitly not part of `v0.1.0`. HTTP, gRPC, and core NATS request/reply remain
future candidates under ADR-0003; JetStream, Outbox, Inbox, receipts, retry, and DLQ must not be presented as remote
query support. The `site` article-publication audit pilot starts only after all dependency-ordered `v0.1.0` module tags
pass the published clean-consumer gate.

## Pre-release gate

1. Identify every feature pull request included since the previous release. If
   Codex Code Review is enabled for the repository, verify a completed result
   on each pull request's final head commit: a posted review or the connector's
   no-findings reaction. Request a missing review with `@codex review`, wait for
   the completed result, and resolve every actionable conversation. A new push
   invalidates the earlier review gate for that pull request.
2. Run `make prepare` and inspect generated module sums and formatting changes.
3. Run `make check`; it includes the Docker-free transactional
   Outbox-to-JetStream-to-Inbox E2E under the race detector.
4. Run `GOMESSENGER_POSTGRES_DSN='postgres://...' make test-postgres`. CI runs the same target against PostgreSQL 18.
   `make test-integration` separately reruns embedded JetStream/SQLite adapters and the durable pipeline.
5. Run `make test-kafka` locally. It runs the transactional pipeline against official Kafka 4.1.2 and 4.3.1 images;
   independent hosted matrix jobs run the same target for both versions.
6. Run `make bench-all` for dispatch, envelope, registry, or admission-path changes. The benchmark workflow records ten
   base/head samples, uploads raw data, and adds a pinned `benchstat` report without enforcing a machine-dependent
   performance threshold.
7. Confirm `git diff --check` and that public docs contain no machine-local paths or development-only `replace`
   examples.
8. Update `CHANGELOG.md` and replace `Unreleased` wording only when the release contents are fixed.
9. On the release pull request's final head commit, wait for the enabled Codex
   Code Review result and resolve every actionable conversation before merge or
   tagging. Request a missing review with `@codex review`; a later push requires
   a new completed result.

Passing local gates proves the checkout, not the published module graph.

## Prepare exact module requirements

The GoMessenger outbox adapter depends on the unified outbox v0.12 contract. Outbox root and backend `v0.12.0` tags are
already published. Prepare all GoMessenger module requirements in one reviewable commit:

```sh
make check
make release-ready VERSION=vX.Y.Z OUTBOX_VERSION=v0.12.0
make release-readiness VERSION=vX.Y.Z OUTBOX_VERSION=v0.12.0
```

`release-ready` replaces development `v0.0.0` requirements with exact versions, removes development path replacements
from nested `go.mod` files, adds matching local pre-tag replacements to `go.work`, and uses a temporary local root
replacement only while tidying the Outbox adapter before its requested GoMessenger tag exists. It then tidies the
durable example, clean consumer, and SQLite E2E modules with `GOWORK=off`, and formats source. Run it only after the
selected Outbox root and backend tags resolve from the Go proxy.
`release-readiness` verifies every expected GoMessenger requirement in published modules and the clean consumer, plus
the Outbox root/SQLite pair used by clean consumer/E2E modules and the Outbox root/PostgreSQL pair used by the durable
example. It rejects remaining `replace` directives in published module files and any Outbox replacement in these
consumer/example modules. The unpublished local
E2E module deliberately keeps GoMessenger path replacements to test the checkout itself; it is not a published-module
resolution probe. The gate does not resolve or test the not-yet-published GoMessenger tags. The full source gate must
pass before release preparation; published resolution is proved separately after the dependency-ordered tags exist.
Published modules in the checkout use development replacements only before release preparation; no published module
may depend on them.

Commit the exact-version module files before creating any GoMessenger tag.

## Dependency-ordered tags

For release `vX.Y.Z`, create and push tags in dependency order:

```text
vX.Y.Z
adapters/inbox/vX.Y.Z
adapters/outbox/vX.Y.Z
observability/vX.Y.Z
adapters/nats/vX.Y.Z
adapters/kafka/vX.Y.Z
tools/gomessengerctl/vX.Y.Z
```

The root must resolve before any nested module can resolve its root requirement. `adapters/inbox`, `adapters/outbox`,
and
`observability` depend only on the root and their external dependencies, so prepare and verify each after the root tag
is
published. `adapters/nats` and `adapters/kafka` additionally depend on the published inbox tag and may be tagged in
either order after it. The CLI additionally depends on the published NATS, Kafka, and inbox tags. For each nested
module, run the following after its dependencies resolve, inspect and commit any `go.sum` update, and only then create
its tag:

```sh
cd MODULE_DIRECTORY
GOWORK=off go mod tidy
GOWORK=off go test ./...
git diff --check
```

Never retarget an existing tag. A mistake after publication requires a new patch version.

## Published verification

After all tags are visible through the Go module proxy, run:

```sh
make test-consumer-release VERSION=vX.Y.Z
```

The script accepts only an exact stable `vX.Y.Z` tag, creates a clean temporary module, downloads root and nested modules
by that tag, compiles the command/query/event facade and adapters, installs the published `gomessengerctl` module, and
uses no local replacement. Record this separately from the local gate in the release notes.

## Post-publication adoption surface

Only after the published clean-consumer probe succeeds:

1. replace the README pre-release notice with exact `go get MODULE@vX.Y.Z` commands for the local-only, NATS durable,
   and Kafka durable scenarios;
2. add and verify the latest-release and pkg.go.dev badges; add Go Report Card and coverage badges only when their
   linked reports exist and describe the released repository state;
3. retain the public description `Typed durable messaging for Go: commands, local queries, events, Outbox/Inbox, NATS
   JetStream, Kafka, retries, and DLQ.` and keep topics focused on implemented messaging contracts;
4. identify public material as `GoMessenger — typed durable messaging for Go` or `assurrussa/gomessenger` so the name
   remains searchable without implying a universal messenger;
5. publish release notes and technical launch material only with an explicit separation between source gates,
   published-module verification, the still-pending real-service pilot, and any later deployment/manual smoke.

Do not add `exactly-once`, `event-sourcing`, `saga`, or `workflow-engine` topics: those are not current product claims.

## Operational rollout

Published packages are not a deployed service. For an infrastructure migration:

1. apply additive database migrations;
2. apply reviewed compatible topology changes;
3. deploy relay and consumer registrations;
4. verify readiness, DLQ publishing, lag, and telemetry;
5. enable producers;
6. run a real end-to-end message and controlled redelivery smoke;
7. drain old paths only after both correctness and backlog are verified.

State explicitly which of publication, deployment, and manual production smoke has actually occurred.

The committed GitHub Actions workflows prove a clean CI checkout only after a repository remote exists and the branch is
pushed. Their presence is not evidence that CI has run for a local-only repository.
