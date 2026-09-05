# Release process

GoMessenger is a multi-module repository. Tags are immutable and every optional module must be published before the
clean release consumer can resolve the complete facade.

## Release scope

The current preparation targets [v0.3.0](releases/v0.3.0.md): supported producer/relay/consumer batches, quarantine v2,
terminal Inbox protection and execution-time expiry. Keep the published README install commands on v0.2.2 until the
new release passes published verification.

Distributed query transports remain outside the public contract. HTTP, gRPC, and core NATS request/reply remain future
candidates under ADR-0003. The `site` article-publication audit pilot requires all used modules to resolve at the same
published release version and remains a separate service integration task.

## Pre-release gate

1. Identify every feature pull request included since the previous release. If
   Codex Code Review is enabled for the repository, verify a completed result
   on each pull request's final head commit: a posted review or the connector's
   no-findings reaction. Request a missing review with `@codex review`, wait for
   the completed result, and resolve every actionable conversation. A new push
   invalidates the earlier review gate for that pull request.
2. Run the applicable `release-ready` layer below and inspect module sums and formatting changes.
3. Run `make check-workspace`; it includes the Docker-free transactional
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

`make check-workspace` proves the local GoMessenger workspace graph. The current `go.work` does not override Outbox;
its root and backend dependencies resolve at v0.15.0. `make check` uses `GOWORK=off`, but development replacements in
individual `go.mod` files still apply. Before final release preparation, either source gate can pass without proving
that an external consumer can resolve the new GoMessenger APIs. The final `release-readiness` and published-consumer
gates close that boundary.

## Prepare exact module requirements

The GoMessenger outbox adapter depends on the unified outbox v0.15 contract. Outbox root and backend `v0.15.0` tags are
already published. The GoMessenger graph cannot be prepared in one pre-tag commit: a clean `GOWORK=off` build must be
able to resolve every exact dependency, so the root and each dependency layer must be published before the next layer
is pinned.

Features that consume a newer Outbox public contract must first publish and
verify a compatible Outbox core/backend version, then pass that exact version
as `OUTBOX_VERSION`. Outbox path overrides remain in `go.work` only and are
never a substitute for this release boundary.

`RELEASE_LAYER` selects which module files may change. Its default is `final`, preserving the existing command.
Every preparation checks the selected layer's published prerequisites before editing `go.mod` or `go.work`, tidies
each affected module with `GOWORK=off`, and checks that layer's requirements. A failed prerequisite check changes no
source files. Preparation never creates or pushes tags.

Before the root tag, keep nested modules on their current development graph and run:

```sh
make release-ready VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=root
make release-readiness VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=root
make check-workspace
```

The root layer verifies the published Outbox root/backend prerequisites, checks that the facade has no replacement,
and tidies only the root module. It does not pin nested modules to an unavailable GoMessenger tag.

After the reviewed root tag resolves through the Go proxy, prepare the root-dependent modules:

```sh
make release-ready VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=modules
make release-readiness VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=modules
make check
```

This layer updates and removes development replacements in `adapters/inbox`, `adapters/outbox`, and `observability`.
Review and commit the layer, then publish those three tags. After the Inbox tag resolves, prepare the transports:

```sh
make release-ready VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=transports
make release-readiness VERSION=v0.3.0 OUTBOX_VERSION=v0.15.0 RELEASE_LAYER=transports
make check
```

This layer updates `adapters/nats` and `adapters/kafka` while leaving the CLI and fixtures unchanged. Review and commit
the layer, then publish both transport tags. After all six root/adapter/observability tags resolve, finalize the graph:

```sh
make release-ready VERSION=vX.Y.Z OUTBOX_VERSION=v0.15.0
make release-readiness VERSION=vX.Y.Z OUTBOX_VERSION=v0.15.0
make check
```

The final layer aligns every GoMessenger requirement, including fixture requirements previously set to `v0.0.0`.
It removes development replacements from all published modules, the external consumer fixture and durable example,
and adds matching local replacements to `go.work`. It requires root, Inbox, Outbox adapter, observability, NATS and
Kafka tags plus the selected Outbox root/backend tags to resolve first. Commit and review the final layer before the
CLI tag. A partial layer's successful readiness result is not final release readiness.

Final `release-readiness` verifies every expected GoMessenger requirement in published modules and all fixtures, plus
the Outbox root in the consumer, the root/SQLite pair in E2E, and the root/PostgreSQL pair in the durable example.
It rejects every `replace` directive in published modules, the consumer and the example, plus Outbox
replacements in the E2E fixture. The unpublished local
E2E module deliberately keeps GoMessenger path replacements to test the checkout itself; it is not a published-module
resolution probe. The gate verifies the committed version graph but does not replace clean published resolution. The
full source gate must pass for every reviewed dependency-layer commit; published resolution is proved separately after
all dependency-ordered tags exist.
Published modules in the checkout use development replacements only before release preparation; no published module
may depend on them.

Commit each exact-version dependency layer before creating tags for that layer. Never pin a module to a GoMessenger
tag that is not yet resolvable through the configured Go proxy.

## Dependency-ordered tags

For release `vX.Y.Z`, create reviewed commits and push tags in dependency order:

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
and `observability` depend only on the root and their external dependencies, so prepare and verify them after the root
tag is published. `adapters/nats` and `adapters/kafka` additionally depend on the published Inbox tag and may be tagged
in either order after it. The CLI additionally depends on the published NATS, Kafka, and Inbox tags. For each nested
module, run the following after its dependencies resolve, inspect and commit any `go.sum` update in a reviewed layer,
and only then create its tag:

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

The script forces `GOWORK=off`, accepts only an exact stable `vX.Y.Z` tag, creates a clean temporary module and downloads
root and nested modules by that tag. It compiles the command/query/event facade and adapters, installs the published `gomessengerctl` module, and
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
