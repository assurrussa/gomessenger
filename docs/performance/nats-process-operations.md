# Bounded NATS process operations screen

Status: source candidate; no execution result is recorded by this document.

This opt-in test closes a narrow gap: independently running GoMessenger consumer
processes sharing one durable, a rolling replacement with the same global ACK
window, and a killed/restarted file-backed broker process retaining its store.
It uses the repository's existing NATS server dependency and SQLite Inbox. It
does not measure the PostgreSQL/Outbox host path, a multi-node broker cluster,
production readiness, a power-loss crash, or the maximum sustainable frontier.

## Existing contracts reused

- The current `testdata/e2e` module and its already declared dependencies and
  checkout replacements. No host module, public API, dependency, CI trigger,
  deployment setting, or production retention policy is changed.
- Existing `processPayload`, NATS route/consumer APIs, Inbox SQL transaction
  boundary, `connectNATS`, and SQLite migrations.
- The current test binary is re-executed for broker and consumer roles. The
  broker is linked from the existing `nats-server/v2` dependency (currently
  2.14.5), not the host's separately tested NATS 2.12.3 image. The actual server
  version and process IDs are retained in the report.

Two consumer processes prove independent replication and replacement while
another consumer remains available. No one-process baseline or four-process
comparison is needed for this operational contract. Every child uses
`GOMAXPROCS=1`; normal-load consumers each use concurrency 1 and the same explicit
`MaxAckPending=32`. There are no fixture-enforced CPU/memory quotas. This is not
a scalability comparison.

SQLite uses one connection per process, WAL, `synchronous=FULL`, and a five-second
busy timeout. The handler stores the message identity, job identity, worker PID,
and execution count in the Inbox transaction. A repeated handler execution
increments the count, so a unique constraint cannot silently hide a duplicate
business effect. Final reconciliation checks every offered-and-admitted message
against its durable effect and completed Inbox identity. Every stage rechecks
the complete campaign's compact identity/job/PID set, so mutation of an older
row cannot escape the sequence-based timing poller. Both consumer PIDs must
commit confirmed work in the two-process stage; the replacement must do so too.

The existing E2E fixture runs all consumers in one process, while the demo's
capacity helpers belong to a separate HTTP/PostgreSQL/k6 module. Reusing those
or adding a service would expand this test's boundary. The specialized test-only
code supplies only child supervision, bounded generation, reconciliation and
evidence retention; no reusable service framework is introduced.

## Bounded operational acceptance

Use exactly two consumer processes and two 120-second phases:

1. A steady phase at 125 messages/s must satisfy every steady acceptance check.
2. Only after that passes, a rolling/restart phase runs at 62 messages/s to
   preserve recovery headroom.

These are conservative synthetic test settings, selected from the earlier
bounded rate policy; that selection is not measured capacity evidence. This
screen does not search for a maximum rate, run a long soak, or compare replica
scalability. A reconciled but unsustainable steady phase is an explicit failure;
it never proceeds to faults or silently retries at a lower rate. A separate
capacity measurement, including highest-first halving/refinement, must not be
claimed from this operational result.

Unconfirmed publication, invalid telemetry, missing/extra/duplicate effects,
unexpected DLQ, or failed bounded recovery stops the campaign. These are not
ordinary saturation to hide by lowering the rate. All stages and their message
ledgers are retained, including failures.

The generator has sixteen bounded publishers and a 64-item admission queue.
Scheduled arrivals may catch up at most 100 ms of scheduler jitter; later or
unadmitted arrivals are counted as dropped. Parent-held offered, PubAck, and
SQL-visibility timestamps avoid the demo's process-local observation-map gap.
Observed-commit latency is an upper bound, including SQL polling delay; PubAck
latency includes parent queueing. Neither is a pure handler or broker RTT.

An ordinary level is sustainable only with no generator drops, at least 99% of
target confirmed and observed committed inside the 120-second load window,
observed-commit p95 at most two seconds, second-half lag growth at most 1% of the
target rate, a drain within 60 seconds, and no observed delivery errors or
redeliveries. A longer drain can reconcile for at most three minutes, but never
turns that sample into a sustainable one. Drain is outside throughput denominators.

After the steady phase passes, the 120-second fault phase starts. At 30 seconds,
one consumer drains and is replaced by a new PID using local concurrency 2 but
the same durable and global window. At 60 seconds, the broker process is killed
and restarted at the same address using the same store. A previously confirmed
stream record from the fully reconciled preceding stage is snapshotted before
fault-stage publishing starts and must retain its exact sequence and bytes.
The entire campaign's admitted identities must still reconcile, and at least
one newly offered identity after restart must be confirmed and committed.
Publication retries in this phase reuse the
same canonical envelope/message ID. Fault latency and expected redeliveries are
not evaluated as ordinary capacity samples. Generator drops remain recorded;
this phase establishes recovery of admitted work, not uninterrupted admission.

The run schedules at most 22,440 offers, with at most 15,000 in one phase:
four minutes of load plus at most six minutes of bounded recovery. A shared
15-minute context leaves five minutes beyond that ten-minute sum for setup and
checks. Parent and child emergency test timeouts are 20 minutes. File encoding,
fsync and cleanup are not hard-bounded by the cooperative context, so execution
must also use an outer process-group watchdog and retain an owned-child cleanup
route. Startup, evidence writing and drain add wall time to the four-minute load.

## Commands

From the repository root, compile/check the small helpers before authorizing load:

```sh
cd testdata/e2e
GOWORK=off go test -race -count=1 -timeout 5m -run '^TestNATSProcess(FixedAcceptance|ChildEnvironment|EffectIdentity|SaturationCriteria|P95|LoadSettled|CampaignReconciliation|WorkerProgress|ReportReservation|ExitedChild|ReportPaths|LedgerRoot)' ./...
GOWORK=off golangci-lint run --timeout=5m --config ../../.golangci.yml ./...
```

Keep both the operations opt-in and private helper environment variables unset
during this first gate. It compiles all test sources and exercises only the new
small regressions. Resolve failures and review changed parts before continuing.
After this gate passes, run the matching aggregate `make test-e2e` from the root.
For a finalized, reviewed candidate, that aggregate may directly serve as the
compile/helper gate instead of repeating focused helpers first. Keep
`GOMESSENGER_KAFKA_BROKERS` unset as well, so unrelated Kafka tests stay opt-in.
The repository's publication gate remains required; these focused checks do not
waive it. Coordinate the measured campaign separately, only after these checks
pass and the shared machine is reserved for load.

After review and execution coordination, use a unique report path:

```sh
cd testdata/e2e
mkdir -p ../../tmp
GOMESSENGER_NATS_OPERATIONS=1 \
GOMESSENGER_NATS_OPERATIONS_REPORT="$PWD/../../tmp/nats-process-$(date -u +%Y%m%dT%H%M%SZ).json" \
GOWORK=off go test -count=1 -timeout 20m -run '^TestNATSProcessOperations$' ./...
```

Do not add `-race` to the measured command and compare those timings with a
non-race run. The helper command does use race and includes a tiny local SQLite
regression for mutation of an earlier stage's row. Normal gates compile the
fixture and skip the opt-in campaign. No automatic workflow is added.

Prerequisites are Go 1.27, writable build/temp/evidence directories, loopback
TCP, and permission to execute the already-built test binary as child processes.
Docker, a new service, a broker CLI, and a new package dependency are unnecessary.
Reserve the shared machine for this load; run other heavy checks sequentially.
Record actual CPU/memory limits and free space on both temporary and evidence
filesystems before load. The bounded message count is not a hard RAM/disk quota:
SQLite, retained JetStream records, identity maps and evidence accumulate until
cleanup. Resource exhaustion is a failed/incomplete run, never acceptance.

Save the exact Git revision, dirty diff, module selection and toolchain alongside
the JSON report before making a source-level claim. The report additionally
retains build information and the executed test binary's SHA-256. Per-stage
`.jsonl` ledgers are created exclusively under `<report>.records`. The report
path must be absolute beneath the checkout's existing `tmp` directory; this
test-only boundary is intentional. Report and ledger operations use opened
`os.Root` handles, rejecting traversal and symlink escapes. The report and its
ledger directory are reserved exclusively before load. An existing report,
ledger directory or ledger symlink is refused rather than overwriting prior
evidence. The ledger directory handle stays open throughout the campaign so
renaming or replacing its pathname cannot redirect writes. Ledger paths in the
JSON are relative to the checkout root. Temporary
broker/database state and owned child processes are cleaned up after the test.

## Existing host pilot is a separate evidence class

The real article-publication host already exists in `assurrussa/sportsite`
(formerly `site`). Its committed historical report records a 901.390-second,
4,500-row local soak and the real local admin publication flow:

- [Historical pilot report](https://github.com/assurrussa/sportsite/blob/3e59b2c6cf9a531092f369598ab6913709ef7325/docs/tasks/gomessenger-site-pilot.md)
- [GoMessenger v0.3.0 / Outbox v0.15.0 E2E acceptance](https://github.com/assurrussa/sportsite/blob/3e59b2c6cf9a531092f369598ab6913709ef7325/docs/tasks/2026-09-06-gomessenger-v030.md)
- [Current reusable host commands](https://github.com/assurrussa/sportsite/blob/dfcf3f30e3c2e76593ed89ba0002a176ed117945/Taskfile.yml#L197-L242)

Preserve those historical passes. They are not current-ref raw logs or production
acceptance. Current dependency adoption belongs to the shared consumer-update
package, and this process screen does not rerun the 15-minute host soak merely
to refresh its timestamp.
