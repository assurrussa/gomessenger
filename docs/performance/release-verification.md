# Release capacity verification

To verify the candidate at 1,500 msg/s, run three independent confirmations for each advertised payload profile on
the exact clean release-candidate checkout. The September 5 `SCREEN_ONLY` result is not a substitute for this series.
This check establishes the tested rate and latency floor; it does not measure maximum throughput or a batching speedup.

## Preconditions

- Finish the source changes, commit the candidate and record its commit. Before publishing release-wide claims,
  prepare the complete module graph and pass `release-readiness` and the published-consumer probe. A pre-tag run must
  remain labelled with its exact candidate commit and checkout-local scope.
- Use clean GoMessenger and adjacent `outbox` checkouts. Preserve any existing local changes by using an isolated clean
  pair instead of resetting the working repositories. The current runner records both checkouts even though Outbox
  resolves from published v0.15.0; require `outboxVersion=v0.15.0` in each runtime report.
- Run the source gate once on that state and the PostgreSQL/Kafka correctness gates from the release process.
  The candidate's already completed CI can supply the same-commit correctness evidence; do not reuse an older commit's
  result for changed runtime code.
- Docker must expose at least 12 logical CPUs: the PostgreSQL/NATS/API set shares CPUs `0-1`, while k6 uses `2-11`.
  The tested services have a combined 2 GiB memory limit (PostgreSQL 1 GiB, NATS 512 MiB, API 512 MiB), with swap
  disabled. Leave additional host/Docker memory for k6 and builds, and avoid competing load.
- Have Go 1.27, Docker Compose, jq and rsync available. The runner builds with `golang:1.27-alpine` and uses
  PostgreSQL `18-alpine`, NATS `2.12.3-alpine` and k6 `2.2.0`. Keep the resolved images fixed throughout the series;
  runtime reports retain their digests. Run only one capacity series at a time because the runner owns one Compose
  project and recreates its PostgreSQL/JetStream volumes for every cell.

## Fixed-rate confirmation

Run the following with Bash from the clean GoMessenger checkout. It explicitly tests the stronger 200 ms p95 claim;
the general capacity runner otherwise defaults to a 2-second p95 SLO. A failure at 200 ms is a failed latency claim,
even when integrity reconciles. Do not relax the SLO after observing results and present it as the original test.

```bash
set -euo pipefail
test -z "$(git status --porcelain)"
test -z "$(git -C ../outbox status --porcelain)"
git rev-parse HEAD
git -C ../outbox rev-parse HEAD

release_capacity_id="v0.3.0-1500-p95-200-$(date -u +%Y%m%dT%H%M%SZ)"
release_capacity_root="$(pwd)/tmp/capacity/releases/${release_capacity_id}"

for repetition in 1 2 3; do
  for payload in small mixed; do
    CAPACITY_RUN_ID="${release_capacity_id}-${payload}-r${repetition}" \
    CAPACITY_RESULTS_ROOT="${release_capacity_root}" \
    CAPACITY_CELL_RATE=1500 \
    CAPACITY_MIN_RATE=1500 \
    CAPACITY_CELL_VARIANT=full-batch \
    CAPACITY_CELL_TOPOLOGY=o2-c2 \
    CAPACITY_PAYLOAD_PROFILE="${payload}" \
    CAPACITY_POSTGRES_PROFILE=stock \
    CAPACITY_PRECONDITION_DURATION=120s \
    CAPACITY_MEASURED_DURATION=120s \
    CAPACITY_DRAIN_TIMEOUT=60s \
    CAPACITY_E2E_P95_SLO=200ms \
    CAPACITY_OUTBOX_BATCH_MAX_MESSAGES=100 \
    CAPACITY_CONSUMER_BATCH_MAX_MESSAGES=100 \
    bash scripts/run-capacity-cell.sh
  done
done

printf 'Capacity evidence: %s\n' "${release_capacity_root}"
```

The `o2-c2` profile fixes two Outbox workers, two consumer invocations, producer/relay pools of `6+2`, and an Inbox pool
of `8`. Each cell has two minutes of precondition, waits for the required checkpoint, measures for two minutes and
allows up to one minute of drain. Budget at least 24 minutes of warm-up/measurement for six cells, plus checkpoint
waiting, builds, startup and drain. The first failed cell stops this command and retains its artifacts for diagnosis.

## Acceptance and retained evidence

Each payload needs 3/3 sustainable confirmations with the same commits, runtime dependencies, image digests, settings
and resources. Check report spec `2.1`, confirmed batch modes/sizes, the explicit minimum-rate gate and the 200 ms SLO.
Accept only with no HTTP failures or dropped iterations, throughput at least 99% of target, non-growing Outbox and
consumer lags, bounded drain, no Outbox retry/defer/DLQ or broker redelivery/consumer DLQ, zero connection-health
failures, no OOM, and exact post-drain count/hash reconciliation.

Retain every run's `report.json`, `report.md`, `environment.json`, `samples.jsonl`, `resources.jsonl`, k6 summaries,
Compose logs, container state, profiles and PostgreSQL plans. A compact versioned summary must include throughput,
p95, both lags, drain, integrity and source hashes for all six runs, including failures. Review raw archives before
public upload because environment and logs may contain host-specific information.

For a claim about maximum throughput or a batch speedup, use the matched frontier/matrix procedure in the
[performance contract](README.md), with its controls and three confirmations. `capacity-batch-proof` currently accepts
only `checkout-workspace` evidence with a local Outbox development replacement; it rejects published Outbox v0.15.0.
Do not use that validator as a release check for the current published dependency graph.
