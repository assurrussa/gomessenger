#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
run_script="${repo_root}/scripts/run-capacity-nats.sh"

variant="${CAPACITY_CELL_VARIANT:-full-batch}"
topology="${CAPACITY_CELL_TOPOLOGY:-o2-c2}"
payload="${CAPACITY_PAYLOAD_PROFILE:-mixed}"
postgres_profile="${CAPACITY_POSTGRES_PROFILE:-stock}"
rate="${CAPACITY_CELL_RATE:?CAPACITY_CELL_RATE is required}"
run_id="${CAPACITY_RUN_ID:?CAPACITY_RUN_ID is required}"
outbox_batch_size="${CAPACITY_OUTBOX_BATCH_MAX_MESSAGES:-100}"
consumer_batch_size="${CAPACITY_CONSUMER_BATCH_MAX_MESSAGES:-100}"

for numeric in rate outbox_batch_size consumer_batch_size; do
  value="${!numeric}"
  if ! [[ "${value}" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer, got %q\n' "${numeric}" "${value}" >&2
    exit 2
  fi
done

case "${variant}" in
  legacy)
    ingress_mode=single
    relay_mode=single
    consumer_mode=single
    ;;
  consumer-batch)
    ingress_mode=single
    relay_mode=single
    consumer_mode=batch
    ;;
  relay-batch)
    ingress_mode=single
    relay_mode=batch
    consumer_mode=batch
    ;;
  full-batch)
    ingress_mode=batch
    relay_mode=batch
    consumer_mode=batch
    ;;
  *)
    printf 'CAPACITY_CELL_VARIANT must be legacy, consumer-batch, relay-batch, or full-batch\n' >&2
    exit 2
    ;;
esac

case "${topology}" in
  o1-c1)
    outbox_workers=1
    consumer_concurrency=1
    producer_conns=4
    relay_conns=1
    db_conns=4
    ;;
  o2-c1)
    outbox_workers=2
    consumer_concurrency=1
    producer_conns=6
    relay_conns=2
    db_conns=6
    ;;
  o2-c2)
    outbox_workers=2
    consumer_concurrency=2
    producer_conns=6
    relay_conns=2
    db_conns=8
    ;;
  *)
    printf 'CAPACITY_CELL_TOPOLOGY must be o1-c1, o2-c1, or o2-c2\n' >&2
    exit 2
    ;;
esac

case "${postgres_profile}" in
  stock)
    checkpoint_timeout=5min
    max_wal_size=1GB
    shared_buffers=128MB
    wal_buffers=-1
    ;;
  checkpoint)
    checkpoint_timeout=15min
    max_wal_size=4GB
    shared_buffers=128MB
    wal_buffers=-1
    ;;
  memory-wal)
    checkpoint_timeout=5min
    max_wal_size=1GB
    shared_buffers=256MB
    wal_buffers=16MB
    ;;
  combined)
    checkpoint_timeout=15min
    max_wal_size=4GB
    shared_buffers=256MB
    wal_buffers=16MB
    ;;
  *)
    printf 'CAPACITY_POSTGRES_PROFILE must be stock, checkpoint, memory-wal, or combined\n' >&2
    exit 2
    ;;
esac

CAPACITY_RUN_ID="${run_id}" \
CAPACITY_PROFILE=full \
CAPACITY_RATES="${rate}" \
CAPACITY_WARMUP_DURATION="${CAPACITY_PRECONDITION_DURATION:-2m}" \
CAPACITY_STAGE_DURATION="${CAPACITY_MEASURED_DURATION:-2m}" \
CAPACITY_DRAIN_TIMEOUT="${CAPACITY_DRAIN_TIMEOUT:-60s}" \
CAPACITY_READY_TIMEOUT="${CAPACITY_READY_TIMEOUT:-90s}" \
CAPACITY_E2E_P95_SLO="${CAPACITY_E2E_P95_SLO:-2s}" \
CAPACITY_CHECKPOINT_TIMEOUT="${CAPACITY_CHECKPOINT_TIMEOUT:-5m}" \
CAPACITY_PPROF_SECONDS="${CAPACITY_PPROF_SECONDS:-30}" \
CAPACITY_PAYLOAD_PROFILE="${payload}" \
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18-alpine}" \
CAPACITY_POSTGRES_PROFILE="${postgres_profile}" \
POSTGRES_CHECKPOINT_TIMEOUT="${POSTGRES_CHECKPOINT_TIMEOUT:-${checkpoint_timeout}}" \
POSTGRES_MAX_WAL_SIZE="${max_wal_size}" \
POSTGRES_SHARED_BUFFERS="${shared_buffers}" \
POSTGRES_WAL_BUFFERS="${wal_buffers}" \
OUTBOX_WORKERS="${outbox_workers}" \
OUTBOX_RESERVATION_BATCH_SIZE=1 \
OUTBOX_PRODUCER_MAX_CONNS="${producer_conns}" \
OUTBOX_RELAY_MAX_CONNS="${relay_conns}" \
OUTBOX_INGRESS_MODE="${ingress_mode}" \
OUTBOX_RELAY_MODE="${relay_mode}" \
OUTBOX_BATCH_MAX_MESSAGES="${outbox_batch_size}" \
OUTBOX_BATCH_MAX_BYTES="${CAPACITY_OUTBOX_BATCH_MAX_BYTES:-4194304}" \
OUTBOX_BATCH_MAX_WAIT="${CAPACITY_OUTBOX_BATCH_MAX_WAIT:-25ms}" \
NATS_CONSUMER_CONCURRENCY="${consumer_concurrency}" \
CONSUMER_MODE="${consumer_mode}" \
CONSUMER_BATCH_MAX_MESSAGES="${consumer_batch_size}" \
CONSUMER_BATCH_MAX_BYTES="${CAPACITY_CONSUMER_BATCH_MAX_BYTES:-4194304}" \
CONSUMER_BATCH_MAX_WAIT="${CAPACITY_CONSUMER_BATCH_MAX_WAIT:-25ms}" \
DB_MAX_OPEN_CONNS="${db_conns}" \
bash "${run_script}"
