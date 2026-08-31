#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cell_script="${repo_root}/scripts/run-capacity-cell.sh"

command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 2; }

short_capacity_run_id() {
  local value="$1"
  local maximum=64
  if (( ${#value} <= maximum )); then
    printf '%s' "${value}"
    return
  fi
  local digest
  digest="$(printf '%s' "${value}" | git -C "${repo_root}" hash-object --stdin)"
  printf '%s-%s' "${value:0:$((maximum - 13))}" "${digest:0:12}"
}

variant="${CAPACITY_FRONTIER_VARIANT:-full-batch}"
topology="${CAPACITY_FRONTIER_TOPOLOGY:-o2-c2}"
payload="${CAPACITY_PAYLOAD_PROFILE:-mixed}"
postgres_profile="${CAPACITY_POSTGRES_PROFILE:-stock}"
frontier_id="${CAPACITY_FRONTIER_ID:-frontier-$(date -u +%Y%m%dT%H%M%SZ)-${variant}-${topology}-${payload}-${postgres_profile}}"
if ! [[ "${frontier_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  printf 'CAPACITY_FRONTIER_ID must contain only letters, digits, dot, underscore, or dash\n' >&2
  exit 2
fi
frontier_root="${CAPACITY_FRONTIER_ROOT:-${repo_root}/tmp/capacity/frontiers}"
results_root="${CAPACITY_RESULTS_ROOT:-${repo_root}/tmp/capacity}"
if [[ "${frontier_root}" != /* ]]; then
  frontier_root="${repo_root}/${frontier_root}"
fi
if [[ "${results_root}" != /* ]]; then
  results_root="${repo_root}/${results_root}"
fi
frontier_dir="${frontier_root}/${frontier_id}"
runs_file="${frontier_dir}/runs.jsonl"
mkdir -p "${frontier_dir}"
: > "${runs_file}"

start_rate="${CAPACITY_FRONTIER_START_RATE:-250}"
fallback_rate="${CAPACITY_FRONTIER_FALLBACK_RATE:-100}"
step="${CAPACITY_FRONTIER_STEP:-50}"
maximum_rate="${CAPACITY_FRONTIER_MAX_RATE:-16000}"
confirmations="${CAPACITY_FRONTIER_CONFIRMATIONS:-3}"
run_control="${CAPACITY_FRONTIER_RUN_CONTROL:-1}"
outbox_batch_size="${CAPACITY_OUTBOX_BATCH_MAX_MESSAGES:-100}"
consumer_batch_size="${CAPACITY_CONSUMER_BATCH_MAX_MESSAGES:-100}"

for numeric in start_rate fallback_rate step maximum_rate confirmations outbox_batch_size consumer_batch_size; do
  value="${!numeric}"
  if ! [[ "${value}" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer, got %q\n' "${numeric}" "${value}" >&2
    exit 2
  fi
done
if (( fallback_rate >= start_rate || step > start_rate || maximum_rate < start_rate )); then
  printf 'invalid frontier bounds: fallback=%d start=%d step=%d max=%d\n' \
    "${fallback_rate}" "${start_rate}" "${step}" "${maximum_rate}" >&2
  exit 2
fi
if [[ "${run_control}" != "0" && "${run_control}" != "1" ]]; then
  printf 'CAPACITY_FRONTIER_RUN_CONTROL must be 0 or 1\n' >&2
  exit 2
fi

attempt=0
candidate_state=invalid
candidate_report=
candidate_run_id=

run_candidate() {
  local rate="$1"
  local phase="$2"
  local outbox_batch="$3"
  local consumer_batch="$4"
  attempt=$((attempt + 1))
  candidate_run_id="$(short_capacity_run_id \
    "${frontier_id}-${phase}-r$(printf '%06d' "${rate}")-n$(printf '%03d' "${attempt}")")"
  candidate_report="${results_root}/${candidate_run_id}/report.json"

  printf '\n==> frontier candidate %d msg/s (%s, outbox_batch=%s, consumer_batch=%s, run=%s)\n' \
    "${rate}" "${phase}" "${outbox_batch}" "${consumer_batch}" "${candidate_run_id}"
  set +e
  CAPACITY_RUN_ID="${candidate_run_id}" \
  CAPACITY_CELL_RATE="${rate}" \
  CAPACITY_CELL_VARIANT="${variant}" \
  CAPACITY_CELL_TOPOLOGY="${topology}" \
  CAPACITY_PAYLOAD_PROFILE="${payload}" \
  CAPACITY_POSTGRES_PROFILE="${postgres_profile}" \
  CAPACITY_OUTBOX_BATCH_MAX_MESSAGES="${outbox_batch}" \
  CAPACITY_CONSUMER_BATCH_MAX_MESSAGES="${consumer_batch}" \
  bash "${cell_script}"
  local command_status=$?
  set -e

  candidate_state=invalid
  if [[ "${command_status}" == 0 && -f "${candidate_report}" ]] && jq -e '
      .specVersion == "2.1" and .integrityPassed == true and
      (.stages | length) == 1 and (.failure // "") == ""
    ' "${candidate_report}" >/dev/null; then
    if jq -e '.stages[0].sustainable == true' "${candidate_report}" >/dev/null; then
      candidate_state=pass
    else
      candidate_state=fail
    fi
  fi

  jq -n \
    --arg runId "${candidate_run_id}" \
    --arg phase "${phase}" \
    --arg state "${candidate_state}" \
    --arg report "${candidate_report}" \
    --argjson rate "${rate}" \
    --argjson outboxBatchSize "${outbox_batch}" \
    --argjson consumerBatchSize "${consumer_batch}" \
    --argjson commandStatus "${command_status}" \
    '{runId:$runId,phase:$phase,state:$state,rate:$rate,outboxBatchSize:$outboxBatchSize,consumerBatchSize:$consumerBatchSize,commandStatus:$commandStatus,report:$report}' \
    >> "${runs_file}"

  if [[ "${candidate_state}" == invalid ]]; then
    printf 'candidate run is invalid; inspect %s\n' "${candidate_report}" >&2
  fi
}

low=0
high=0
run_candidate "${start_rate}" ladder "${outbox_batch_size}" "${consumer_batch_size}"
case "${candidate_state}" in
  pass)
    low="${start_rate}"
    candidate=$((start_rate * 2))
    while (( candidate <= maximum_rate )); do
      run_candidate "${candidate}" ladder "${outbox_batch_size}" "${consumer_batch_size}"
      [[ "${candidate_state}" != invalid ]] || exit 1
      if [[ "${candidate_state}" == fail ]]; then
        high="${candidate}"
        break
      fi
      low="${candidate}"
      candidate=$((candidate * 2))
    done
    if (( high == 0 )); then
      high=$((maximum_rate + step))
    fi
    ;;
  fail)
    high="${start_rate}"
    run_candidate "${fallback_rate}" fallback "${outbox_batch_size}" "${consumer_batch_size}"
    [[ "${candidate_state}" != invalid ]] || exit 1
    if [[ "${candidate_state}" == pass ]]; then
      low="${fallback_rate}"
    fi
    ;;
  invalid)
    exit 1
    ;;
esac

while (( low > 0 && high - low > step )); do
  candidate=$((((low + high) / 2 / step) * step))
  if (( candidate <= low )); then
    candidate=$((low + step))
  fi
  run_candidate "${candidate}" bisection "${outbox_batch_size}" "${consumer_batch_size}"
  [[ "${candidate_state}" != invalid ]] || exit 1
  if [[ "${candidate_state}" == pass ]]; then
    low="${candidate}"
  else
    high="${candidate}"
  fi
done

confirmed=0
frontier="${low}"
while (( frontier > 0 && confirmed == 0 )); do
  passed=0
  for ordinal in $(seq 1 "${confirmations}"); do
    run_candidate "${frontier}" "confirm-${ordinal}" "${outbox_batch_size}" "${consumer_batch_size}"
    [[ "${candidate_state}" != invalid ]] || exit 1
    if [[ "${candidate_state}" == pass ]]; then
      passed=$((passed + 1))
    fi
  done
  if (( passed == confirmations )); then
    confirmed=1
  else
    frontier=$((frontier - step))
  fi
done

if (( frontier == 0 )); then
  printf 'no sustainable rate at or above %d msg/s\n' "${fallback_rate}" >&2
  exit 1
fi

control_report=
if [[ "${variant}" != legacy && "${run_control}" == 1 ]]; then
  control_rate="${CAPACITY_CONTROL_RATE:-${frontier}}"
  run_candidate "${control_rate}" control 1 1
  [[ "${candidate_state}" != invalid ]] || exit 1
  control_report="${candidate_report}"
fi

confirmation_report="${candidate_report}"
if [[ -n "${control_report}" ]]; then
  confirmation_report="$(jq -r 'select(.phase | startswith("confirm-")) | .report' "${runs_file}" | tail -n 1)"
fi
committed="$(jq -r '.stages[0].loadWindow.committed' "${confirmation_report}")"
wal_bytes="$(jq -r '.stages[0].postgresql.loadDelta.wal.wal_bytes // 0' "${confirmation_report}")"
wal_per_message="$(jq -n --argjson wal "${wal_bytes}" --argjson messages "${committed}" \
  'if $messages > 0 then $wal / $messages else 0 end')"

jq -s \
  --arg frontierId "${frontier_id}" \
  --arg variant "${variant}" \
  --arg topology "${topology}" \
  --arg payload "${payload}" \
  --arg postgresProfile "${postgres_profile}" \
  --arg confirmationReport "${confirmation_report}" \
  --arg controlReport "${control_report}" \
  --argjson frontierRate "${frontier}" \
  --argjson outboxBatchSize "${outbox_batch_size}" \
  --argjson consumerBatchSize "${consumer_batch_size}" \
  --argjson p95Millis "$(jq -r '.stages[0].latency.p95Millis' "${confirmation_report}")" \
  --argjson drainSeconds "$(jq -r '.stages[0].drainSeconds' "${confirmation_report}")" \
  --argjson walPerMessage "${wal_per_message}" \
  '{
    specVersion:"2.1-frontier-1",
    frontierId:$frontierId,
    variant:$variant,
    topology:$topology,
    payloadProfile:$payload,
    postgresProfile:$postgresProfile,
    frontierRate:$frontierRate,
    outboxBatchMaxMessages:$outboxBatchSize,
    consumerBatchMaxMessages:$consumerBatchSize,
    p95Millis:$p95Millis,
    drainSeconds:$drainSeconds,
    walPerMessage:$walPerMessage,
    confirmationReport:$confirmationReport,
    controlReport:(if $controlReport == "" then null else $controlReport end),
    runs:.
  }' "${runs_file}" > "${frontier_dir}/frontier.json"

printf '\nConfirmed frontier: %d msg/s\n' "${frontier}"
printf 'Frontier summary: %s\n' "${frontier_dir}/frontier.json"
