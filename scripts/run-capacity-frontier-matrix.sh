#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
frontier_script="${repo_root}/scripts/run-capacity-frontier.sh"
matrix_id="${CAPACITY_MATRIX_ID:-matrix-$(date -u +%Y%m%dT%H%M%SZ)}"
matrix_dir="${repo_root}/tmp/capacity/frontiers/${matrix_id}"
mkdir -p "${matrix_dir}"

run_frontier() {
  local frontier_id="$1"
  local topology="$2"
  local variant="$3"
  local payload="$4"
  local postgres_profile="$5"
  local control="$6"
  CAPACITY_FRONTIER_ID="${frontier_id}" \
  CAPACITY_FRONTIER_TOPOLOGY="${topology}" \
  CAPACITY_FRONTIER_VARIANT="${variant}" \
  CAPACITY_PAYLOAD_PROFILE="${payload}" \
  CAPACITY_POSTGRES_PROFILE="${postgres_profile}" \
  CAPACITY_FRONTIER_RUN_CONTROL="${control}" \
  bash "${frontier_script}"
}

topology_summaries=()
topology_variant="${CAPACITY_TOPOLOGY_VARIANT:-full-batch}"
topology_payload="${CAPACITY_TOPOLOGY_PAYLOAD:-mixed}"
for topology in o1-c1 o2-c1 o2-c2; do
  frontier_id="${matrix_id}-topology-${topology}"
  run_frontier "${frontier_id}" "${topology}" "${topology_variant}" "${topology_payload}" stock 0
  topology_summaries+=("${repo_root}/tmp/capacity/frontiers/${frontier_id}/frontier.json")
done

winner="$(jq -s -r 'sort_by(-.frontierRate, .p95Millis, .drainSeconds, .walPerMessage) | .[0].topology' \
  "${topology_summaries[@]}")"
printf '\nTopology winner: %s\n' "${winner}"

result_summaries=("${topology_summaries[@]}")
for payload in small mixed; do
  for variant in legacy consumer-batch relay-batch full-batch; do
    frontier_id="${matrix_id}-stock-${winner}-${payload}-${variant}"
    control=1
    [[ "${variant}" != legacy ]] || control=0
    run_frontier "${frontier_id}" "${winner}" "${variant}" "${payload}" stock "${control}"
    result_summaries+=("${repo_root}/tmp/capacity/frontiers/${frontier_id}/frontier.json")
  done
done

if [[ "${CAPACITY_RUN_TUNED:-1}" == 1 ]]; then
  for postgres_profile in checkpoint memory-wal combined; do
    for payload in small mixed; do
      for variant in relay-batch full-batch; do
        frontier_id="${matrix_id}-${postgres_profile}-${winner}-${payload}-${variant}"
        run_frontier "${frontier_id}" "${winner}" "${variant}" "${payload}" "${postgres_profile}" 1
        result_summaries+=("${repo_root}/tmp/capacity/frontiers/${frontier_id}/frontier.json")
      done
    done
  done
fi

jq -s \
  --arg matrixId "${matrix_id}" \
  --arg winner "${winner}" \
  --arg topologyVariant "${topology_variant}" \
  --arg topologyPayload "${topology_payload}" \
  '{
    specVersion:"2.1-frontier-matrix-1",
    matrixId:$matrixId,
    topologyWinner:$winner,
    topologyCalibration:{variant:$topologyVariant,payloadProfile:$topologyPayload},
    frontiers:.
  }' "${result_summaries[@]}" > "${matrix_dir}/matrix.json"

printf '\nCapacity frontier matrix: %s\n' "${matrix_dir}/matrix.json"
