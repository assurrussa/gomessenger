#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
outbox_root="$(cd "${repo_root}/../outbox" && pwd)"
frontier_script="${repo_root}/scripts/run-capacity-frontier.sh"
cell_script="${repo_root}/scripts/run-capacity-cell.sh"
run_script="${repo_root}/scripts/run-capacity-nats.sh"

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

proof_id="${CAPACITY_PROOF_ID:-batch-proof-$(date -u +%Y%m%dT%H%M%SZ)}"
if ! [[ "${proof_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  printf 'CAPACITY_PROOF_ID must contain only letters, digits, dot, underscore, or dash\n' >&2
  exit 2
fi
proof_dir="${repo_root}/tmp/capacity/frontiers/${proof_id}"
compact_report="${repo_root}/docs/performance/${proof_id}.md"
if [[ -e "${proof_dir}" || -e "${compact_report}" ]]; then
  printf 'proof ID %s already has artifacts; choose a new CAPACITY_PROOF_ID\n' "${proof_id}" >&2
  exit 2
fi

if [[ -n "$(git -C "${repo_root}" status --porcelain)" ]]; then
  printf 'GoMessenger checkout must be clean before a batch proof run\n' >&2
  exit 2
fi
if [[ -n "$(git -C "${outbox_root}" status --porcelain)" ]]; then
  printf 'Outbox checkout must be clean before a batch proof run\n' >&2
  exit 2
fi

printf '\n==> Outbox source and backend gate\n'
make -C "${outbox_root}" check-all
printf '\n==> GoMessenger checkout-workspace source gate\n'
make -C "${repo_root}" check-workspace
printf '\n==> GoMessenger batch integration gate\n'
make -C "${repo_root}" test-batch-integration

printf '\n==> Build immutable capacity images once for the proof\n'
CAPACITY_RUN_ID="${proof_id}-image-build" \
CAPACITY_RESULTS_ROOT="${proof_dir}/build" \
CAPACITY_BUILD_ONLY=1 \
bash "${run_script}"
export CAPACITY_SKIP_BUILD=1

git_commit="$(git -C "${repo_root}" rev-parse HEAD)"
outbox_git_commit="$(git -C "${outbox_root}" rev-parse HEAD)"
frontiers_file="${proof_dir}/frontiers.jsonl"
common_runs_file="${proof_dir}/common-runs.jsonl"
manifest_file="${proof_dir}/manifest.json"
mkdir -p "${proof_dir}"
: > "${frontiers_file}"
: > "${common_runs_file}"

payloads=(small mixed)
variants=(legacy consumer-batch relay-batch full-batch)

for payload in "${payloads[@]}"; do
  for variant in "${variants[@]}"; do
    frontier_id="${proof_id}-frontier-${payload}-${variant}"
    printf '\n==> proof frontier payload=%s variant=%s\n' "${payload}" "${variant}"
    CAPACITY_FRONTIER_ID="${frontier_id}" \
    CAPACITY_FRONTIER_ROOT="${proof_dir}/frontiers" \
    CAPACITY_RESULTS_ROOT="${proof_dir}/runs" \
    CAPACITY_FRONTIER_TOPOLOGY=o2-c2 \
    CAPACITY_FRONTIER_VARIANT="${variant}" \
    CAPACITY_PAYLOAD_PROFILE="${payload}" \
    CAPACITY_POSTGRES_PROFILE=stock \
    CAPACITY_FRONTIER_CONFIRMATIONS=3 \
    CAPACITY_FRONTIER_RUN_CONTROL=0 \
    CAPACITY_PRECONDITION_DURATION=60s \
    CAPACITY_MEASURED_DURATION=120s \
    CAPACITY_DRAIN_TIMEOUT=60s \
    CAPACITY_OUTBOX_BATCH_MAX_MESSAGES=100 \
    CAPACITY_CONSUMER_BATCH_MAX_MESSAGES=100 \
    bash "${frontier_script}"

    frontier_path="${proof_dir}/frontiers/${frontier_id}/frontier.json"
    jq -n \
      --arg payloadProfile "${payload}" \
      --arg variant "${variant}" \
      --arg path "${frontier_path}" \
      '{payloadProfile:$payloadProfile,variant:$variant,path:$path}' \
      >> "${frontiers_file}"
  done
done

minimum_frontier=0
while IFS= read -r frontier_path; do
  frontier_rate="$(jq -r '.frontierRate' "${frontier_path}")"
  if (( minimum_frontier == 0 || frontier_rate < minimum_frontier )); then
    minimum_frontier="${frontier_rate}"
  fi
done < <(jq -r '.path' "${frontiers_file}")
if (( minimum_frontier == 0 )); then
  printf 'no frontier found for the common-rate series\n' >&2
  exit 1
fi
common_rate=$((((minimum_frontier * 80 / 100) / 50) * 50))
if (( common_rate < 50 )); then
  printf 'common rate is below 50 msg/s\n' >&2
  exit 1
fi

common_status=0
for payload in "${payloads[@]}"; do
  for ordinal in 1 2 3; do
    case "${ordinal}" in
      1) order=(legacy consumer-batch relay-batch full-batch) ;;
      2) order=(full-batch relay-batch consumer-batch legacy) ;;
      3) order=(consumer-batch legacy full-batch relay-batch) ;;
    esac
    for variant in "${order[@]}"; do
      run_id="$(short_capacity_run_id "${proof_id}-common-${payload}-${variant}-r${ordinal}")"
      result_dir="${proof_dir}/runs/${run_id}"
      printf '\n==> proof common payload=%s variant=%s rate=%d repetition=%d\n' \
        "${payload}" "${variant}" "${common_rate}" "${ordinal}"
      set +e
      CAPACITY_RUN_ID="${run_id}" \
      CAPACITY_RESULTS_ROOT="${proof_dir}/runs" \
      CAPACITY_CELL_RATE="${common_rate}" \
      CAPACITY_CELL_VARIANT="${variant}" \
      CAPACITY_CELL_TOPOLOGY=o2-c2 \
      CAPACITY_PAYLOAD_PROFILE="${payload}" \
      CAPACITY_POSTGRES_PROFILE=stock \
      CAPACITY_PRECONDITION_DURATION=60s \
      CAPACITY_MEASURED_DURATION=120s \
      CAPACITY_DRAIN_TIMEOUT=60s \
      CAPACITY_OUTBOX_BATCH_MAX_MESSAGES=100 \
      CAPACITY_CONSUMER_BATCH_MAX_MESSAGES=100 \
      bash "${cell_script}"
      command_status=$?
      set -e
      if (( command_status != 0 )); then
        common_status=1
      fi

      jq -n \
        --arg payloadProfile "${payload}" \
        --arg variant "${variant}" \
        --arg runId "${run_id}" \
        --arg report "${result_dir}/report.json" \
        --arg resources "${result_dir}/resources.jsonl" \
        --arg samples "${result_dir}/samples.jsonl" \
        --argjson repetition "${ordinal}" \
        --argjson rate "${common_rate}" \
        --argjson commandStatus "${command_status}" \
        '{payloadProfile:$payloadProfile,variant:$variant,repetition:$repetition,rate:$rate,runId:$runId,commandStatus:$commandStatus,report:$report,resources:$resources,samples:$samples}' \
        >> "${common_runs_file}"
    done
  done
done

jq -n \
  --arg proofId "${proof_id}" \
  --arg evidenceScope "checkout-workspace" \
  --arg gitCommit "${git_commit}" \
  --arg outboxGitCommit "${outbox_git_commit}" \
  --slurpfile frontiers "${frontiers_file}" \
  --slurpfile commonRuns "${common_runs_file}" \
  '{
    specVersion:"2.1-batch-proof-manifest-1",
    proofId:$proofId,
    evidenceScope:$evidenceScope,
    gitCommit:$gitCommit,
    outboxGitCommit:$outboxGitCommit,
    topology:"o2-c2",
    postgresProfile:"stock",
    warmupSeconds:60,
    measuredSeconds:120,
    drainTimeoutSeconds:60,
    confirmations:3,
    frontierStep:50,
    advantageThreshold:1.3,
    maximumP95Ratio:1.1,
    minimumAverageBatch:10,
    maximumMemoryFraction:0.8,
    maximumMemoryGrowthRatio:0.1,
    maximumMemoryGrowthBytes:33554432,
    frontiers:$frontiers,
    commonRuns:$commonRuns
  }' > "${manifest_file}"

set +e
(cd "${repo_root}/examples/durable-postgres-nats" && \
  go run ./cmd/batch-proof -dir "${proof_dir}")
verdict_status=$?
set -e
if (( common_status != 0 || verdict_status != 0 )); then
  exit 1
fi

cp "${proof_dir}/proof.md" "${compact_report}"
printf '\nCheckout-workspace batch proof: %s\n' "${proof_dir}/proof.json"
printf 'Compact local report: %s\n' "${compact_report}"
