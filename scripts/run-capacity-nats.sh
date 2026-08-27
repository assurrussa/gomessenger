#!/usr/bin/env bash

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
compose_file="${repo_root}/examples/durable-postgres-nats/compose.capacity.yaml"
project_name="gomessenger-capacity-nats"

run_id="${CAPACITY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
results_dir="${repo_root}/tmp/capacity/${run_id}"
mkdir -p "${results_dir}"

export CAPACITY_RUN_ID="${run_id}"
export CAPACITY_HOST_OS="${CAPACITY_HOST_OS:-$(uname -s)}"
export CAPACITY_HOST_ARCH="${CAPACITY_HOST_ARCH:-$(uname -m)}"
if command -v getconf >/dev/null 2>&1; then
  export CAPACITY_HOST_CPUS="${CAPACITY_HOST_CPUS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)}"
fi
export CAPACITY_HOST_CPUS="${CAPACITY_HOST_CPUS:-unknown}"
export CAPACITY_GIT_COMMIT="${CAPACITY_GIT_COMMIT:-$(git -C "${repo_root}" rev-parse HEAD 2>/dev/null || printf unknown)}"
if [[ -n "$(git -C "${repo_root}" status --porcelain 2>/dev/null)" ]]; then
  export CAPACITY_GIT_DIRTY="true"
else
  export CAPACITY_GIT_DIRTY="false"
fi

set +e
docker compose -p "${project_name}" -f "${compose_file}" \
  up --build --abort-on-container-exit --exit-code-from capacity-runner 2>&1 \
  | tee "${results_dir}/compose.log"
run_status=${PIPESTATUS[0]}

if [[ "${KEEP_CAPACITY_STACK:-0}" != "1" ]]; then
  docker compose -p "${project_name}" -f "${compose_file}" \
    down --volumes --remove-orphans
  cleanup_status=$?
  if [[ ${run_status} -eq 0 && ${cleanup_status} -ne 0 ]]; then
    run_status=${cleanup_status}
  fi
else
  printf 'Capacity stack preserved for diagnostics: project=%s\n' "${project_name}"
fi

printf 'Capacity artifacts: %s\n' "${results_dir}"
exit "${run_status}"
