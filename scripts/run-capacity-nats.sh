#!/usr/bin/env bash

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
compose_file="${repo_root}/examples/durable-postgres-nats/compose.capacity.yaml"
project_name="gomessenger-capacity-nats"

run_id="${CAPACITY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
results_root="${CAPACITY_RESULTS_ROOT:-${repo_root}/tmp/capacity}"
if [[ "${results_root}" != /* ]]; then
  results_root="${repo_root}/${results_root}"
fi
results_dir="${results_root}/${run_id}"
mkdir -p "${results_dir}"

export CAPACITY_RUN_ID="${run_id}"
export CAPACITY_RESULTS_HOST_DIR="${results_root}"
export CAPACITY_HOST_OS="${CAPACITY_HOST_OS:-$(uname -s)}"
export CAPACITY_HOST_ARCH="${CAPACITY_HOST_ARCH:-$(uname -m)}"
if command -v getconf >/dev/null 2>&1; then
  export CAPACITY_HOST_CPUS="${CAPACITY_HOST_CPUS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)}"
fi
export CAPACITY_HOST_CPUS="${CAPACITY_HOST_CPUS:-unknown}"
export CAPACITY_GIT_COMMIT="$(git -C "${repo_root}" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "${repo_root}" status --porcelain 2>/dev/null)" ]]; then
  export CAPACITY_GIT_DIRTY="true"
else
  export CAPACITY_GIT_DIRTY="false"
fi
outbox_root="$(cd "${repo_root}/../outbox" 2>/dev/null && pwd || true)"
if [[ -z "${outbox_root}" || ! -d "${outbox_root}/.git" ]]; then
  printf 'The checkout-local capacity build requires ../outbox\n' >&2
  exit 1
fi
export CAPACITY_OUTBOX_GIT_COMMIT="$(git -C "${outbox_root}" rev-parse HEAD)"
if [[ -n "$(git -C "${outbox_root}" status --porcelain)" ]]; then
  export CAPACITY_OUTBOX_GIT_DIRTY=true
else
  export CAPACITY_OUTBOX_GIT_DIRTY=false
fi

mkdir -p "${repo_root}/tmp"
build_context="$(mktemp -d "${repo_root}/tmp/capacity-build-context.XXXXXX")"
cleanup_build_context() {
  rm -rf "${build_context}"
}
trap cleanup_build_context EXIT
export CAPACITY_BUILD_CONTEXT="${build_context}"
rsync -a --delete \
  --exclude='.git' --exclude='tmp' --exclude='coverage' \
  "${repo_root}/" "${build_context}/gomessenger/"
rsync -a --delete \
  --exclude='.git' --exclude='.cache' --exclude='tmp' --exclude='coverage' \
  "${outbox_root}/" "${build_context}/outbox/"

postgres_image="${POSTGRES_IMAGE:-postgres:18-alpine}"
nats_image="${NATS_IMAGE:-nats:2.12.3-alpine}"
for image in "${postgres_image}" "${nats_image}"; do
  if [[ "${CAPACITY_REFRESH_IMAGES:-0}" == 1 ]] || ! docker image inspect "${image}" >/dev/null 2>&1; then
    if ! docker pull "${image}"; then
      printf 'Unable to pull capacity image %s\n' "${image}" >&2
      exit 1
    fi
  fi
done
export POSTGRES_IMAGE="${postgres_image}"
export NATS_IMAGE="${nats_image}"
export CAPACITY_POSTGRES_IMAGE="${postgres_image}"
export CAPACITY_NATS_IMAGE="${nats_image}"
export CAPACITY_POSTGRES_IMAGE_DIGEST="$(docker image inspect --format '{{index .RepoDigests 0}}' "${postgres_image}")"
export CAPACITY_NATS_IMAGE_DIGEST="$(docker image inspect --format '{{index .RepoDigests 0}}' "${nats_image}")"

build_only="${CAPACITY_BUILD_ONLY:-0}"
skip_build="${CAPACITY_SKIP_BUILD:-0}"
if [[ "${build_only}" != "0" && "${build_only}" != "1" ]]; then
  printf 'CAPACITY_BUILD_ONLY must be 0 or 1\n' >&2
  exit 2
fi
if [[ "${skip_build}" != "0" && "${skip_build}" != "1" ]]; then
  printf 'CAPACITY_SKIP_BUILD must be 0 or 1\n' >&2
  exit 2
fi
if [[ "${build_only}" == "1" ]]; then
  docker compose -p "${project_name}" -f "${compose_file}" \
    build capacity-api capacity-runner
  exit $?
fi

compose_build_args=(--build)
if [[ "${skip_build}" == "1" ]]; then
  compose_build_args=(--no-build)
fi

# Every capacity run starts from clean PostgreSQL and JetStream volumes. This
# also makes repeated frontier confirmations independent observations.
if ! docker compose -p "${project_name}" -f "${compose_file}" \
  down --volumes --remove-orphans >/dev/null 2>&1; then
  printf 'Unable to clean the previous capacity stack\n' >&2
  exit 1
fi

resources_file="${results_dir}/resources.jsonl"
bash "${repo_root}/scripts/sample-capacity-resources.sh" "${project_name}" "${resources_file}" &
resource_sampler_pid=$!
bash "${repo_root}/scripts/capture-capacity-profiles.sh" "${results_dir}" &
profile_sampler_pid=$!
bash "${repo_root}/scripts/capture-capacity-explain.sh" \
  "${project_name}" "${compose_file}" "${repo_root}/scripts/capacity-explain.sql" \
  "${results_dir}/postgres-explain.txt" &
explain_sampler_pid=$!

set +e
docker compose -p "${project_name}" -f "${compose_file}" \
  up "${compose_build_args[@]}" --abort-on-container-exit --exit-code-from capacity-runner 2>&1 \
  | tee "${results_dir}/compose.log"
run_status=${PIPESTATUS[0]}

kill "${resource_sampler_pid}" 2>/dev/null
wait "${resource_sampler_pid}" 2>/dev/null
kill "${profile_sampler_pid}" 2>/dev/null
wait "${profile_sampler_pid}" 2>/dev/null
kill "${explain_sampler_pid}" 2>/dev/null
wait "${explain_sampler_pid}" 2>/dev/null

bash "${repo_root}/scripts/capture-capacity-container-state.sh" \
  "${project_name}" "${results_dir}/container-state.json"
state_status=$?
if [[ ${run_status} -eq 0 && ${state_status} -ne 0 ]]; then
  run_status=${state_status}
fi
if jq -e 'any(.[]; .oomKilled == true)' "${results_dir}/container-state.json" >/dev/null; then
  printf 'Capacity run observed an OOM-killed container\n' >&2
  run_status=1
fi

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
