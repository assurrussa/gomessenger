#!/usr/bin/env bash

set -uo pipefail

output_dir="${1:?profile output directory is required}"
port="${CAPACITY_HTTP_PORT:-18080}"
base_url="http://127.0.0.1:${port}"
profile_seconds="${CAPACITY_PPROF_SECONDS:-5}"
if ! [[ "${profile_seconds}" =~ ^[1-9][0-9]*$ ]] || (( profile_seconds > 60 )); then
  printf 'CAPACITY_PPROF_SECONDS must be in 1..60\n' > "${output_dir}/pprof-error.txt"
  exit 0
fi
mkdir -p "${output_dir}"

ready=0
for _ in $(seq 1 120); do
  if curl -fsS --max-time 2 "${base_url}/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" != 1 ]]; then
  printf 'capacity API did not become ready for pprof capture\n' > "${output_dir}/pprof-error.txt"
  exit 0
fi

curl -fsS --max-time "$((profile_seconds + 15))" \
  "${base_url}/debug/pprof/profile?seconds=${profile_seconds}" \
  -o "${output_dir}/cpu.pprof" 2> "${output_dir}/cpu.pprof.error" || true
curl -fsS --max-time 10 "${base_url}/debug/pprof/heap" \
  -o "${output_dir}/heap.pprof" 2> "${output_dir}/heap.pprof.error" || true
curl -fsS --max-time 10 "${base_url}/debug/pprof/goroutine?debug=1" \
  -o "${output_dir}/goroutines.txt" 2> "${output_dir}/goroutines.error" || true
