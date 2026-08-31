#!/usr/bin/env bash

set -uo pipefail

project_name="${1:?compose project name is required}"
compose_file="${2:?compose file is required}"
sql_file="${3:?EXPLAIN SQL file is required}"
output_file="${4:?EXPLAIN output file is required}"

for _ in $(seq 1 120); do
  table_name="$(docker compose -p "${project_name}" -f "${compose_file}" exec -T postgres \
	psql -U gomessenger -d gomessenger -Atqc "SELECT to_regclass('public.jobs')" 2>/dev/null || true)"
  if [[ "${table_name}" == jobs ]]; then
    if ! docker compose -p "${project_name}" -f "${compose_file}" exec -T postgres \
      psql -X -v ON_ERROR_STOP=1 -U gomessenger -d gomessenger \
      < "${sql_file}" > "${output_file}" 2>&1; then
      printf 'capacity EXPLAIN capture failed\n' >> "${output_file}"
    fi
    exit 0
  fi
  sleep 1
done

printf 'capacity EXPLAIN capture timed out waiting for the Outbox schema\n' > "${output_file}"
