#!/usr/bin/env bash

set -uo pipefail

project_name="${1:?compose project name is required}"
output_file="${2:?resource output path is required}"

: > "${output_file}"
while true; do
  container_ids=()
  while IFS= read -r container_id; do
    if [[ -n "${container_id}" ]]; then
      container_ids+=("${container_id}")
    fi
  done < <(docker ps --filter "label=com.docker.compose.project=${project_name}" --format '{{.ID}}' 2>/dev/null)
  if [[ ${#container_ids[@]} -gt 0 ]]; then
    observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker stats --no-stream \
      --format "{\"observedAt\":\"${observed_at}\",\"container\":{{json .}}}" \
      "${container_ids[@]}" >> "${output_file}" 2>/dev/null || true
  fi
  sleep 1
done
