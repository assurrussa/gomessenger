#!/usr/bin/env bash

set -euo pipefail

project_name="${1:?compose project name is required}"
output_file="${2:?container state output path is required}"

container_ids=()
while IFS= read -r container_id; do
  [[ -z "${container_id}" ]] || container_ids+=("${container_id}")
done < <(docker ps -a --filter "label=com.docker.compose.project=${project_name}" --format '{{.ID}}')

if [[ ${#container_ids[@]} -eq 0 ]]; then
  printf '[]\n' > "${output_file}"
  exit 0
fi

docker inspect "${container_ids[@]}" | jq '[.[] | {
  name:(.Name | ltrimstr("/")),
  image:.Config.Image,
  imageId:.Image,
  status:.State.Status,
  exitCode:.State.ExitCode,
  oomKilled:.State.OOMKilled,
  restartCount:.RestartCount,
  cpuSet:.HostConfig.CpusetCpus,
  memoryBytes:.HostConfig.Memory,
  memorySwapBytes:.HostConfig.MemorySwap
}]' > "${output_file}"
