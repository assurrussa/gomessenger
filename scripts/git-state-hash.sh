#!/usr/bin/env bash

set -euo pipefail

repo_root="${1:?repository root is required}"

if command -v shasum >/dev/null 2>&1; then
  digest=(shasum -a 256)
elif command -v sha256sum >/dev/null 2>&1; then
  digest=(sha256sum)
else
  printf 'neither shasum nor sha256sum is available\n' >&2
  exit 2
fi

{
  git -C "${repo_root}" diff --binary --no-ext-diff HEAD --
  while IFS= read -r -d '' path; do
    printf 'untracked:%s\0' "${path}"
    "${digest[@]}" "${repo_root}/${path}"
  done < <(git -C "${repo_root}" ls-files --others --exclude-standard -z | LC_ALL=C sort -z)
} | "${digest[@]}" | awk '{print $1}'
