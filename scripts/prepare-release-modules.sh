#!/bin/sh
set -eu

version="${1:?GoMessenger version is required}"
outbox_version="${2:?outbox version is required}"

validate_version() {
	if ! printf '%s\n' "$1" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
		echo "$2 must be an exact vX.Y.Z tag" >&2
		exit 2
	fi
}

require_version() {
	module="$1"
	dependency="$2"
	wanted="$3"
	(cd "$module" && go mod edit -require="${dependency}@${wanted}")
}

drop_replace() {
	module="$1"
	dependency="$2"
	(cd "$module" && go mod edit -dropreplace="$dependency")
}

workspace_replace() {
	dependency="$1"
	wanted="$2"
	local_path="$3"
	go work edit -replace="${dependency}@${wanted}=${local_path}"
}

require_resolvable_module() {
	dependency="$1"
	wanted="$2"
	if ! GOWORK=off go list -m "${dependency}@${wanted}" >/dev/null 2>&1; then
		echo "${dependency}@${wanted} does not resolve through the configured Go proxy" >&2
		exit 1
	fi
}

validate_version "$version" VERSION
validate_version "$outbox_version" OUTBOX_VERSION

# Resolve every dependency required by the final layer before mutating any
# go.mod file. Dependency-layer publication is documented in docs/release.md.
require_resolvable_module github.com/assurrussa/gomessenger "$version"
require_resolvable_module github.com/assurrussa/gomessenger/adapters/inbox "$version"
require_resolvable_module github.com/assurrussa/gomessenger/adapters/kafka "$version"
require_resolvable_module github.com/assurrussa/gomessenger/adapters/nats "$version"
require_resolvable_module github.com/assurrussa/outbox "$outbox_version"
require_resolvable_module github.com/assurrussa/outbox/backends/pgsql "$outbox_version"
require_resolvable_module github.com/assurrussa/outbox/backends/sqlite "$outbox_version"

require_version adapters/inbox github.com/assurrussa/gomessenger "$version"
require_version adapters/kafka github.com/assurrussa/gomessenger "$version"
require_version adapters/kafka github.com/assurrussa/gomessenger/adapters/inbox "$version"
require_version adapters/nats github.com/assurrussa/gomessenger "$version"
require_version adapters/nats github.com/assurrussa/gomessenger/adapters/inbox "$version"
require_version adapters/outbox github.com/assurrussa/gomessenger "$version"
require_version adapters/outbox github.com/assurrussa/outbox "$outbox_version"
require_version observability github.com/assurrussa/gomessenger "$version"
require_version tools/gomessengerctl github.com/assurrussa/gomessenger "$version"
require_version tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/inbox "$version"
require_version tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/kafka "$version"
require_version tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/nats "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger/adapters/inbox "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger/adapters/kafka "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger/adapters/nats "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger/adapters/outbox "$version"
require_version testdata/consumer github.com/assurrussa/gomessenger/observability "$version"
require_version testdata/consumer github.com/assurrussa/outbox "$outbox_version"
require_version testdata/e2e github.com/assurrussa/outbox "$outbox_version"
require_version testdata/e2e github.com/assurrussa/outbox/backends/sqlite "$outbox_version"
require_version examples/durable-postgres-nats github.com/assurrussa/outbox "$outbox_version"
require_version examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql "$outbox_version"

drop_replace adapters/inbox github.com/assurrussa/gomessenger
drop_replace adapters/kafka github.com/assurrussa/gomessenger
drop_replace adapters/kafka github.com/assurrussa/gomessenger/adapters/inbox
drop_replace adapters/nats github.com/assurrussa/gomessenger
drop_replace adapters/nats github.com/assurrussa/gomessenger/adapters/inbox
drop_replace adapters/outbox github.com/assurrussa/gomessenger
drop_replace adapters/outbox github.com/assurrussa/outbox
drop_replace observability github.com/assurrussa/gomessenger
drop_replace tools/gomessengerctl github.com/assurrussa/gomessenger
drop_replace tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/inbox
drop_replace tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/kafka
drop_replace tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/nats
drop_replace examples/durable-postgres-nats github.com/assurrussa/outbox
drop_replace examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql

workspace_replace github.com/assurrussa/gomessenger "$version" .
workspace_replace github.com/assurrussa/gomessenger/adapters/inbox "$version" ./adapters/inbox
workspace_replace github.com/assurrussa/gomessenger/adapters/kafka "$version" ./adapters/kafka
workspace_replace github.com/assurrussa/gomessenger/adapters/nats "$version" ./adapters/nats
workspace_replace github.com/assurrussa/gomessenger/adapters/outbox "$version" ./adapters/outbox
workspace_replace github.com/assurrussa/gomessenger/observability "$version" ./observability

for module in adapters/outbox tools/gomessengerctl examples/durable-postgres-nats testdata/consumer testdata/e2e; do
	(cd "$module" && GOWORK=off go mod tidy)
done

echo "prepared module requirements for ${version} with outbox ${outbox_version}"
