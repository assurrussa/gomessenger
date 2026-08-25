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

validate_version "$version" VERSION
validate_version "$outbox_version" OUTBOX_VERSION

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

workspace_replace github.com/assurrussa/gomessenger "$version" .
workspace_replace github.com/assurrussa/gomessenger/adapters/inbox "$version" ./adapters/inbox
workspace_replace github.com/assurrussa/gomessenger/adapters/kafka "$version" ./adapters/kafka
workspace_replace github.com/assurrussa/gomessenger/adapters/nats "$version" ./adapters/nats
workspace_replace github.com/assurrussa/gomessenger/adapters/outbox "$version" ./adapters/outbox
workspace_replace github.com/assurrussa/gomessenger/observability "$version" ./observability

(cd testdata/consumer && GOWORK=off go mod tidy)

echo "prepared module requirements for ${version} with outbox ${outbox_version}"
