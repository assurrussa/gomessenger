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

requirement_version() {
	module="$1"
	dependency="$2"
	(cd "$module" && go mod edit -json) | awk -v dependency="$dependency" '
		/"Path":/ {
			path = $2
			gsub(/[",]/, "", path)
		}
		/"Version":/ && path == dependency {
			version = $2
			gsub(/[",]/, "", version)
			print version
			exit
		}
	'
}

check_requirement() {
	module="$1"
	dependency="$2"
	wanted="$3"
	actual="$(requirement_version "$module" "$dependency")"
	if [ "$actual" != "$wanted" ]; then
		echo "$module requires $dependency@$actual, expected $wanted" >&2
		exit 1
	fi
}

check_no_replace() {
	module="$1"
	if (cd "$module" && go mod edit -json) | grep -q '"Replace": \['; then
		echo "$module/go.mod contains a development replace directive" >&2
		exit 1
	fi
}

check_dependency_no_replace() {
	module="$1"
	dependency="$2"
	if (cd "$module" && go mod edit -json) | awk -v dependency="$dependency" '
		/"Old": \{/ {
			in_old = 1
			next
		}
		in_old && /"Path":/ {
			path = $2
			gsub(/[",]/, "", path)
			if (path == dependency) {
				found = 1
			}
			in_old = 0
		}
		END { exit found ? 0 : 1 }
	'; then
		echo "$module/go.mod replaces published dependency $dependency" >&2
		exit 1
	fi
}

validate_version "$version" VERSION
validate_version "$outbox_version" OUTBOX_VERSION

for module in adapters/inbox adapters/nats adapters/outbox observability tools/gomessengerctl; do
	check_no_replace "$module"
done

check_dependency_no_replace testdata/consumer github.com/assurrussa/outbox
check_dependency_no_replace testdata/e2e github.com/assurrussa/outbox
check_dependency_no_replace testdata/e2e github.com/assurrussa/outbox/backends/sqlite

check_requirement adapters/inbox github.com/assurrussa/gomessenger "$version"
check_requirement adapters/nats github.com/assurrussa/gomessenger "$version"
check_requirement adapters/nats github.com/assurrussa/gomessenger/adapters/inbox "$version"
check_requirement adapters/outbox github.com/assurrussa/gomessenger "$version"
check_requirement adapters/outbox github.com/assurrussa/outbox "$outbox_version"
check_requirement observability github.com/assurrussa/gomessenger "$version"
check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger "$version"
check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/inbox "$version"
check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/nats "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/inbox "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/nats "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/outbox "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/observability "$version"
check_requirement testdata/consumer github.com/assurrussa/outbox "$outbox_version"
check_requirement testdata/e2e github.com/assurrussa/outbox "$outbox_version"
check_requirement testdata/e2e github.com/assurrussa/outbox/backends/sqlite "$outbox_version"
