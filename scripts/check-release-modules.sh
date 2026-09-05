#!/bin/sh
set -eu

version="${1:?GoMessenger version is required}"
outbox_version="${2:?outbox version is required}"
layer="${3:-final}"

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

case "$layer" in
	root) modules="" ;;
	modules) modules="adapters/inbox adapters/outbox observability" ;;
	transports) modules="adapters/kafka adapters/nats" ;;
	final) modules="adapters/inbox adapters/outbox observability adapters/kafka adapters/nats tools/gomessengerctl" ;;
	*)
		echo "RELEASE_LAYER must be root, modules, transports, or final" >&2
		exit 2
		;;
esac

check_no_replace .
for module in $modules; do
	check_no_replace "$module"
	check_requirement "$module" github.com/assurrussa/gomessenger "$version"
done

case "$layer" in
	modules|final)
		check_requirement adapters/outbox github.com/assurrussa/outbox "$outbox_version"
		;;
esac
case "$layer" in
	transports|final)
		check_requirement adapters/kafka github.com/assurrussa/gomessenger/adapters/inbox "$version"
		check_requirement adapters/nats github.com/assurrussa/gomessenger/adapters/inbox "$version"
		;;
esac
if [ "$layer" != final ]; then
	exit 0
fi

check_no_replace testdata/consumer
check_no_replace examples/durable-postgres-nats
check_dependency_no_replace testdata/consumer github.com/assurrussa/outbox
check_dependency_no_replace testdata/e2e github.com/assurrussa/outbox
check_dependency_no_replace testdata/e2e github.com/assurrussa/outbox/backends/sqlite
check_dependency_no_replace examples/durable-postgres-nats github.com/assurrussa/outbox
check_dependency_no_replace examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql

check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/inbox "$version"
check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/kafka "$version"
check_requirement tools/gomessengerctl github.com/assurrussa/gomessenger/adapters/nats "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/inbox "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/kafka "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/nats "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/adapters/outbox "$version"
check_requirement testdata/consumer github.com/assurrussa/gomessenger/observability "$version"
check_requirement testdata/consumer github.com/assurrussa/outbox "$outbox_version"
for module in testdata/e2e examples/durable-postgres-nats; do
	check_requirement "$module" github.com/assurrussa/gomessenger "$version"
	check_requirement "$module" github.com/assurrussa/gomessenger/adapters/inbox "$version"
	check_requirement "$module" github.com/assurrussa/gomessenger/adapters/nats "$version"
	check_requirement "$module" github.com/assurrussa/gomessenger/adapters/outbox "$version"
done
check_requirement testdata/e2e github.com/assurrussa/gomessenger/adapters/kafka "$version"
check_requirement testdata/e2e github.com/assurrussa/outbox "$outbox_version"
check_requirement testdata/e2e github.com/assurrussa/outbox/backends/sqlite "$outbox_version"
check_requirement examples/durable-postgres-nats github.com/assurrussa/outbox "$outbox_version"
check_requirement examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql "$outbox_version"
