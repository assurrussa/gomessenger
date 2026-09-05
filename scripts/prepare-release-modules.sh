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

require_version() {
	(cd "$1" && go mod edit -require="$2@$3")
}

drop_replace() {
	(cd "$1" && go mod edit -dropreplace="$2")
}

workspace_replace() {
	go work edit -replace="$1@${version}=$2" ./go.work
}

require_resolvable_module() {
	if ! GOWORK=off go list -m "$1@$2" >/dev/null 2>&1; then
		echo "$1@$2 does not resolve through the configured Go proxy" >&2
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

# Every prerequisite is checked before the first go.mod/go.work mutation.
for dependency in github.com/assurrussa/outbox github.com/assurrussa/outbox/backends/pgsql github.com/assurrussa/outbox/backends/sqlite; do
	require_resolvable_module "$dependency" "$outbox_version"
done
if [ "$layer" != root ]; then
	require_resolvable_module github.com/assurrussa/gomessenger "$version"
fi
case "$layer" in
	transports|final)
		require_resolvable_module github.com/assurrussa/gomessenger/adapters/inbox "$version"
		;;
esac
if [ "$layer" = final ]; then
	for dependency in adapters/outbox observability adapters/kafka adapters/nats; do
		require_resolvable_module "github.com/assurrussa/gomessenger/$dependency" "$version"
	done
fi

if [ "$layer" = root ]; then
	sh ./scripts/check-release-modules.sh "$version" "$outbox_version" root
	GOWORK=off go mod tidy
	echo "prepared root for ${version}; nested requirements remain on their current graph"
	exit 0
fi

for module in $modules; do
	require_version "$module" github.com/assurrussa/gomessenger "$version"
	drop_replace "$module" github.com/assurrussa/gomessenger
done
case "$layer" in
	modules|final)
		require_version adapters/outbox github.com/assurrussa/outbox "$outbox_version"
		drop_replace adapters/outbox github.com/assurrussa/outbox
		;;
esac
case "$layer" in
	transports|final)
		for module in adapters/kafka adapters/nats; do
			require_version "$module" github.com/assurrussa/gomessenger/adapters/inbox "$version"
			drop_replace "$module" github.com/assurrussa/gomessenger/adapters/inbox
		done
		;;
esac

if [ "$layer" = final ]; then
	for dependency in adapters/inbox adapters/kafka adapters/nats; do
		require_version tools/gomessengerctl "github.com/assurrussa/gomessenger/$dependency" "$version"
		drop_replace tools/gomessengerctl "github.com/assurrussa/gomessenger/$dependency"
	done

	# The consumer and example resolve published modules. Only the E2E fixture
	# retains GoMessenger path replacements to exercise the current checkout.
	for module in testdata/consumer testdata/e2e examples/durable-postgres-nats; do
		for dependency in github.com/assurrussa/gomessenger github.com/assurrussa/gomessenger/adapters/inbox github.com/assurrussa/gomessenger/adapters/nats github.com/assurrussa/gomessenger/adapters/outbox; do
			require_version "$module" "$dependency" "$version"
		done
		require_version "$module" github.com/assurrussa/outbox "$outbox_version"
		drop_replace "$module" github.com/assurrussa/outbox
	done
	for module in testdata/consumer testdata/e2e; do
		require_version "$module" github.com/assurrussa/gomessenger/adapters/kafka "$version"
	done
	require_version testdata/consumer github.com/assurrussa/gomessenger/observability "$version"
	require_version testdata/e2e github.com/assurrussa/outbox/backends/sqlite "$outbox_version"
	drop_replace testdata/e2e github.com/assurrussa/outbox/backends/sqlite
	require_version examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql "$outbox_version"
	drop_replace examples/durable-postgres-nats github.com/assurrussa/outbox/backends/pgsql
	for module in testdata/consumer examples/durable-postgres-nats; do
		for dependency in github.com/assurrussa/gomessenger github.com/assurrussa/gomessenger/adapters/inbox github.com/assurrussa/gomessenger/adapters/kafka github.com/assurrussa/gomessenger/adapters/nats github.com/assurrussa/gomessenger/adapters/outbox github.com/assurrussa/gomessenger/observability; do
			drop_replace "$module" "$dependency"
		done
	done
	modules="$modules examples/durable-postgres-nats testdata/consumer testdata/e2e"
fi

workspace_replace github.com/assurrussa/gomessenger .
for dependency in adapters/inbox adapters/kafka adapters/nats adapters/outbox observability; do
	workspace_replace "github.com/assurrussa/gomessenger/$dependency" "./$dependency"
done

for module in $modules; do
	(cd "$module" && GOWORK=off go mod tidy)
done
sh ./scripts/check-release-modules.sh "$version" "$outbox_version" "$layer"
echo "prepared ${layer} module requirements for ${version} with outbox ${outbox_version}"
