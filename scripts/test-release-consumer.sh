#!/bin/sh
set -eu

version="${1:?version is required}"
if ! printf '%s\n' "$version" | LC_ALL=C grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
	echo "version must be an exact vX.Y.Z tag" >&2
	exit 2
fi

script_dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT HUP INT TERM
cd "$workdir"
go mod init example.com/gomessenger-release-consumer
go get "github.com/assurrussa/gomessenger@${version}"
go get "github.com/assurrussa/gomessenger/adapters/inbox@${version}"
go get "github.com/assurrussa/gomessenger/adapters/nats@${version}"
go get "github.com/assurrussa/gomessenger/adapters/outbox@${version}"
go get "github.com/assurrussa/gomessenger/observability@${version}"
mkdir -p "$workdir/bin"
GOBIN="$workdir/bin" go install "github.com/assurrussa/gomessenger/tools/gomessengerctl@${version}"
cp "$script_dir/../testdata/releaseconsumer/consumer_test.go" ./consumer_test.go
go mod tidy
go test ./...
