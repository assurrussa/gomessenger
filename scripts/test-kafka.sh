#!/bin/sh
set -eu

versions="${KAFKA_VERSIONS:-4.1.2 4.3.1}"
port="${KAFKA_PORT:-19092}"

if ! command -v docker >/dev/null 2>&1; then
	echo "docker is required for make test-kafka" >&2
	exit 2
fi

container=""
cleanup() {
	if [ -n "$container" ]; then
		docker rm -f "$container" >/dev/null 2>&1 || true
		container=""
	fi
}
trap cleanup EXIT INT TERM

for version in $versions; do
	container="gomessenger-kafka-${version}-$$"
	echo "starting apache/kafka:${version} on 127.0.0.1:${port}"
	docker run --rm --detach \
		--name "$container" \
		--publish "127.0.0.1:${port}:9092" \
		--env KAFKA_NODE_ID=1 \
		--env KAFKA_PROCESS_ROLES=broker,controller \
		--env KAFKA_LISTENERS=PLAINTEXT://:29092,PLAINTEXT_HOST://:9092,CONTROLLER://:9093 \
		--env "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://127.0.0.1:${port}" \
		--env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		--env KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
		--env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
		--env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
		--env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		--env KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1 \
		--env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		--env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		--env KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS=1 \
		--env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
		--env KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
		"apache/kafka:${version}" >/dev/null

	ready=0
	attempt=0
	while [ "$attempt" -lt 90 ]; do
		if docker exec "$container" /opt/kafka/bin/kafka-broker-api-versions.sh \
			--bootstrap-server localhost:29092 >/dev/null 2>&1; then
			ready=1
			break
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	if [ "$ready" -ne 1 ]; then
		docker logs "$container" >&2 || true
		echo "Kafka ${version} did not become ready" >&2
		exit 1
	fi

	if ! (
		cd testdata/e2e
		GOWORK=off \
			GOMESSENGER_KAFKA_BROKERS="127.0.0.1:${port}" \
			GOMESSENGER_KAFKA_VERSION="$version" \
			go test -race -count=1 -run '^TestKafka(Batch)?Pipeline$' ./...
	); then
		docker logs --tail 300 "$container" >&2 || true
		exit 1
	fi
	cleanup
done
