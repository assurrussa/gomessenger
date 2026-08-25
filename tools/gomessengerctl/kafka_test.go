package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
)

const cmdKafka = "kafka"

func TestKafkaTopologyOfflineValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafka-topology.json")
	data, err := json.Marshal(testKafkaTopology())
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write topology: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{cmdKafka, "topology", cmdValidate, flagFile, path}, &stdout, &stderr); code != exitOK {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestKafkaDLQDryRunAndInspectRedactWireData(t *testing.T) {
	path, marker := writeKafkaReplayRecord(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{
		cmdKafka, cmdDLQ, cmdReplay, flagFile, path, "--brokers", "127.0.0.1:1",
	}, &stdout, &stderr); code != exitOK {
		t.Fatalf("dry-run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stdout.String(), "originalBase64") {
		t.Fatalf("dry-run exposed wire data: %s", stdout.String())
	}
	var plan kafkaadapter.ReplayPlan
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil || plan.ReplayID == "" || plan.InputSHA256 == "" {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{cmdKafka, cmdDLQ, commandInspect, flagFile, path}, &stdout, &stderr); code != exitOK {
		t.Fatalf("inspect code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, forbidden := range []string{marker, "originalBase64", "recordKeyBase64", "handler rejected"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("inspection exposed %q: stdout=%q stderr=%q", forbidden, stdout.String(), stderr.String())
		}
	}
}

func testKafkaTopology() kafkaadapter.Topology {
	const (
		source     = "orders.command.orders-create.v1"
		consumerID = "billing"
	)
	base := kafkaadapter.TopicSpec{
		Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: 2,
		RetentionMillis: 604_800_000, RetentionBytes: -1,
		MaxMessageBytes: kafkaadapter.DefaultMaxSourceMessageBytes,
	}
	sourceSpec := base
	sourceSpec.Name = source
	sourceSpec.Role = kafkaadapter.TopicRoleSource
	retry := base
	retry.Name = source + ".gm." + consumerID + ".retry.t0"
	retry.Role = kafkaadapter.TopicRoleRetry
	retry.SourceTopic = source
	retry.ConsumerID = consumerID
	retry.RetentionMillis = -1
	retry.RetentionBytes = -1
	replay := base
	replay.Name = source + ".gm." + consumerID + ".replay"
	replay.Role = kafkaadapter.TopicRoleReplay
	replay.SourceTopic = source
	replay.ConsumerID = consumerID
	dlq := base
	dlq.Name = source + ".gm." + consumerID + ".dlq"
	dlq.Role = kafkaadapter.TopicRoleDLQ
	dlq.SourceTopic = source
	dlq.ConsumerID = consumerID
	dlq.MaxMessageBytes = kafkaadapter.DefaultMaxDLQMessageBytes
	return kafkaadapter.Topology{
		SpecVersion: kafkaadapter.TopologySpecVersion,
		Topics:      []kafkaadapter.TopicSpec{sourceSpec, retry, replay, dlq},
	}
}

func writeKafkaReplayRecord(t *testing.T) (path, marker string) {
	t.Helper()
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("parse ID: %v", err)
	}
	event := messenger.MustEvent("orders-created", 1, messenger.JSON[map[string]string]())
	marker = "Kafka payload must stay hidden"
	wire, err := messenger.EncodeEventEnvelope(event, messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: event.Info().Name, SchemaVersion: 1,
		Source: "urn:service:orders", Time: time.Unix(1_700_000_000, 0).UTC(), CorrelationID: id,
		ContentType: event.Info().ContentType,
	}, map[string]string{"marker": marker})
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	record, err := json.Marshal(kafkaadapter.DLQRecord{
		SpecVersion: kafkaadapter.DLQSpecVersion, ConsumerID: "billing",
		SourceTopic: "orders.event.orders-created.v1", SourcePartition: 0, SourceOffset: 42,
		RecordKeyBase64: base64.StdEncoding.EncodeToString([]byte(id.String())),
		OriginalBase64:  base64.StdEncoding.EncodeToString(wire), MessageID: id.String(), Attempt: 3,
		AttemptGeneration: "generation-1", FailureKind: "permanent", Error: "handler rejected",
		FailedAt: time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("marshal DLQ: %v", err)
	}
	path = filepath.Join(t.TempDir(), "kafka-dlq.json")
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write DLQ: %v", err)
	}
	return path, marker
}
