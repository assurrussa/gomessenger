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
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	flagServer        = "--server"
	flagFile          = "--file"
	cmdValidate       = "validate"
	cmdDLQ            = "dlq"
	cmdReplay         = "replay"
	testStream        = "MESSAGES"
	testReplaySubject = "test.event.media.processed.v1"
	headerContentType = "Content-Type"
)

func TestManifestAndTopologyOfflineValidation(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "manifest.json")
	manifest := `{
		"specVersion":"1.0",
		"source":"urn:service:test",
		"descriptors":[{
			"kind":"event",
			"name":"media.processed",
			"schemaVersion":1,
			"contentType":"application/json",
			"dataEncoding":"json",
			"route":"nats.events"
		}]
	}`
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"manifest", cmdValidate, flagFile, manifestPath}, &stdout, &stderr); code != exitOK {
		t.Fatalf("manifest code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	topologyPath := filepath.Join(directory, "topology.json")
	topology, err := json.Marshal(natsadapter.Topology{
		SpecVersion: natsadapter.TopologySpecVersion,
		Streams:     []natsadapter.StreamSpec{natsadapter.DevStream("MESSAGES", "test.>")},
	})
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	if err := os.WriteFile(topologyPath, topology, 0o600); err != nil {
		t.Fatalf("write topology: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"topology", cmdValidate, flagFile, topologyPath}, &stdout, &stderr); code != exitOK {
		t.Fatalf("topology code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestStrictJSONRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	invalidJSON := `{"specVersion":"1.0","source":"urn:service:test","unknown":true}`
	if err := os.WriteFile(path, []byte(invalidJSON), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"manifest", cmdValidate, flagFile, path}, &stdout, &stderr); code != exitFailure {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDLQReplayDryRunDoesNotConnectOrExposeWireData(t *testing.T) {
	path, marker := writeReplayRecord(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{
		cmdDLQ, cmdReplay, flagFile, path,
		flagServer, "nats://127.0.0.1:1", "--timeout", "1ms",
	}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var plan natsadapter.ReplayPlan
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if plan.Subject != testReplaySubject || plan.ReplayID == "" || plan.InputSHA256 == "" {
		t.Fatalf("plan = %#v", plan)
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stdout.String(), "originalBase64") ||
		strings.Contains(stdout.String(), "originalHeaders") {
		t.Fatalf("dry-run exposed wire data: %s", stdout.String())
	}
}

func TestDLQInspectDoesNotExposePayloadHeadersOrHandlerError(t *testing.T) {
	path, marker := writeReplayRecord(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{cmdDLQ, commandInspect, flagFile, path}, &stdout, &stderr); code != exitOK {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, forbidden := range []string{marker, "originalBase64", "originalHeaders", headerContentType, "rejected"} {
		if strings.Contains(stdout.String(), forbidden) {
			t.Fatalf("inspection exposed %q: %s", forbidden, stdout.String())
		}
	}
	var inspection map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &inspection); err != nil {
		t.Fatalf("decode inspection: %v", err)
	}
	replayable, ok := inspection["replayable"].(bool)
	if !ok || !replayable || inspection["originalBytes"] == float64(0) ||
		inspection["originalHeaderCount"] != float64(1) {
		t.Fatalf("inspection = %#v", inspection)
	}
}

func TestDLQInspectAndReplayRedactWireValidationDetails(t *testing.T) {
	marker := "private-time-value"
	record, err := json.Marshal(natsadapter.DLQRecord{
		SpecVersion: natsadapter.DLQSpecVersion,
		ConsumerID:  "media-worker",
		Subject:     testReplaySubject,
		Attempt:     1,
		FailureKind: "decode",
		Error:       "failed",
		FailedAt:    time.Unix(1_700_000_001, 0).UTC(),
		WireMode:    natsadapter.WireCloudEventsBinary,
		OriginalHeaders: map[string][]string{
			"Ce-Specversion":   {"1.0"},
			"Ce-Id":            {"018f4f2c-4a00-7000-8000-000000000001"},
			"Ce-Source":        {"urn:service:test"},
			"Ce-Type":          {"media.processed"},
			"Ce-Schemaversion": {"1"},
			"Ce-Time":          {marker},
			headerContentType:  {"application/json"},
		},
		OriginalBase64: base64.StdEncoding.EncodeToString([]byte(`{}`)),
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	path := filepath.Join(t.TempDir(), "invalid-replay.json")
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{cmdDLQ, commandInspect, flagFile, path}, &stdout, &stderr); code != exitOK {
		t.Fatalf("inspect code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stderr.String(), marker) {
		t.Fatalf("inspection exposed wire value: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	var inspection map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &inspection); err != nil {
		t.Fatalf("decode inspection: %v", err)
	}
	replayable, ok := inspection["replayable"].(bool)
	if inspection["replayError"] != dlqReplayUnavailable || !ok || replayable {
		t.Fatalf("inspection = %#v", inspection)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{cmdDLQ, cmdReplay, flagFile, path}, &stdout, &stderr); code != exitFailure {
		t.Fatalf("replay code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), marker) || !strings.Contains(stderr.String(), dlqReplayUnavailable) {
		t.Fatalf("replay validation output = %q", stderr.String())
	}
}

func TestDLQReplayConfirmedWaitsForPubAckAndDeduplicates(t *testing.T) {
	path, _ := writeReplayRecord(t)
	instance, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	instance.Start()
	if !instance.ReadyForConnections(10 * time.Second) {
		instance.Shutdown()
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() {
		instance.Shutdown()
		instance.WaitForShutdown()
	})
	connection, err := natsio.Connect(instance.ClientURL())
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: testStream, Subjects: []string{"test.>"}, Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	var firstOutput, secondOutput, stderr bytes.Buffer
	args := []string{cmdDLQ, cmdReplay, flagFile, path, "--confirm", flagServer, instance.ClientURL()}
	if code := run(args, &firstOutput, &stderr); code != exitOK {
		t.Fatalf("first code=%d stdout=%q stderr=%q", code, firstOutput.String(), stderr.String())
	}
	stderr.Reset()
	if code := run(args, &secondOutput, &stderr); code != exitOK {
		t.Fatalf("second code=%d stdout=%q stderr=%q", code, secondOutput.String(), stderr.String())
	}
	var first, second natsadapter.ReplayResult
	if err := json.Unmarshal(firstOutput.Bytes(), &first); err != nil {
		t.Fatalf("decode first result: %v", err)
	}
	if err := json.Unmarshal(secondOutput.Bytes(), &second); err != nil {
		t.Fatalf("decode second result: %v", err)
	}
	if first.Duplicate || !second.Duplicate || first.Plan.ReplayID != second.Plan.ReplayID ||
		first.Stream != testStream || second.Stream != testStream {
		t.Fatalf("replay results = %#v / %#v", first, second)
	}
}

func writeReplayRecord(t *testing.T) (path, marker string) {
	t.Helper()
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("parse message ID: %v", err)
	}
	event := messenger.MustEvent("media.processed", 1, messenger.JSON[map[string]string]())
	marker = "payload-must-stay-hidden"
	wire, err := messenger.EncodeEventEnvelope(event, messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: event.Info().Name, SchemaVersion: 1,
		Source: "urn:service:test", Time: time.Unix(1_700_000_000, 0).UTC(), CorrelationID: id,
		ContentType: event.Info().ContentType,
	}, map[string]string{"marker": marker})
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	record, err := json.Marshal(natsadapter.DLQRecord{
		SpecVersion: natsadapter.DLQSpecVersion, ConsumerID: "media-worker",
		Subject: testReplaySubject, Attempt: 1, FailureKind: "permanent",
		Error: "rejected", FailedAt: time.Unix(1_700_000_001, 0).UTC(), WireMode: natsadapter.WireNative,
		Envelope: wire, OriginalHeaders: map[string][]string{
			headerContentType: {"application/vnd.gomessenger+json; version=1.0"},
		},
		OriginalBase64: base64.StdEncoding.EncodeToString(wire),
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	path = filepath.Join(t.TempDir(), "record.json")
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}
	return path, marker
}

func TestQuarantineInspectionAndReplayRejectBeforeConnection(t *testing.T) {
	record := natsadapter.DLQRecord{
		SpecVersion: natsadapter.QuarantineSpecVersion, ConsumerID: "quarantine-worker",
		Subject: testReplaySubject, Attempt: 1, FailureKind: "decode", Error: "private failure",
		FailedAt: time.Unix(1700000001, 0).UTC(), WireMode: natsadapter.WireNative,
		Quarantine: &natsadapter.QuarantineInfo{
			Reason:      "record_too_large",
			InputSHA256: strings.Repeat("a", 64), OriginalBytes: 2000000, HeaderCount: 65,
			HeaderBytes: 500, OriginalOmitted: true,
		},
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "quarantine.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{cmdDLQ, commandInspect, flagFile, path}, &stdout, &stderr); code != exitOK {
		t.Fatalf("inspect: %d %s", code, stderr.String())
	}
	var inspection dlqInspection
	if err := json.Unmarshal(stdout.Bytes(), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Quarantine == nil || *inspection.Quarantine != *record.Quarantine || inspection.Replayable ||
		inspection.OriginalBytes != 2000000 || inspection.ReplayError != natsadapter.ErrQuarantineReplay.Error() {
		t.Fatalf("inspection: %+v", inspection)
	}
	for _, confirm := range []bool{false, true} {
		stdout.Reset()
		stderr.Reset()
		args := []string{cmdDLQ, cmdReplay, flagFile, path, flagServer, "://invalid-before-network"}
		if confirm {
			args = append(args, "--confirm")
		}
		if code := run(args, &stdout, &stderr); code != exitFailure ||
			!strings.Contains(stderr.String(), natsadapter.ErrQuarantineReplay.Error()) {
			t.Fatalf("replay confirm=%t: %d %s", confirm, code, stderr.String())
		}
	}
}
