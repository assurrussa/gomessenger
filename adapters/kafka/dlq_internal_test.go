package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

type replayPublisherStub struct {
	record *kgo.Record
	err    error
}

func (publisher *replayPublisherStub) PublishReplay(_ context.Context, record *kgo.Record) (*kgo.Record, error) {
	publisher.record = record
	if publisher.err != nil {
		return nil, publisher.err
	}
	result := *record
	result.Partition = 2
	result.Offset = 17
	return &result, nil
}

func TestDLQRecordDecodePlanAndReplay(t *testing.T) {
	native, key := testNativeEnvelope(t)
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID,
		SourceTopic: testSourceTopic, SourcePartition: 1, SourceOffset: 9,
		RecordKeyBase64: base64.StdEncoding.EncodeToString(key),
		OriginalBase64:  base64.StdEncoding.EncodeToString(native),
		MessageID:       testMessageID, Attempt: 3,
		FailureKind: "attempts_exhausted", Error: "database unavailable",
		FailedAt: time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC),
	}
	encoded, err := encodeDLQRecord(record)
	if err != nil {
		t.Fatalf("encodeDLQRecord: %v", err)
	}
	decoded, err := DecodeDLQRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeDLQRecord: %v", err)
	}
	first, err := PlanDLQReplay(decoded)
	if err != nil {
		t.Fatalf("PlanDLQReplay: %v", err)
	}
	second, err := PlanDLQReplay(decoded)
	if err != nil || second != first || first.ReplayID == "" {
		t.Fatalf("replay plans = %#v / %#v, %v", first, second, err)
	}
	publisher := &replayPublisherStub{}
	result, err := ReplayDLQ(t.Context(), publisher, decoded)
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if result.Partition != 2 || result.Offset != 17 || publisher.record.Topic != first.Topic ||
		string(publisher.record.Value) != string(native) || string(publisher.record.Key) != string(key) {
		t.Fatalf("replay result=%#v record=%#v", result, publisher.record)
	}
	control, err := decodeControlHeaders(publisher.record.Headers)
	if err != nil || control.attemptGeneration != first.ReplayID {
		t.Fatalf("replay control = %#v, %v", control, err)
	}
}

func TestDLQRecordKeepsPresentEmptyOriginalValue(t *testing.T) {
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID, SourceTopic: testSourceTopic,
		SourcePartition: 0, SourceOffset: 0, RecordKeyBase64: "", OriginalBase64: "",
		Attempt: 1, FailureKind: "malformed", Error: "missing payload", FailedAt: time.Now().UTC(),
	}
	data, err := encodeDLQRecord(record)
	if err != nil {
		t.Fatalf("encodeDLQRecord: %v", err)
	}
	if _, err := DecodeDLQRecord(data); err != nil {
		t.Fatalf("DecodeDLQRecord: %v", err)
	}
}

func TestDecodeDLQRecordRequiresSourcePositionFields(t *testing.T) {
	t.Parallel()
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID, SourceTopic: testSourceTopic,
		SourcePartition: 0, SourceOffset: 0, RecordKeyBase64: "", OriginalBase64: "",
		Attempt: 1, FailureKind: failureKindDecode, Error: "empty", FailedAt: time.Now().UTC(),
	}
	encoded, err := encodeDLQRecord(record)
	if err != nil {
		t.Fatalf("encodeDLQRecord: %v", err)
	}
	for _, field := range []string{"sourcePartition", "sourceOffset"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			var object map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &object); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			delete(object, field)
			malformed, err := json.Marshal(object)
			if err != nil {
				t.Fatalf("encode fixture: %v", err)
			}
			if _, err := DecodeDLQRecord(malformed); !errors.Is(err, messenger.ErrInvalidMessage) {
				t.Fatalf("DecodeDLQRecord error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestDLQFailureTextRoundTripsAsBoundedUTF8(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		failure string
	}{
		{name: "split multibyte rune", failure: strings.Repeat("€", 342)},
		{name: "invalid UTF-8", failure: strings.Repeat("\xff", 1024)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := &kgo.Record{Topic: testSourceTopic, Partition: 0, Offset: 1}
			record := makeDLQRecord(
				testConsumerID,
				source,
				sourceControl(source),
				"",
				1,
				failureKindDecode,
				errors.New(test.failure),
				time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC),
			)
			encoded, err := encodeDLQRecord(record)
			if err != nil {
				t.Fatalf("encodeDLQRecord: %v", err)
			}
			decoded, err := DecodeDLQRecord(encoded)
			if err != nil {
				t.Fatalf("DecodeDLQRecord: %v", err)
			}
			if decoded.Error != record.Error || len(decoded.Error) > 1024 || !utf8.ValidString(decoded.Error) {
				t.Fatalf("decoded error length=%d valid=%v, want bounded round trip",
					len(decoded.Error), utf8.ValidString(decoded.Error))
			}
		})
	}
}

func TestReplayRejectsUndecodableOriginal(t *testing.T) {
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID, SourceTopic: testSourceTopic,
		SourcePartition: 0, SourceOffset: 1,
		RecordKeyBase64: base64.StdEncoding.EncodeToString([]byte("key")),
		OriginalBase64:  base64.StdEncoding.EncodeToString([]byte("not an envelope")),
		Attempt:         1, FailureKind: failureKindDecode, Error: "bad", FailedAt: time.Now().UTC(),
	}
	if _, err := PlanDLQReplay(record); err == nil {
		t.Fatal("invalid original record was replayable")
	}
	publisher := &replayPublisherStub{err: errors.New("must not run")}
	if _, err := ReplayDLQ(t.Context(), publisher, record); err == nil || publisher.record != nil {
		t.Fatalf("ReplayDLQ error=%v record=%#v", err, publisher.record)
	}
}

func TestReplayRejectsSourceOrKeyIdentityConflict(t *testing.T) {
	t.Parallel()
	native, key := testNativeEnvelope(t)
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID,
		SourceTopic: testSourceTopic, SourcePartition: 0, SourceOffset: 1,
		RecordKeyBase64: base64.StdEncoding.EncodeToString(key),
		OriginalBase64:  base64.StdEncoding.EncodeToString(native),
		MessageID:       testMessageID, Attempt: 1,
		FailureKind: "permanent", Error: "failed", FailedAt: time.Now().UTC(),
	}
	record.RecordKeyBase64 = base64.StdEncoding.EncodeToString([]byte("other-order"))
	if _, err := PlanDLQReplay(record); err == nil {
		t.Fatal("record with conflicting Kafka key was replayable")
	}
	record.RecordKeyBase64 = base64.StdEncoding.EncodeToString(key)
	record.SourceTopic = "prod.event.other.v1"
	if _, err := PlanDLQReplay(record); err == nil {
		t.Fatal("record with conflicting source topic was replayable")
	}
}

func TestValidateProtectedReplayRecord(t *testing.T) {
	t.Parallel()
	native, key := testNativeEnvelope(t)
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: testConsumerID,
		SourceTopic: testSourceTopic, SourcePartition: 0, SourceOffset: 1,
		RecordKeyBase64: base64.StdEncoding.EncodeToString(key),
		OriginalBase64:  base64.StdEncoding.EncodeToString(native),
		MessageID:       testMessageID, Attempt: 1,
		FailureKind: "permanent", Error: "failed", FailedAt: time.Now().UTC(),
	}
	publisher := &replayPublisherStub{}
	if _, err := ReplayDLQ(t.Context(), publisher, record); err != nil {
		t.Fatalf("build protected replay record: %v", err)
	}
	canonical, err := validateProtectedReplayRecord(publisher.record)
	if err != nil || string(canonical) != string(native) {
		t.Fatalf("validate replay = %q, %v", canonical, err)
	}
	publisher.record.Timestamp = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	if cloned := newReplayRecord(publisher.record, canonical); !cloned.Timestamp.IsZero() {
		t.Fatalf("replay timestamp = %s, want zero for producer publication time", cloned.Timestamp)
	}
	invalid := *publisher.record
	invalid.Topic = record.SourceTopic
	if _, err := validateProtectedReplayRecord(&invalid); err == nil {
		t.Fatal("ordinary source topic accepted protected replay")
	}
}

func testNativeEnvelope(t *testing.T) (native, key []byte) {
	t.Helper()
	type payload struct {
		OrderID string `json:"orderId"`
	}
	descriptor := messenger.MustEvent("orders.created", 1, messenger.JSON[payload]())
	id := mustMessageID(t, testMessageID)
	metadata := messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: descriptor.Info().Name,
		SchemaVersion: 1, Source: "urn:service:orders", Time: time.Date(2026, time.August, 25, 11, 0, 0, 0, time.UTC),
		CorrelationID: id, Key: testDomainKey, ContentType: descriptor.Info().ContentType,
	}
	native, err := messenger.EncodeEventEnvelope(descriptor, metadata, payload{OrderID: testDomainKey})
	if err != nil {
		t.Fatalf("EncodeEventEnvelope: %v", err)
	}
	return native, []byte(metadata.Key)
}
