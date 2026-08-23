package nats_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

const (
	testContentTypeHeader    = "Content-Type"
	testReplayIDHeader       = "Gomessenger-Replay-Id"
	testReplayConsumerHeader = "Gomessenger-Replay-Consumer"
)

type replayPublisher struct {
	message *natsio.Msg
	ack     *jetstream.PubAck
	err     error
}

func (publisher *replayPublisher) PublishMsg(
	_ context.Context,
	message *natsio.Msg,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	publisher.message = message
	return publisher.ack, publisher.err
}

func TestDecodeDLQRecordStrictlyValidatesAndCanonicalizes(t *testing.T) {
	envelope, _ := testNativeEnvelope(t)
	data, err := json.Marshal(nats.DLQRecord{
		SpecVersion: nats.DLQSpecVersion, ConsumerID: testConsumerID,
		Subject: testMessageSubject,
		Attempt: 2, FailureKind: testPermanentFailure, Error: testRejectedError, FailedAt: time.Now().UTC(),
		WireMode: nats.WireNative, Envelope: envelope,
		OriginalHeaders: map[string][]string{
			testContentTypeHeader: {testNativeMediaType},
			natsio.MsgIdHdr:       {testOldBrokerID},
		},
		OriginalBase64: base64.StdEncoding.EncodeToString(envelope),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	record, err := nats.DecodeDLQRecord(data)
	if err != nil || record.ConsumerID != testConsumerID {
		t.Fatalf("decode = %#v, %v", record, err)
	}
	data[len(data)-1] = ','
	if _, err := nats.DecodeDLQRecord(data); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("invalid error = %v", err)
	}
}

func TestDLQRecordAcceptsPresentEmptyWirePayload(t *testing.T) {
	native, metadata := testNativeEnvelope(t)
	headers := make(natsio.Header)
	if _, err := nats.EncodeCloudEventBinary(native, headers); err != nil {
		t.Fatalf("encode CloudEvent headers: %v", err)
	}
	record := nats.DLQRecord{
		SpecVersion:     nats.DLQSpecVersion,
		ConsumerID:      testConsumerID,
		Subject:         testMessageSubject,
		Attempt:         1,
		FailureKind:     testPermanentFailure,
		Error:           testRejectedError,
		FailedAt:        time.Now().UTC(),
		WireMode:        nats.WireCloudEventsBinary,
		OriginalHeaders: map[string][]string(headers),
		OriginalBase64:  "",
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := nats.DecodeDLQRecord(data)
	if err != nil {
		t.Fatalf("decode empty payload: %v", err)
	}
	plan, err := nats.PlanDLQReplay(decoded)
	if err != nil || plan.MessageID != metadata.ID.String() {
		t.Fatalf("empty payload plan = %#v, %v", plan, err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("decode fields: %v", err)
	}
	delete(fields, "originalBase64")
	withoutOriginal, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal missing field: %v", err)
	}
	if _, err := nats.DecodeDLQRecord(withoutOriginal); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("missing originalBase64 error = %v", err)
	}
}

func TestDLQReplayPlanAndConfirmedPublishAreDeterministic(t *testing.T) {
	envelope, metadata := testNativeEnvelope(t)
	record := nats.DLQRecord{
		SpecVersion: nats.DLQSpecVersion,
		ConsumerID:  testConsumerID,
		Subject:     testMessageSubject,
		Attempt:     2,
		FailureKind: testPermanentFailure,
		Error:       testRejectedError,
		FailedAt:    time.Now().UTC(),
		WireMode:    nats.WireNative,
		Envelope:    envelope,
		OriginalHeaders: map[string][]string{
			testContentTypeHeader: {testNativeMediaType},
			natsio.MsgIdHdr:       {testOldBrokerID},
		},
		OriginalBase64: base64.StdEncoding.EncodeToString(envelope),
	}
	first, err := nats.PlanDLQReplay(record)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	second, err := nats.PlanDLQReplay(record)
	if err != nil || first != second || first.MessageID != metadata.ID.String() || first.ReplayID == "" {
		t.Fatalf("plans = %#v / %#v, err=%v", first, second, err)
	}
	caseVariant := record
	caseVariant.OriginalHeaders = map[string][]string{
		"content-type": {testNativeMediaType},
		"nats-msg-id":  {testOldBrokerID},
	}
	casePlan, err := nats.PlanDLQReplay(caseVariant)
	if err != nil || casePlan != first {
		t.Fatalf("case-insensitive plan = %#v, %v", casePlan, err)
	}
	caseVariant.OriginalHeaders[testContentTypeHeader] = []string{"duplicate"}
	if _, err := nats.PlanDLQReplay(caseVariant); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("duplicate header error = %v", err)
	}
	nextFailure := record
	nextFailure.FailedAt = record.FailedAt.Add(time.Nanosecond)
	nextFailurePlan, err := nats.PlanDLQReplay(nextFailure)
	if err != nil || nextFailurePlan.InputSHA256 != first.InputSHA256 || nextFailurePlan.ReplayID == first.ReplayID {
		t.Fatalf("next failure plan = %#v, %v", nextFailurePlan, err)
	}
	publisher := &replayPublisher{ack: &jetstream.PubAck{Stream: testStreamName, Sequence: 42, Duplicate: true}}
	result, err := nats.ReplayDLQ(t.Context(), publisher, record)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if result.Plan != first || result.Stream != testStreamName || result.Sequence != 42 || !result.Duplicate {
		t.Fatalf("result = %#v", result)
	}
	if publisher.message == nil || publisher.message.Subject != record.Subject ||
		string(publisher.message.Data) != string(envelope) || publisher.message.Header.Get(natsio.MsgIdHdr) != "" ||
		publisher.message.Header.Get(testReplayIDHeader) != first.ReplayID ||
		publisher.message.Header.Get(testReplayConsumerHeader) != record.ConsumerID {
		t.Fatalf("published message = %#v", publisher.message)
	}
	if _, err := nats.ReplayDLQ(t.Context(), nil, record); !errors.Is(err, nats.ErrInvalidConfig) {
		t.Fatalf("nil publisher error = %v", err)
	}
}

func TestDLQReplayDigestSeparatesEmbeddedHeaderDelimiters(t *testing.T) {
	envelope, _ := testNativeEnvelope(t)
	record := nats.DLQRecord{
		SpecVersion: nats.DLQSpecVersion, ConsumerID: testConsumerID, Subject: testMessageSubject,
		Attempt: 2, FailureKind: testPermanentFailure, Error: testRejectedError, FailedAt: time.Now().UTC(),
		WireMode: nats.WireNative, OriginalBase64: base64.StdEncoding.EncodeToString(envelope),
		OriginalHeaders: map[string][]string{
			testContentTypeHeader: {testNativeMediaType},
			"X-Values":            {"a", "b"},
		},
	}
	multipleValues, err := nats.PlanDLQReplay(record)
	if err != nil {
		t.Fatalf("plan multiple values: %v", err)
	}
	record.OriginalHeaders["X-Values"] = []string{"a\x00b"}
	embeddedDelimiter, err := nats.PlanDLQReplay(record)
	if err != nil {
		t.Fatalf("plan embedded delimiter: %v", err)
	}
	if multipleValues.InputSHA256 == embeddedDelimiter.InputSHA256 ||
		multipleValues.ReplayID == embeddedDelimiter.ReplayID {
		t.Fatalf("distinct header structures collided: %#v / %#v", multipleValues, embeddedDelimiter)
	}
}

func TestDLQReplayRejectsLegacyBinaryRecordWithoutHeaders(t *testing.T) {
	record := nats.DLQRecord{
		SpecVersion:    nats.DLQSpecVersion,
		ConsumerID:     testConsumerID,
		Subject:        testMessageSubject,
		Attempt:        1,
		FailureKind:    testPermanentFailure,
		Error:          testRejectedError,
		FailedAt:       time.Now().UTC(),
		WireMode:       nats.WireCloudEventsBinary,
		OriginalBase64: base64.StdEncoding.EncodeToString([]byte(`{"jobId":42}`)),
	}
	if _, err := nats.PlanDLQReplay(record); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("legacy binary replay error = %v", err)
	}
}
