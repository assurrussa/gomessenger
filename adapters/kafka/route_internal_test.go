package kafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type routeTestDelivery struct {
	metadata messenger.Metadata
	native   []byte
}

func (delivery routeTestDelivery) Metadata() messenger.Metadata { return delivery.metadata }

func (delivery routeTestDelivery) HandlerCount() int { return 0 }

func (delivery routeTestDelivery) MarshalEnvelope() ([]byte, error) {
	return append([]byte(nil), delivery.native...), nil
}

func (delivery routeTestDelivery) Fingerprint() ([sha256.Size]byte, error) {
	return sha256.Sum256(delivery.native), nil
}

func (delivery routeTestDelivery) Invoke(context.Context) error { return nil }

func TestRouteRecordLeavesKafkaTimestampForPublicationTime(t *testing.T) {
	native, key := testNativeEnvelope(t)
	envelope, err := messenger.UnmarshalEnvelope(native)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	record := newRouteRecord(testSourceTopic, envelope.Metadata(), native)
	if !record.Timestamp.IsZero() {
		t.Fatalf("Kafka timestamp = %s, want zero for producer publication time", record.Timestamp)
	}
	if !bytes.Equal(record.Key, key) {
		t.Fatalf("record key = %q, want %q", record.Key, key)
	}
	native[0] ^= 0xff
	if bytes.Equal(record.Value, native) {
		t.Fatal("route record retained the caller's mutable envelope buffer")
	}
}

func TestDecodeDeliveryEnvelopeUsesSerializedMetadataForRecordKey(t *testing.T) {
	native, _ := testNativeEnvelope(t)
	envelope, err := messenger.UnmarshalEnvelope(native)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	payload, encoding, err := envelope.Payload()
	if err != nil {
		t.Fatalf("Payload: %v", err)
	}
	metadata := envelope.Metadata()
	metadata.Key = string([]byte{0xff})
	native, err = messenger.MarshalEnvelope(metadata, payload, encoding)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}

	decoded, encoded, err := decodeDeliveryEnvelope(routeTestDelivery{metadata: metadata, native: native})
	if err != nil {
		t.Fatalf("decodeDeliveryEnvelope: %v", err)
	}
	record := newRouteRecord(testSourceTopic, decoded, encoded)
	wireEnvelope, err := messenger.UnmarshalEnvelope(record.Value)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope record: %v", err)
	}
	if err := validateRecordKey(record.Key, wireEnvelope.Metadata()); err != nil {
		t.Fatalf("validateRecordKey: %v", err)
	}
	if bytes.Equal(record.Key, []byte(metadata.Key)) {
		t.Fatal("record key retained non-canonical invalid UTF-8 bytes")
	}
}

func TestRouteRechecksExpiryAfterTransactionAdmission(t *testing.T) {
	initial := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	var clockNanos atomic.Int64
	clockNanos.Store(initial.UnixNano())
	firstClock := make(chan struct{})
	var clockCalls atomic.Int32
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	transport := &Transport{state: transportRunning, txGate: gate}
	route := &Route{transport: transport, namespace: testNamespace}
	route.clock = func() time.Time {
		now := time.Unix(0, clockNanos.Load()).UTC()
		if clockCalls.Add(1) == 1 {
			close(firstClock)
		}
		return now
	}
	metadata := messenger.Metadata{
		Kind: messenger.KindEvent, Name: "order-created", SchemaVersion: 1,
		ExpiresAt: initial.Add(time.Second),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := route.publishEnvelope(ctx, metadata, nil)
		result <- err
	}()

	select {
	case <-firstClock:
	case <-time.After(time.Second):
		t.Fatal("route did not check initial expiry")
	}
	clockNanos.Store(initial.Add(2 * time.Second).UnixNano())
	<-gate
	select {
	case err := <-result:
		if !errors.Is(err, ErrMessageExpired) || !messenger.IsPermanent(err) {
			t.Fatalf("publish after admission expiry error = %v, want permanent ErrMessageExpired", err)
		}
	case <-time.After(time.Second):
		t.Fatal("route did not reject expiry after transaction admission")
	}
}

func TestRouteTopicDerivationFailureIsPermanent(t *testing.T) {
	route := &Route{
		transport: &Transport{state: transportRunning},
		namespace: testNamespace,
		clock:     time.Now,
	}
	_, err := route.publishEnvelope(t.Context(), messenger.Metadata{
		Kind: messenger.KindEvent, Name: "order_created", SchemaVersion: 1,
	}, nil)
	if !errors.Is(err, ErrInvalidConfig) || !messenger.IsPermanent(err) {
		t.Fatalf("topic derivation error = %v, want permanent ErrInvalidConfig", err)
	}
}
