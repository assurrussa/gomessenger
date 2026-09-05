package nats

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	testQuarantineFailure = "decode"
	testQuarantineHeader  = "Content-Type"
)

func TestQuarantineCaptureAndOmission(t *testing.T) {
	headers := make(map[string][]string)
	for i := range 65 {
		headers[fmt.Sprintf("X-Header-%02d", i)] = []string{"value"}
	}
	for _, test := range []struct {
		name    string
		data    []byte
		omitted bool
	}{
		{name: "complete", data: []byte("broken JSON")},
		{name: "empty"},
		{name: "oversized", data: bytes.Repeat([]byte("x"), DefaultMaxDLQRecordBytes), omitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := quarantineTestRecord()
			prepared, err := prepareDLQ(record, headers, test.data)
			if err != nil {
				t.Fatal(err)
			}
			record, err = DecodeDLQRecord(prepared.data)
			if err != nil {
				t.Fatal(err)
			}
			capture := record.Quarantine
			if record.SpecVersion != QuarantineSpecVersion || capture == nil || capture.Replayable ||
				capture.OriginalOmitted != test.omitted || capture.HeaderCount != 65 || capture.OriginalBytes != len(test.data) {
				t.Fatalf("unexpected capture: %+v", capture)
			}
			if _, err := PlanDLQReplay(record); !errors.Is(err, ErrQuarantineReplay) {
				t.Fatalf("replay error: %v", err)
			}
			if !test.omitted {
				record.OriginalHeaders["X-Header-00"] = []string{"tampered"}
				if err := validateDLQRecord(record); err == nil {
					t.Fatal("accepted tampered capture")
				}
			}
		})
	}
}

func quarantineTestRecord() DLQRecord {
	return DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: "quarantine-worker", Subject: "test.quarantine",
		Attempt: 1, FailureKind: testQuarantineFailure, Error: "malformed test input", FailedAt: time.Now(), WireMode: WireNative,
	}
}

func TestQuarantineDigestPreservesAmbiguousHeaders(t *testing.T) {
	headers := map[string][]string{"X": {"upper"}, "x": {"lower"}, "Invalid": {string([]byte{255})}}
	want := quarantineDigest("source", WireNative, headers, []byte("body"))
	for range 50 {
		if got := quarantineDigest("source", WireNative, headers, []byte("body")); got != want {
			t.Fatal("unstable digest")
		}
	}
	prepared, err := prepareDLQ(quarantineTestRecord(), headers, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeDLQRecord(prepared.data)
	if err != nil || record.Quarantine == nil || !record.Quarantine.OriginalOmitted {
		t.Fatalf("lossy capture: %+v, %v", record, err)
	}
}

func TestQuarantineInvalidInternalRecordFailsClosed(t *testing.T) {
	record := quarantineTestRecord()
	record.FailedAt = time.Time{}
	if _, err := prepareDLQ(record, nil, nil); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("prepare: %v", err)
	}
}

func FuzzQuarantineCapture(f *testing.F) {
	f.Add("header", "value", []byte("body"))
	f.Add("X", string([]byte{255}), []byte{})
	f.Fuzz(func(t *testing.T, key, value string, data []byte) {
		if len(key)+len(value)+len(data) > DefaultMaxDLQRecordBytes {
			t.Skip()
		}
		prepared, err := prepareQuarantine(quarantineTestRecord(), map[string][]string{key: {value}}, data, "headers_unreplayable")
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeDLQRecord(prepared.data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := PlanDLQReplay(decoded); !errors.Is(err, ErrQuarantineReplay) {
			t.Fatalf("replay: %v", err)
		}
	})
}

func TestQuarantinePreparationFailureStopsConsumer(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch_%t", batch), func(t *testing.T) {
			conn, cleanup := startInternalNATSServer(t)
			defer cleanup()
			command := messenger.MustCommand("quarantine.fatal", 1, messenger.JSON[string]())
			subject, err := Subject("quarantine", command.Info())
			if err != nil {
				t.Fatal(err)
			}
			config := HandlerConfig{
				Stream: "QUARANTINE_FATAL", Namespace: "quarantine", ConsumerID: "quarantine-fatal",
				WireMode: WireNative, Concurrency: 1, Timeout: time.Second, MaxAttempts: 1,
				AckWait: time.Second, DLQSubject: "quarantine.dlq", MemoryStorage: true, Replicas: 1,
			}
			if _, err := ApplyTopology(t.Context(), conn, Topology{
				SpecVersion: TopologySpecVersion,
				Streams:     []StreamSpec{DevStream(config.Stream, subject), DevDLQStream("QUARANTINE_FATAL_DLQ", config.DLQSubject)},
			}); err != nil {
				t.Fatal(err)
			}
			store, err := inbox.New(&testNATSBatchBackend{})
			if err != nil {
				t.Fatal(err)
			}
			var consumer *Consumer
			if batch {
				consumer, err = NewBatchCommandConsumer(conn, store, command,
					func(context.Context, []messenger.Message[string]) (messenger.BatchResult, error) {
						return messenger.BatchResult{}, errors.New("unexpected handler")
					}, config, messenger.BatchConfig{MaxMessages: 1})
			} else {
				consumer, err = NewCommandConsumer(conn, store, command,
					func(context.Context, messenger.Message[string]) error { return errors.New("unexpected handler") }, config)
			}
			if err != nil {
				t.Fatal(err)
			}
			consumer.clock = func() time.Time { return time.Time{} } // Invalid internal timestamp prevents even a minimal terminal record.
			js, err := jetstream.New(conn)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := js.Publish(t.Context(), subject, []byte("malformed")); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := consumer.Run(ctx); !errors.Is(err, messenger.ErrInvalidMessage) {
				t.Fatalf("Run: %v", err)
			}
			if err := consumer.Readiness(t.Context()); !errors.Is(err, messenger.ErrRuntimeNotRunning) {
				t.Fatalf("readiness after fatal preparation: %v", err)
			}
		})
	}
}

func TestV1DLQDigestCompatibility(t *testing.T) {
	record := DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: "worker", Subject: "messages.one",
		Attempt: 1, FailureKind: testQuarantineFailure, Error: "invalid",
		FailedAt: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), WireMode: WireNative,
	}
	headers := map[string][]string{testQuarantineHeader: {"application/json"}}
	data := []byte(`{"value":1}`)
	prepared, err := prepareDLQ(record, headers, data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDLQRecord(prepared.data)
	if err != nil || decoded.SpecVersion != DLQSpecVersion || decoded.Quarantine != nil {
		t.Fatalf("v1: %+v %v", decoded, err)
	}
	input := replayDigest(record.Subject, record.WireMode, headers, data)
	replay := replayRecordDigest(record, input)
	if hex.EncodeToString(input[:]) != "1ab8f973f09c7196336d1afb7a3817cf98d342b1451aa72995a35e01ed2fb67d" ||
		hex.EncodeToString(replay[:]) != "d86eec3752c27fd50d07102f148d491409b00b24032dfc01dc5cd6e8f0f4c892" ||
		prepared.dedupID != "gm-dlq-792f026593419dd9fafb6580bcd89464da715d73f72628cbf9fda74e6fe9a489" {
		t.Fatal("v1 source, replay, or DLQ identity changed")
	}
}
