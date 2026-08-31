package nats

import (
	"context"
	"errors"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNormalNATSBatchBoundaryIncludesCompletedPull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "typed sentinel", err: jetstream.ErrBatchCompleted, want: true},
		{name: "legacy formatted status", err: errors.New("nats: Batch Completed"), want: true},
		{name: "other server status", err: errors.New("nats: consumer deleted"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := normalNATSBatchBoundary(context.Background(), test.err); got != test.want {
				t.Fatalf("normalNATSBatchBoundary() = %t, want %t", got, test.want)
			}
		})
	}
}

const testNATSSource = "urn:test"

func TestNATSBatchDecodedMessagesMatchActiveFingerprint(t *testing.T) {
	messageID, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000087")
	if err != nil {
		t.Fatal(err)
	}
	key := inbox.Key{ConsumerID: "batch-worker", Source: testNATSSource, MessageID: messageID}
	first := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("first"))}
	active := inbox.BatchItem{Key: key, Fingerprint: inbox.FingerprintEnvelope([]byte("active"))}
	decoded, err := natsBatchDecodedMessages([]inbox.BatchItem{active}, map[inbox.BatchItem]decodedMessage{
		first:  {canonical: []byte("first")},
		active: {canonical: []byte("active")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(decoded[0].canonical); got != "active" {
		t.Fatalf("decoded payload = %q, want active fingerprint payload", got)
	}
}

func TestHasNATSBatchConflictingGeneration(t *testing.T) {
	t.Parallel()
	id, err := messenger.ParseMessageID("01991387-6880-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	existing := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-1",
	}
	deliveries := []*natsBatchDelivery{existing}

	// Same (ID, Source) and same generation -> not conflicting
	sameGenCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-1",
	}
	if hasNATSBatchConflictingGeneration(deliveries, sameGenCandidate) {
		t.Fatal("expected candidate with same attempt generation to NOT be conflicting")
	}

	// Same (ID, Source) but different generation -> conflicting!
	diffGenCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id, Source: testNATSSource},
		},
		attemptGeneration: "gen-2",
	}
	if !hasNATSBatchConflictingGeneration(deliveries, diffGenCandidate) {
		t.Fatal("expected candidate with different attempt generation to be conflicting")
	}

	// Different ID -> not conflicting
	id2, _ := messenger.ParseMessageID("01991387-6880-7000-8000-000000000002")
	diffIDCandidate := &natsBatchDelivery{
		decoded: decodedMessage{
			metadata: messenger.Metadata{ID: id2, Source: testNATSSource},
		},
		attemptGeneration: "gen-2",
	}
	if hasNATSBatchConflictingGeneration(deliveries, diffIDCandidate) {
		t.Fatal("expected candidate with different ID to NOT be conflicting")
	}
}
