package inbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

const (
	testInboxConsumerID = "worker"
	testInboxSource     = "urn:test"
)

type fakeBackend struct {
	process func(context.Context, inbox.Key, inbox.Fingerprint, inbox.Handler) (inbox.Result, error)
	prune   func(context.Context, time.Time, int) (int64, error)
}

func (b fakeBackend) Process(
	ctx context.Context,
	key inbox.Key,
	fingerprint inbox.Fingerprint,
	handler inbox.Handler,
) (inbox.Result, error) {
	return b.process(ctx, key, fingerprint, handler)
}

func (b fakeBackend) Prune(ctx context.Context, before time.Time, limit int) (int64, error) {
	return b.prune(ctx, before, limit)
}

func TestStoreValidatesInputs(t *testing.T) {
	backend := fakeBackend{
		process: func(context.Context, inbox.Key, inbox.Fingerprint, inbox.Handler) (inbox.Result, error) {
			return inbox.Result{}, nil
		},
		prune: func(context.Context, time.Time, int) (int64, error) { return 0, nil },
	}
	store, err := inbox.New(backend)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := store.Process(
		t.Context(), inbox.Key{}, inbox.Fingerprint{}, func(context.Context) error { return nil },
	); !errors.Is(err, inbox.ErrInvalidKey) {
		t.Fatalf("process error = %v", err)
	}
	if store.SupportsAttempts() {
		t.Fatal("legacy backend unexpectedly reports durable attempt support")
	}
	if _, err := store.ProcessAttempt(
		t.Context(),
		inbox.Key{ConsumerID: testInboxConsumerID, Source: testInboxSource, MessageID: mustInboxMessageID(t)},
		inbox.Fingerprint{},
		1,
		func(context.Context) error { return nil },
	); !errors.Is(err, inbox.ErrAttemptTrackingUnsupported) {
		t.Fatalf("attempt backend error = %v", err)
	}
	if _, err := store.Prune(t.Context(), time.Time{}, 0); err == nil {
		t.Fatal("prune accepted invalid bounds")
	}
}

func mustInboxMessageID(t *testing.T) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000021")
	if err != nil {
		t.Fatalf("parse message ID: %v", err)
	}
	return id
}

func TestFingerprintEnvelopeMatchesMessenger(t *testing.T) {
	data := []byte(`{"specVersion":"1.0"}`)
	if got, want := inbox.FingerprintEnvelope(data), inbox.Fingerprint(messenger.EnvelopeFingerprint(data)); got != want {
		t.Fatalf("fingerprint = %x, want %x", got, want)
	}
}

func TestAttemptFingerprintSeparatesReplayGenerations(t *testing.T) {
	fingerprint := inbox.FingerprintEnvelope([]byte(`{"specVersion":"1.0"}`))
	key := inbox.Key{ConsumerID: testInboxConsumerID, Source: testInboxSource, MessageID: mustInboxMessageID(t)}
	if got := inbox.AttemptFingerprint(key, fingerprint); got != fingerprint {
		t.Fatalf("default attempt fingerprint = %x, want %x", got, fingerprint)
	}
	key.AttemptGeneration = "gm-replay-one"
	first := inbox.AttemptFingerprint(key, fingerprint)
	key.AttemptGeneration = "gm-replay-two"
	second := inbox.AttemptFingerprint(key, fingerprint)
	if first == fingerprint || second == fingerprint || first == second {
		t.Fatalf("generation fingerprints are not distinct: source=%x first=%x second=%x", fingerprint, first, second)
	}
}

func TestValidateKeyRejectsInvalidAttemptGeneration(t *testing.T) {
	key := inbox.Key{
		ConsumerID: testInboxConsumerID, Source: testInboxSource, MessageID: mustInboxMessageID(t),
		AttemptGeneration: " invalid ",
	}
	if err := inbox.ValidateKey(key); !errors.Is(err, inbox.ErrInvalidKey) {
		t.Fatalf("invalid generation error = %v", err)
	}
}
