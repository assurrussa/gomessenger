package nats_test

import (
	"testing"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

func TestSubject(t *testing.T) {
	got, err := nats.Subject("media.prod", messenger.DescriptorInfo{
		Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 2,
	})
	if err != nil {
		t.Fatalf("subject: %v", err)
	}
	if want := "media.prod.event.media.processed.v2"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
}

func TestSubjectRejectsWildcardsAndEmptySegments(t *testing.T) {
	for _, namespace := range []string{"media.*", "media..prod", " media"} {
		if _, err := nats.Subject(namespace, messenger.DescriptorInfo{
			Kind: messenger.KindCommand, Name: "job.run", SchemaVersion: 1,
		}); err == nil {
			t.Fatalf("namespace %q accepted", namespace)
		}
	}
}
