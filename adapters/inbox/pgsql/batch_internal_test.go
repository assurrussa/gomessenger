package pgsql

import (
	"errors"
	"testing"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

const (
	testGen1   = "gen-1"
	testGen2   = "gen-2"
	testSource = "urn:test"
)

func TestPrepareBatchGroupsRejectsConflictingGenerations(t *testing.T) {
	t.Parallel()

	msgA, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	msgB, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000002")
	if err != nil {
		t.Fatal(err)
	}

	items := []inbox.BatchItem{
		{Key: inbox.Key{ConsumerID: "c1", Source: testSource, MessageID: msgA, AttemptGeneration: testGen1}},
		{Key: inbox.Key{ConsumerID: "c1", Source: testSource, MessageID: msgB, AttemptGeneration: testGen1}},
		{Key: inbox.Key{ConsumerID: "c1", Source: testSource, MessageID: msgA, AttemptGeneration: testGen2}},
	}

	_, err = prepareBatchGroups(items)
	if err == nil {
		t.Fatal("prepareBatchGroups succeeded, want ErrInvalidBatchResult")
	}
	if !errors.Is(err, messenger.ErrInvalidBatchResult) {
		t.Fatalf("prepareBatchGroups err = %v, want ErrInvalidBatchResult", err)
	}
}
