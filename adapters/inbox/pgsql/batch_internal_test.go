package pgsql

import (
	"testing"

	messenger "github.com/assurrussa/gomessenger"

	"github.com/assurrussa/gomessenger/adapters/inbox"
)

const (
	testGen1   = "gen-1"
	testGen2   = "gen-2"
	testSource = "urn:test"
)

func TestPartitionBatchItemsSplitsConflictingGenerations(t *testing.T) {
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
		{Key: inbox.Key{Source: testSource, MessageID: msgA, AttemptGeneration: testGen1}},
		{Key: inbox.Key{Source: testSource, MessageID: msgB, AttemptGeneration: testGen1}},
		{Key: inbox.Key{Source: testSource, MessageID: msgA, AttemptGeneration: testGen2}},
		{Key: inbox.Key{Source: testSource, MessageID: msgB, AttemptGeneration: testGen2}},
	}

	partitions := partitionBatchItems(items)
	if len(partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(partitions))
	}
	if len(partitions[0].indexes) != 2 || partitions[0].indexes[0] != 0 || partitions[0].indexes[1] != 1 {
		t.Fatalf("unexpected partition 0: %+v", partitions[0])
	}
	if len(partitions[1].indexes) != 2 || partitions[1].indexes[0] != 2 || partitions[1].indexes[1] != 3 {
		t.Fatalf("unexpected partition 1: %+v", partitions[1])
	}
}

func TestPartitionBatchItemsEmptyAndSingle(t *testing.T) {
	t.Parallel()

	if parts := partitionBatchItems(nil); len(parts) != 0 {
		t.Fatalf("expected empty partitions for nil, got %d", len(parts))
	}

	msgA, err := messenger.ParseMessageID("018f4f2c-4a00-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	single := []inbox.BatchItem{
		{Key: inbox.Key{Source: testSource, MessageID: msgA, AttemptGeneration: testGen1}},
	}
	if parts := partitionBatchItems(single); len(parts) != 1 {
		t.Fatalf("expected 1 partition for single item, got %d", len(parts))
	}
}
