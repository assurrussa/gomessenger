//nolint:testpackage // Tests exercise the package-local recorder lifecycle and batching contract.
package demo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPublicationRecorderBatchesAndDeduplicates(t *testing.T) {
	t.Parallel()

	written := make(chan []publicationConfirmation, 1)
	recorder, err := newPublicationRecorderWithWriter(2, time.Hour, func(
		_ context.Context,
		batch []publicationConfirmation,
	) error {
		written <- append([]publicationConfirmation(nil), batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() { runErr <- recorder.Run(ctx) }()
	waitRecorderReady(t, recorder)

	first := testPublication(1)
	duplicate := first
	duplicate.PublishedAt = duplicate.PublishedAt.Add(time.Second)
	recorder.Record(first)
	recorder.Record(duplicate)
	recorder.Record(testPublication(2))

	select {
	case batch := <-written:
		if len(batch) != 2 {
			t.Fatalf("batch size = %d, want 2", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("publication batch was not flushed")
	}
	eventually(t, time.Second, func() bool {
		stats := recorder.Stats()
		return stats.Recorded == 2 && stats.Duplicates == 1 && stats.Flushed == 2 &&
			stats.Batches == 1 && stats.Pending == 0
	})

	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context cancellation", err)
	}
}

func TestPublicationRecorderFlushesOnInterval(t *testing.T) {
	t.Parallel()

	written := make(chan struct{}, 1)
	recorder, err := newPublicationRecorderWithWriter(256, 10*time.Millisecond, func(
		context.Context,
		[]publicationConfirmation,
	) error {
		written <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = recorder.Run(ctx) }()
	waitRecorderReady(t, recorder)
	recorder.Record(testPublication(1))
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("publication recorder did not flush on interval")
	}
}

func TestPublicationRecorderFailureIsUnhealthyAndRetainsBatch(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("measurement database unavailable")
	recorder, err := newPublicationRecorderWithWriter(1, time.Hour, func(
		context.Context,
		[]publicationConfirmation,
	) error {
		return wantErr
	})
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- recorder.Run(t.Context()) }()
	waitRecorderReady(t, recorder)
	recorder.Record(testPublication(1))
	if err := <-runErr; !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want %v", err, wantErr)
	}
	stats := recorder.Stats()
	if stats.Pending != 1 || stats.Error == "" {
		t.Fatalf("Stats() = %#v, want retained unhealthy batch", stats)
	}
	if err := recorder.Readiness(t.Context()); !errors.Is(err, wantErr) {
		t.Fatalf("Readiness() error = %v, want %v", err, wantErr)
	}
}

func TestPublicationRecorderFinalFlushUsesBoundedBatches(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		sizes []int
	)
	recorder, err := newPublicationRecorderWithWriter(256, time.Hour, func(
		_ context.Context,
		batch []publicationConfirmation,
	) error {
		mu.Lock()
		defer mu.Unlock()
		sizes = append(sizes, len(batch))
		return nil
	})
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}
	for index := 0; index < 257; index++ {
		recorder.Record(testPublication(index))
	}
	if err := recorder.Flush(t.Context()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 2 || sizes[0] != 256 || sizes[1] != 1 {
		t.Fatalf("batch sizes = %v, want [256 1]", sizes)
	}
	stats := recorder.Stats()
	if stats.Flushed != 257 || stats.Pending != 0 {
		t.Fatalf("Stats() = %#v", stats)
	}
}

func TestPublicationRecorderRejectsConflictingDuplicate(t *testing.T) {
	t.Parallel()

	recorder, err := newPublicationRecorderWithWriter(256, time.Hour, func(
		context.Context,
		[]publicationConfirmation,
	) error {
		return nil
	})
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}
	first := testPublication(1)
	conflict := first
	conflict.SHA256 = "different"
	recorder.Record(first)
	recorder.Record(conflict)
	if stats := recorder.Stats(); stats.Error == "" || stats.Pending != 1 || stats.Recorded != 1 {
		t.Fatalf("Stats() = %#v, want one pending confirmation and conflict", stats)
	}
}

func testPublication(index int) publicationConfirmation {
	return publicationConfirmation{
		envelopeMeasurement: envelopeMeasurement{
			MessageID: fmt.Sprintf("message-%d", index),
			Labels: BenchmarkLabels{
				RunID: testRunID, StageID: testStageID,
			},
			EnvelopeBytes: int64(100 + index),
			SHA256:        fmt.Sprintf("digest-%d", index),
		},
		PublishedAt: time.Date(2026, 8, 27, 10, 0, index%60, 0, time.UTC),
	}
}

func waitRecorderReady(t *testing.T, recorder *publicationRecorder) {
	t.Helper()
	eventually(t, time.Second, func() bool { return recorder.Readiness(t.Context()) == nil })
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
