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
	recorder.mu.Lock()
	retainedConfirmations := len(recorder.seen)
	recorder.mu.Unlock()
	if retainedConfirmations != 0 {
		t.Fatalf("retained flushed confirmations = %d, want 0", retainedConfirmations)
	}

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
	if retainedConfirmations := len(recorder.seen); retainedConfirmations != 0 {
		t.Fatalf("retained flushed confirmations = %d, want 0", retainedConfirmations)
	}
}

func TestPublicationRecorderSizeFlushLeavesConcurrentPartialBatchPending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		batchSize int
		extra     int
	}{
		{name: "one extra confirmation", batchSize: 2, extra: 1},
		{name: "partial tail", batchSize: 4, extra: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testPublicationRecorderPartialTail(t, test.batchSize, test.extra)
		})
	}
}

func testPublicationRecorderPartialTail(t *testing.T, batchSize, extra int) {
	t.Helper()
	firstWriteStarted := make(chan struct{})
	releaseFirstWrite := make(chan struct{})
	var (
		mu    sync.Mutex
		sizes []int
	)
	recorder, err := newPublicationRecorderWithWriter(
		batchSize,
		time.Hour,
		func(_ context.Context, batch []publicationConfirmation) error {
			mu.Lock()
			call := len(sizes)
			sizes = append(sizes, len(batch))
			mu.Unlock()
			if call == 0 {
				close(firstWriteStarted)
				<-releaseFirstWrite
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("newPublicationRecorderWithWriter() error = %v", err)
	}

	for index := 0; index < batchSize; index++ {
		recorder.Record(testPublication(index))
	}
	flushErr := make(chan error, 1)
	go func() { flushErr <- recorder.flushFullBatches(t.Context()) }()
	<-firstWriteStarted
	for index := 0; index < extra; index++ {
		recorder.Record(testPublication(batchSize + index))
	}
	close(releaseFirstWrite)
	if err := <-flushErr; err != nil {
		t.Fatalf("flushFullBatches() error = %v", err)
	}

	stats := recorder.Stats()
	if stats.Flushed != int64(batchSize) || stats.Pending != extra || stats.Batches != 1 {
		t.Fatalf("Stats() = %#v, want one full batch and %d pending", stats, extra)
	}
	mu.Lock()
	gotSizes := append([]int(nil), sizes...)
	mu.Unlock()
	if len(gotSizes) != 1 || gotSizes[0] != batchSize {
		t.Fatalf("batch sizes after size flush = %v, want [%d]", gotSizes, batchSize)
	}

	if err := recorder.Flush(t.Context()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	mu.Lock()
	gotSizes = append(gotSizes[:0], sizes...)
	mu.Unlock()
	if len(gotSizes) != 2 || gotSizes[1] != extra {
		t.Fatalf("batch sizes after final flush = %v, want [%d %d]", gotSizes, batchSize, extra)
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
