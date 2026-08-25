package kafka

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var errRetryOffsetRewindNotConfirmed = errors.New("messenger/kafka: retry offset rewind was not confirmed")

type topicPartition struct {
	topic     string
	partition int32
}

type deferredPartition struct {
	topicPartition
	deadline time.Time
	index    int
}

type deferredPartitionHeap []*deferredPartition

func (h deferredPartitionHeap) Len() int { return len(h) }

func (h deferredPartitionHeap) Less(left, right int) bool {
	return h[left].deadline.Before(h[right].deadline)
}

func (h deferredPartitionHeap) Swap(left, right int) {
	h[left], h[right] = h[right], h[left]
	h[left].index = left
	h[right].index = right
}

func (h *deferredPartitionHeap) Push(value any) {
	item := value.(*deferredPartition) //nolint:forcetypeassert // container/heap requires this exact element type.
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *deferredPartitionHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}

// retryPartitionScheduler is bounded by the number of distinct deferred
// topic-partitions seen by one worker. It never retains records or payloads.
type retryPartitionScheduler struct {
	deadlines  deferredPartitionHeap
	partitions map[topicPartition]*deferredPartition
	ownedPause map[topicPartition]struct{}
}

func newRetryPartitionScheduler() *retryPartitionScheduler {
	return &retryPartitionScheduler{
		partitions: make(map[topicPartition]*deferredPartition),
		ownedPause: make(map[topicPartition]struct{}),
	}
}

func (scheduler *retryPartitionScheduler) schedule(
	partition topicPartition,
	deadline time.Time,
	ownsPause bool,
) {
	if item, ok := scheduler.partitions[partition]; ok {
		item.deadline = deadline
		heap.Fix(&scheduler.deadlines, item.index)
	} else {
		item := &deferredPartition{topicPartition: partition, deadline: deadline}
		scheduler.partitions[partition] = item
		heap.Push(&scheduler.deadlines, item)
	}
	if ownsPause {
		scheduler.ownedPause[partition] = struct{}{}
	}
}

func (scheduler *retryPartitionScheduler) pollTimeout(now time.Time, maximum time.Duration) time.Duration {
	if len(scheduler.deadlines) == 0 {
		return maximum
	}
	return min(maximum, max(time.Duration(0), scheduler.deadlines[0].deadline.Sub(now)))
}

func (scheduler *retryPartitionScheduler) releaseDue(now time.Time) map[string][]int32 {
	var resume map[string][]int32
	for len(scheduler.deadlines) > 0 && !scheduler.deadlines[0].deadline.After(now) {
		item := heap.Pop(&scheduler.deadlines).(*deferredPartition) //nolint:forcetypeassert // heap element type is fixed.
		delete(scheduler.partitions, item.topicPartition)
		if _, owned := scheduler.ownedPause[item.topicPartition]; !owned {
			continue
		}
		delete(scheduler.ownedPause, item.topicPartition)
		if resume == nil {
			resume = make(map[string][]int32)
		}
		resume[item.topic] = append(resume[item.topic], item.partition)
	}
	for topic := range resume {
		sort.Slice(resume[topic], func(left, right int) bool { return resume[topic][left] < resume[topic][right] })
	}
	return resume
}

type retryPartitionSession interface {
	AllowRebalance()
	PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(topicPartitions map[string][]int32)
	SetOffsets(offsets map[string]map[int32]kgo.EpochOffset)
	CommittedOffsets() map[string]map[int32]kgo.EpochOffset
	UncommittedOffsets() map[string]map[int32]kgo.EpochOffset
}

func pauseAndRewindRetryPartition(
	session retryPartitionSession,
	record *kgo.Record,
) (topicPartition, bool, error) {
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	previouslyPaused := session.PauseFetchPartitions(nil)
	ownedPause := !containsPartition(previouslyPaused, partition)
	target := map[string][]int32{record.Topic: {record.Partition}}
	session.PauseFetchPartitions(target)

	want := kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
	session.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		record.Topic: {record.Partition: want},
	})
	committedByTopic, committedTopicFound := session.CommittedOffsets()[record.Topic]
	committed, committedPartitionFound := committedByTopic[record.Partition]
	uncommittedByTopic := session.UncommittedOffsets()[record.Topic]
	uncommitted, hasUncommitted := uncommittedByTopic[record.Partition]
	if !committedTopicFound || !committedPartitionFound || committed != want || hasUncommitted && uncommitted != want {
		return partition, ownedPause, fmt.Errorf(
			"%w: %s[%d] want epoch %d offset %d, committed=(%t epoch %d offset %d), "+
				"uncommitted=(%t epoch %d offset %d)",
			errRetryOffsetRewindNotConfirmed,
			record.Topic,
			record.Partition,
			want.Epoch,
			want.Offset,
			committedPartitionFound,
			committed.Epoch,
			committed.Offset,
			hasUncommitted,
			uncommitted.Epoch,
			uncommitted.Offset,
		)
	}
	return partition, ownedPause, nil
}

func containsPartition(paused map[string][]int32, partition topicPartition) bool {
	for _, candidate := range paused[partition.topic] {
		if candidate == partition.partition {
			return true
		}
	}
	return false
}
