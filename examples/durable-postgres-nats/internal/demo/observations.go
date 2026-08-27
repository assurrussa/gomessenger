package demo

import (
	"context"
	"sort"
	"sync"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type benchmarkObservationRecorder struct {
	mu      sync.Mutex
	labels  map[string]BenchmarkLabels
	byStage map[BenchmarkLabels]*stageObservations
}

type stageObservations struct {
	handleDurations []time.Duration
	ackDurations    []time.Duration
	handleErrors    int64
	ackErrors       int64
	duplicates      int64
}

func newBenchmarkObservationRecorder() *benchmarkObservationRecorder {
	return &benchmarkObservationRecorder{
		labels: make(map[string]BenchmarkLabels), byStage: make(map[BenchmarkLabels]*stageObservations),
	}
}

func (r *benchmarkObservationRecorder) register(messageID string, labels BenchmarkLabels) {
	if r == nil || messageID == "" || labels == (BenchmarkLabels{}) {
		return
	}
	r.mu.Lock()
	r.labels[messageID] = labels
	r.mu.Unlock()
}

func (r *benchmarkObservationRecorder) unregister(messageID string) {
	if r == nil || messageID == "" {
		return
	}
	r.mu.Lock()
	delete(r.labels, messageID)
	r.mu.Unlock()
}

func (r *benchmarkObservationRecorder) Observe(_ context.Context, observation messenger.Observation) {
	if r == nil || (observation.Operation != messenger.OperationHandle &&
		observation.Operation != messenger.OperationBrokerAck) {
		return
	}
	messageID := observation.MessageID.String()
	r.mu.Lock()
	defer r.mu.Unlock()
	labels, ok := r.labels[messageID]
	if !ok {
		return
	}
	stage := r.byStage[labels]
	if stage == nil {
		stage = &stageObservations{}
		r.byStage[labels] = stage
	}
	switch observation.Operation {
	case messenger.OperationHandle:
		stage.handleDurations = append(stage.handleDurations, observation.Duration)
		if observation.Err != nil {
			stage.handleErrors++
		}
		if observation.Duplicate {
			stage.duplicates++
		}
	case messenger.OperationBrokerAck:
		stage.ackDurations = append(stage.ackDurations, observation.Duration)
		if observation.Err != nil {
			stage.ackErrors++
		} else {
			delete(r.labels, messageID)
		}
	default:
		return
	}
}

func (r *benchmarkObservationRecorder) stats(labels BenchmarkLabels) ConsumerObservationStats {
	if r == nil || labels == (BenchmarkLabels{}) {
		return ConsumerObservationStats{}
	}
	r.mu.Lock()
	stage := r.byStage[labels]
	if stage == nil {
		r.mu.Unlock()
		return ConsumerObservationStats{}
	}
	handleDurations := append([]time.Duration(nil), stage.handleDurations...)
	ackDurations := append([]time.Duration(nil), stage.ackDurations...)
	handleErrors := stage.handleErrors
	ackErrors := stage.ackErrors
	duplicates := stage.duplicates
	r.mu.Unlock()
	return ConsumerObservationStats{
		InboxHandle: operationStats(handleDurations, handleErrors),
		BrokerAck:   operationStats(ackDurations, ackErrors),
		Duplicates:  duplicates,
	}
}

func operationStats(durations []time.Duration, errorsCount int64) OperationStats {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return OperationStats{
		Count: int64(len(durations)), Errors: errorsCount,
		P50Millis: durationPercentileMillis(durations, 0.50),
		P95Millis: durationPercentileMillis(durations, 0.95),
		P99Millis: durationPercentileMillis(durations, 0.99),
	}
}

func durationPercentileMillis(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	position := percentile * float64(len(values)-1)
	lower := int(position)
	upper := min(lower+1, len(values)-1)
	fraction := position - float64(lower)
	value := float64(values[lower]) + (float64(values[upper])-float64(values[lower]))*fraction
	return value / float64(time.Millisecond)
}
