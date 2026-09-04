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
	labels  map[string]benchmarkMessageObservation
	byStage map[BenchmarkLabels]*stageObservations
}

type benchmarkMessageObservation struct {
	labels  BenchmarkLabels
	handled bool
	acked   bool
}

type stageObservations struct {
	handleDurations  []time.Duration
	ackDurations     []time.Duration
	batchDurations   []time.Duration
	handleErrors     int64
	ackErrors        int64
	batchErrors      int64
	batchMessages    int64
	maxBatchMessages int
	duplicates       int64
	accepted         int64
	committed        int64
}

func newBenchmarkObservationRecorder() *benchmarkObservationRecorder {
	return &benchmarkObservationRecorder{
		labels:  make(map[string]benchmarkMessageObservation),
		byStage: make(map[BenchmarkLabels]*stageObservations),
	}
}

func (r *benchmarkObservationRecorder) register(messageID string, labels BenchmarkLabels) {
	if r == nil || messageID == "" || labels == (BenchmarkLabels{}) {
		return
	}
	r.mu.Lock()
	r.labels[messageID] = benchmarkMessageObservation{labels: labels}
	r.mu.Unlock()
}

func (r *benchmarkObservationRecorder) recordAccepted(labels BenchmarkLabels, messages int) {
	if r == nil || labels == (BenchmarkLabels{}) || messages < 1 {
		return
	}
	r.mu.Lock()
	stage := r.stageLocked(labels)
	stage.accepted += int64(messages)
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
	message, ok := r.labels[messageID]
	if !ok {
		return
	}
	stage := r.stageLocked(message.labels)
	switch observation.Operation {
	case messenger.OperationHandle:
		stage.handleDurations = append(stage.handleDurations, observation.Duration)
		if observation.Err != nil {
			stage.handleErrors++
		}
		if observation.Duplicate {
			stage.duplicates++
		}
		if observation.Err == nil {
			message.handled = true
			if !observation.Duplicate {
				stage.committed++
			}
		}
	case messenger.OperationBrokerAck:
		stage.ackDurations = append(stage.ackDurations, observation.Duration)
		if observation.Err != nil {
			stage.ackErrors++
		} else {
			message.acked = true
		}
	default:
		return
	}
	if message.handled && message.acked {
		delete(r.labels, messageID)
	} else {
		r.labels[messageID] = message
	}
}

func (r *benchmarkObservationRecorder) recordBatch(
	labels BenchmarkLabels,
	messages int,
	duration time.Duration,
	err error,
) {
	if r == nil || labels == (BenchmarkLabels{}) || messages < 1 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stage := r.stageLocked(labels)
	stage.batchDurations = append(stage.batchDurations, duration)
	stage.batchMessages += int64(messages)
	stage.maxBatchMessages = max(stage.maxBatchMessages, messages)
	if err != nil {
		stage.batchErrors++
	}
}

func (r *benchmarkObservationRecorder) stageLocked(labels BenchmarkLabels) *stageObservations {
	stage := r.byStage[labels]
	if stage == nil {
		stage = &stageObservations{}
		r.byStage[labels] = stage
	}
	return stage
}

func (r *benchmarkObservationRecorder) progress(labels BenchmarkLabels) BenchmarkProgressStats {
	if r == nil || labels == (BenchmarkLabels{}) {
		return BenchmarkProgressStats{}
	}
	r.mu.Lock()
	stage := r.byStage[labels]
	if stage == nil {
		r.mu.Unlock()
		return BenchmarkProgressStats{}
	}
	accepted, committed := stage.accepted, stage.committed
	r.mu.Unlock()
	return BenchmarkProgressStats{
		Accepted: accepted, Staged: accepted, Committed: committed,
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
	batchDurations := append([]time.Duration(nil), stage.batchDurations...)
	batchErrors := stage.batchErrors
	batchMessages := stage.batchMessages
	maxBatchMessages := stage.maxBatchMessages
	r.mu.Unlock()
	batchInvocations := int64(len(batchDurations))
	var averageBatch float64
	if batchInvocations != 0 {
		averageBatch = float64(batchMessages) / float64(batchInvocations)
	}
	return ConsumerObservationStats{
		InboxHandle: operationStats(handleDurations, handleErrors),
		BrokerAck:   operationStats(ackDurations, ackErrors),
		Batch: BatchHandlerStats{
			Invocations: batchInvocations, Messages: batchMessages,
			AverageMessages: averageBatch, MaxMessages: maxBatchMessages,
			Handler: operationStats(batchDurations, batchErrors),
		},
		Duplicates: duplicates,
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
