package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaCompareName     = "publish-comparison"
	kafkaCompareWarmup   = 128
	kafkaCompareMessages = 2048
	kafkaCompareRepeats  = 3
)

type kafkaPublishCase struct {
	Name      string
	BatchSize int
	Typed     bool
	Batch     bool
}

type kafkaPublishInput struct {
	Wire     []byte
	Outgoing messenger.Outgoing[kafkaPipelinePayload]
}

type kafkaPublishResult struct {
	Receipts []messenger.Receipt
	Errors   []error
	Err      error
}

type kafkaPublishSample struct {
	Mode                   string    `json:"mode"`
	Repeat                 int       `json:"repeat"`
	BatchSize              int       `json:"batchSize"`
	Messages               int       `json:"messages"`
	EnvelopeBytes          int       `json:"envelopeBytes"`
	Calls                  int       `json:"calls"`
	StartedAt              time.Time `json:"startedAt"`
	ElapsedNanos           int64     `json:"elapsedNanos"`
	CallNanos              []int64   `json:"callNanos"`
	PublishNanosPerMessage float64   `json:"publishNanosPerMessage"`
	MessagesPerSecond      float64   `json:"messagesPerSecond"`
	P50CallNanos           int64     `json:"p50CallNanos"`
	P95CallNanos           int64     `json:"p95CallNanos"`
	Reconciled             int       `json:"reconciled"`
}

type kafkaPublishReport struct {
	SpecVersion      string               `json:"specVersion"`
	Scope            string               `json:"scope"`
	Commit           string               `json:"commit"`
	KafkaVersion     string               `json:"kafkaVersion"`
	KafkaImage       string               `json:"kafkaImage"`
	GoVersion        string               `json:"goVersion"`
	Platform         string               `json:"platform"`
	CPUs             int                  `json:"cpus"`
	GoMaxProcs       int                  `json:"goMaxProcs"`
	WarmupMessages   int                  `json:"warmupMessages"`
	PayloadDataBytes int                  `json:"payloadDataBytes"`
	Complete         bool                 `json:"complete"`
	Samples          []kafkaPublishSample `json:"samples"`
}

// TestKafkaPublishComparison is opt-in and uses only a disposable broker started
// by scripts/test-kafka.sh. It is a closed-loop publication screen, not capacity.
func TestKafkaPublishComparison(t *testing.T) {
	if os.Getenv("GOMESSENGER_KAFKA_PUBLISH_COMPARE") != "1" {
		t.Skip("use KAFKA_TEST_MODE=publish-comparison make test-kafka")
	}
	brokersValue := os.Getenv("GOMESSENGER_KAFKA_BROKERS")
	if brokersValue == "" {
		t.Fatal("comparison requested without a disposable Kafka broker")
	}
	event := messenger.MustEvent(kafkaCompareName, 1, messenger.JSON[kafkaPipelinePayload]())
	inputs := kafkaPublishInputs(t, event)
	report := kafkaPublishReport{
		SpecVersion: "1.0", Scope: "closed-loop-publish-commit-screen",
		Commit: os.Getenv("GOMESSENGER_KAFKA_COMMIT"), KafkaVersion: os.Getenv("GOMESSENGER_KAFKA_VERSION"),
		KafkaImage: os.Getenv("GOMESSENGER_KAFKA_IMAGE"), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU(), GoMaxProcs: runtime.GOMAXPROCS(0),
		WarmupMessages: kafkaCompareWarmup, PayloadDataBytes: 256,
		Samples: make([]kafkaPublishSample, 0, 5*kafkaCompareRepeats),
	}
	defer func() { writeKafkaPublishReport(t, report) }()
	persistKafkaPublishReport(t, report)
	cases := []kafkaPublishCase{
		{Name: "typed-single", BatchSize: 1, Typed: true},
		{Name: "envelope-single", BatchSize: 1},
		{Name: "envelope-batch-1", BatchSize: 1, Batch: true},
		{Name: "envelope-batch-16", BatchSize: 16, Batch: true},
		{Name: "envelope-batch-64", BatchSize: 64, Batch: true},
	}
	for repeat := range kafkaCompareRepeats {
		order := slices.Clone(cases)
		if repeat%2 == 1 {
			slices.Reverse(order)
		}
		for _, mode := range order {
			if !t.Run(fmt.Sprintf("r%d/%s", repeat+1, mode.Name), func(t *testing.T) {
				sample := runKafkaPublishCase(t, strings.Split(brokersValue, ","), event, inputs, mode)
				sample.Repeat = repeat + 1
				report.Samples = append(report.Samples, sample)
			}) {
				return
			}
			persistKafkaPublishReport(t, report)
		}
	}
	report.Complete = true
}

func kafkaPublishInputs(t *testing.T, event messenger.Event[kafkaPipelinePayload]) []kafkaPublishInput {
	t.Helper()
	inputs := make([]kafkaPublishInput, kafkaCompareWarmup+kafkaCompareMessages+1)
	for index := range inputs {
		payload := kafkaPipelinePayload{Case: fmt.Sprintf("%08d", index), Data: strings.Repeat("x", 256)}
		wire := encodeKafkaIntegrationPayloadWithKey(t, event, payload, kafkaCompareName)
		envelope, err := messenger.UnmarshalEnvelope(wire)
		if err != nil {
			t.Fatal(err)
		}
		metadata := envelope.Metadata()
		inputs[index] = kafkaPublishInput{Wire: wire, Outgoing: messenger.Outgoing[kafkaPipelinePayload]{
			Payload: payload, Metadata: messenger.OutgoingMetadata{
				ID: metadata.ID, Time: metadata.Time, CorrelationID: metadata.CorrelationID, Key: metadata.Key,
			},
		}}
	}
	return inputs
}

func runKafkaPublishCase(
	t *testing.T,
	brokers []string,
	event messenger.Event[kafkaPipelinePayload],
	inputs []kafkaPublishInput,
	mode kafkaPublishCase,
) kafkaPublishSample {
	t.Helper()
	route, publisher, topic := startKafkaPublishCase(t, brokers, event)
	publish := kafkaPublishCall(route, publisher, mode)
	wires := make([][]byte, len(inputs))
	for index := range inputs {
		wires[index] = inputs[index].Wire
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	for index := 0; index < kafkaCompareWarmup; index += mode.BatchSize {
		result := publish(ctx, inputs[index:index+mode.BatchSize], wires[index:index+mode.BatchSize])
		checkKafkaPublishResult(t, inputs[index:index+mode.BatchSize], result)
	}
	measured := inputs[kafkaCompareWarmup : kafkaCompareWarmup+kafkaCompareMessages]
	results := make([]kafkaPublishResult, kafkaCompareMessages/mode.BatchSize)
	sample := kafkaPublishSample{
		Mode: mode.Name, BatchSize: mode.BatchSize, Messages: kafkaCompareMessages, Calls: len(results),
		CallNanos: make([]int64, len(results)), StartedAt: time.Now().UTC(),
	}
	for _, input := range measured {
		sample.EnvelopeBytes += len(input.Wire)
	}
	started := time.Now()
	for call := range results {
		start := kafkaCompareWarmup + call*mode.BatchSize
		end := start + mode.BatchSize
		callStarted := time.Now()
		results[call] = publish(ctx, inputs[start:end], wires[start:end])
		sample.CallNanos[call] = time.Since(callStarted).Nanoseconds()
		if results[call].Err != nil || slices.ContainsFunc(results[call].Errors, func(err error) bool { return err != nil }) {
			t.Fatalf("publication failed in call %d: top=%v items=%v", call, results[call].Err, results[call].Errors)
		}
	}
	sample.ElapsedNanos = time.Since(started).Nanoseconds()
	for call, result := range results {
		start := call * mode.BatchSize
		checkKafkaPublishResult(t, measured[start:start+mode.BatchSize], result)
	}
	// A final committed record closes the ordered, single-partition prefix. The
	// verifier must encounter every exact input once before reaching this fence.
	fence := len(inputs) - 1
	receipt, err := route.PublishEnvelope(ctx, wires[fence])
	checkKafkaPublishResult(t, inputs[fence:], kafkaPublishResult{
		Receipts: []messenger.Receipt{receipt}, Errors: []error{err},
	})
	sample.Reconciled = reconcileKafkaPublishCase(t, ctx, brokers, topic, inputs)
	return summarizeKafkaPublishSample(sample)
}

func kafkaPublishCall(
	route *kafkaadapter.Route,
	publisher messenger.Publisher[kafkaPipelinePayload],
	mode kafkaPublishCase,
) func(context.Context, []kafkaPublishInput, [][]byte) kafkaPublishResult {
	return func(ctx context.Context, inputs []kafkaPublishInput, wires [][]byte) kafkaPublishResult {
		if mode.Batch {
			receipts, itemErrors, err := route.PublishEnvelopeBatch(ctx, wires)
			return kafkaPublishResult{Receipts: receipts, Errors: itemErrors, Err: err}
		}
		var receipt messenger.Receipt
		var err error
		if mode.Typed {
			receipt, err = publisher.PublishMessage(ctx, inputs[0].Outgoing)
		} else {
			receipt, err = route.PublishEnvelope(ctx, wires[0])
		}
		return kafkaPublishResult{Receipts: []messenger.Receipt{receipt}, Errors: []error{err}}
	}
}

func checkKafkaPublishResult(t *testing.T, inputs []kafkaPublishInput, result kafkaPublishResult) {
	t.Helper()
	if result.Err != nil || len(result.Receipts) != len(inputs) || len(result.Errors) != len(inputs) {
		t.Fatalf("publication result shape: receipts=%d errors=%d top=%v", len(result.Receipts), len(result.Errors), result.Err)
	}
	for index, input := range inputs {
		receipt := result.Receipts[index]
		if result.Errors[index] != nil || receipt.State != messenger.ReceiptBrokerConfirmed ||
			receipt.MessageID != input.Outgoing.Metadata.ID {
			t.Fatalf("unconfirmed or mismatched publication at %d: receipt=%#v error=%v", index, receipt, result.Errors[index])
		}
	}
}

func startKafkaPublishCase(
	t *testing.T,
	brokers []string,
	event messenger.Event[kafkaPipelinePayload],
) (*kafkaadapter.Route, messenger.Publisher[kafkaPipelinePayload], string) {
	t.Helper()
	namespace := fmt.Sprintf("publishcompare%d", time.Now().UnixNano())
	topic, err := kafkaadapter.Topic(namespace, event.Info())
	if err != nil {
		t.Fatal(err)
	}
	transport, err := kafkaadapter.NewTransport(kafkaadapter.TransportConfig{
		Name: kafkaCompareName, Brokers: brokers, ClientID: kafkaCompareName, InstanceID: namespace,
		OperationTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := transport.Shutdown(ctx); err != nil {
			t.Errorf("shutdown comparison transport: %v", err)
		}
	})
	topology := kafkaadapter.Topology{SpecVersion: kafkaadapter.TopologySpecVersion, Topics: []kafkaadapter.TopicSpec{{
		Name: topic, Role: kafkaadapter.TopicRoleSource, Partitions: 1, ReplicationFactor: 1, MinInSyncReplicas: 1,
		RetentionMillis: 86_400_000, RetentionBytes: -1, MaxMessageBytes: kafkaadapter.DefaultMaxSourceMessageBytes,
	}}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := kafkaadapter.ApplyTopology(ctx, transport, topology); err != nil {
		t.Fatal(err)
	}
	eventuallyKafka(t, 20*time.Second, func() bool {
		plan, planErr := kafkaadapter.PlanTopology(ctx, transport, topology)
		return planErr == nil && !plan.HasChanges() && !plan.HasConflicts()
	}, "comparison topology did not converge")
	route, err := kafkaadapter.NewRoute(transport, kafkaadapter.RouteConfig{Name: "publish.compare", Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:kafka-outbox"))
	builder.RouteEvent(event, route)
	bus, _, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- transport.Run(context.Background()) }()
	t.Cleanup(func() {
		transport.BeginDrain()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("run comparison transport: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("comparison transport did not stop")
		}
	})
	eventuallyKafka(t, 20*time.Second, func() bool { return transport.Readiness(ctx) == nil }, "comparison transport not ready")
	return route, messenger.BindPublisher(bus, event), topic
}

func reconcileKafkaPublishCase(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	topic string,
	inputs []kafkaPublishInput,
) int {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	seen := 0
	for seen < len(inputs) {
		fetches := client.PollFetches(verifyCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("read-committed reconciliation: %v", errs)
		}
		for _, record := range fetches.Records() {
			if seen >= len(inputs) || record.Topic != topic || record.Partition != 0 ||
				!bytes.Equal(record.Value, inputs[seen].Wire) || string(record.Key) != inputs[seen].Outgoing.Metadata.Key {
				t.Fatalf("duplicate, missing, reordered, unexpected, or corrupt committed record at index %d", seen)
			}
			seen++
		}
		if verifyCtx.Err() != nil {
			t.Fatalf("reconciled %d/%d records: %v", seen, len(inputs), verifyCtx.Err())
		}
	}
	return seen
}

func summarizeKafkaPublishSample(sample kafkaPublishSample) kafkaPublishSample {
	ordered := slices.Clone(sample.CallNanos)
	slices.Sort(ordered)
	var total int64
	for _, duration := range ordered {
		total += duration
	}
	sample.P50CallNanos = ordered[(len(ordered)*50+99)/100-1]
	sample.P95CallNanos = ordered[(len(ordered)*95+99)/100-1]
	sample.PublishNanosPerMessage = float64(total) / float64(sample.Messages)
	sample.MessagesPerSecond = float64(sample.Messages) / (float64(sample.ElapsedNanos) / float64(time.Second))
	return sample
}

func writeKafkaPublishReport(t *testing.T, report kafkaPublishReport) {
	t.Helper()
	t.Logf("Kafka publish comparison: complete=%t commit=%s Kafka=%s image=%s Go=%s platform=%s CPUs=%d GOMAXPROCS=%d",
		report.Complete, report.Commit, report.KafkaVersion, report.KafkaImage,
		report.GoVersion, report.Platform, report.CPUs, report.GoMaxProcs)
	for _, sample := range report.Samples {
		// Keep a compact summary in durable job logs as well as raw artifact samples.
		sample.CallNanos = nil
		summary, err := json.Marshal(sample)
		if err != nil {
			t.Errorf("encode comparison sample: %v", err)
			return
		}
		t.Logf("KAFKA_PUBLISH_SAMPLE %s", summary)
	}
	persistKafkaPublishReport(t, report)
}

func persistKafkaPublishReport(t *testing.T, report kafkaPublishReport) {
	t.Helper()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("encode comparison report: %v", err)
	}
	if path := os.Getenv("GOMESSENGER_KAFKA_REPORT"); path != "" {
		//nolint:gosec // This opt-in output file is selected by the host launcher, never by broker input.
		if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
			t.Fatalf("write comparison report: %v", err)
		}
	}
}

func TestKafkaPublishStatistics(t *testing.T) {
	input := kafkaPublishSample{Messages: 20, ElapsedNanos: int64(2 * time.Second), CallNanos: []int64{4, 1, 3, 2}}
	got := summarizeKafkaPublishSample(input)
	if got.P50CallNanos != 2 || got.P95CallNanos != 4 || got.PublishNanosPerMessage != 0.5 || got.MessagesPerSecond != 10 {
		t.Fatalf("unexpected summary: %#v", got)
	}
	if !slices.Equal(got.CallNanos, []int64{4, 1, 3, 2}) {
		t.Fatal("summary changed raw sample order")
	}
}
