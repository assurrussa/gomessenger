package e2e_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
	outboxadapter "github.com/assurrussa/gomessenger/adapters/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaPipelinePayload struct {
	Case string `json:"case"`
	Data string `json:"data,omitempty"`
}

const permanentFailureKind = "permanent"

//nolint:gocognit,gocyclo // The ordered end-to-end protocol assertions are intentionally kept in one scenario.
func TestKafkaPipeline(t *testing.T) {
	brokersValue := os.Getenv("GOMESSENGER_KAFKA_BROKERS")
	if brokersValue == "" {
		t.Skip("GOMESSENGER_KAFKA_BROKERS is not set; use make test-kafka")
	}
	brokers := strings.Split(brokersValue, ",")
	namespace := fmt.Sprintf("kafkait%d", time.Now().UnixNano())
	instanceID := fmt.Sprintf("test%d", time.Now().UnixNano())
	consumerID := "pipeline-worker"
	event := messenger.MustEvent("pipeline-event", 1, messenger.JSON[kafkaPipelinePayload]())
	sourceTopic, err := kafkaadapter.Topic(namespace, event.Info())
	if err != nil {
		t.Fatalf("source topic: %v", err)
	}
	retryTopic, err := kafkaadapter.RetryTopic(sourceTopic, consumerID, 0)
	if err != nil {
		t.Fatalf("retry topic: %v", err)
	}
	replayTopic, err := kafkaadapter.ReplayTopic(sourceTopic, consumerID)
	if err != nil {
		t.Fatalf("replay topic: %v", err)
	}
	dlqTopic, err := kafkaadapter.DLQTopic(sourceTopic, consumerID)
	if err != nil {
		t.Fatalf("DLQ topic: %v", err)
	}

	transport, err := kafkaadapter.NewTransport(kafkaadapter.TransportConfig{
		Name: "kafka-integration", Brokers: brokers, ClientID: "gomessenger-kafka-integration",
		InstanceID: instanceID, OperationTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("create Kafka transport: %v", err)
	}
	topology := kafkaIntegrationTopology(sourceTopic, consumerID, retryTopic, replayTopic, dlqTopic)
	if plan, err := kafkaadapter.ApplyTopology(t.Context(), transport, topology); err != nil {
		t.Fatalf("apply Kafka topology: plan=%#v error=%v", plan, err)
	}
	eventuallyKafka(t, 10*time.Second, func() bool {
		plan, planErr := kafkaadapter.PlanTopology(t.Context(), transport, topology)
		return planErr == nil && !plan.HasChanges() && !plan.HasConflicts()
	}, "Kafka topology did not converge")

	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "kafka-inbox.db"))
	if err != nil {
		t.Fatalf("open inbox database: %v", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close inbox database: %v", err)
		}
	})
	if _, err := database.ExecContext(t.Context(), "PRAGMA busy_timeout=5000"); err != nil {
		t.Fatalf("configure inbox database: %v", err)
	}
	if err := inboxsqlite.Migrate(t.Context(), database); err != nil {
		t.Fatalf("migrate inbox database: %v", err)
	}
	store, err := inboxsqlite.New(database)
	if err != nil {
		t.Fatalf("create inbox store: %v", err)
	}

	var attemptsMu sync.Mutex
	attempts := make(map[string]int)
	handled := make(chan string, 8)
	terminalFailed := make(chan struct{}, 1)
	handler := func(_ context.Context, message messenger.Message[kafkaPipelinePayload]) error {
		attemptsMu.Lock()
		attempts[message.Payload.Case]++
		attempt := attempts[message.Payload.Case]
		attemptsMu.Unlock()
		switch message.Payload.Case {
		case "retry":
			if attempt == 1 {
				return errors.New("transient integration failure")
			}
		case "delayed-retry":
			if attempt == 1 {
				return messenger.RetryAfter(errors.New("delayed integration failure"), 2*time.Second)
			}
		case "terminal", "large-terminal":
			if attempt == 1 {
				terminalFailed <- struct{}{}
				return messenger.Permanent(errors.New("permanent integration failure"))
			}
		}
		handled <- message.Payload.Case
		return nil
	}
	consumer, err := kafkaadapter.NewEventConsumer(transport, store, event, handler, kafkaadapter.HandlerConfig{
		Namespace: namespace, ConsumerID: consumerID, Concurrency: 1,
		Timeout: 3 * time.Second, FinalizationTimeout: 3 * time.Second, MaxAttempts: 3,
		BaseRetry: 50 * time.Millisecond, MaxRetry: 100 * time.Millisecond,
		RetryTiers: []time.Duration{100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("create Kafka consumer: %v", err)
	}
	route, err := kafkaadapter.NewRoute(transport, kafkaadapter.RouteConfig{
		Name: "kafka.integration", Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("create Kafka route: %v", err)
	}
	builder := messenger.NewBuilder(messenger.WithSource("urn:service:kafka-integration"))
	builder.RouteEvent(event, route)
	builder.Use("kafka.consumer.pipeline", consumer)
	bus, runtime, err := builder.Build()
	if err != nil {
		t.Fatalf("build Kafka messenger: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	eventuallyKafka(t, 20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return runtime.Readiness(ctx) == nil
	}, "Kafka runtime did not become ready")
	t.Cleanup(func() {
		runtime.BeginDrain()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("shutdown Kafka runtime: %v", err)
		}
		if err := <-runDone; err != nil {
			t.Errorf("run Kafka runtime: %v", err)
		}
	})

	publisher := messenger.BindPublisher(bus, event)
	receipt, err := publisher.Publish(t.Context(), kafkaPipelinePayload{Case: "retry"})
	if err != nil || receipt.State != messenger.ReceiptBrokerConfirmed {
		t.Fatalf("publish retry case: receipt=%#v error=%v", receipt, err)
	}
	waitHandled(t, handled, "retry")
	if got := attemptCount(&attemptsMu, attempts, "retry"); got != 2 {
		t.Fatalf("retry handler attempts = %d, want 2", got)
	}
	delayedStarted := time.Now()
	if _, err := publisher.Publish(t.Context(), kafkaPipelinePayload{Case: "delayed-retry"}); err != nil {
		t.Fatalf("publish delayed retry: %v", err)
	}
	waitHandled(t, handled, "delayed-retry")
	if elapsed := time.Since(delayedStarted); elapsed < 1800*time.Millisecond {
		t.Fatalf("delayed retry completed after %s, want approximately 2s or longer", elapsed)
	}
	if got := attemptCount(&attemptsMu, attempts, "delayed-retry"); got != 2 {
		t.Fatalf("delayed retry handler attempts = %d, want 2", got)
	}
	if _, err := publisher.Publish(t.Context(), kafkaPipelinePayload{Case: "delayed-barrier"}); err != nil {
		t.Fatalf("publish delayed retry barrier: %v", err)
	}
	waitHandled(t, handled, "delayed-barrier")

	relay, err := outboxadapter.NewRelayJob(route, outboxadapter.RelayJobConfig{})
	if err != nil {
		t.Fatalf("create Kafka outbox relay: %v", err)
	}
	outboxEnvelope := encodeKafkaIntegrationEnvelope(t, event, "outbox")
	if err := relay.Handle(t.Context(), string(outboxEnvelope)); err != nil {
		t.Fatalf("relay Kafka outbox envelope: %v", err)
	}
	waitHandled(t, handled, "outbox")
	if err := relay.Handle(t.Context(), string(outboxEnvelope)); err != nil {
		t.Fatalf("relay duplicate Kafka outbox envelope: %v", err)
	}
	if _, err := publisher.Publish(t.Context(), kafkaPipelinePayload{Case: "barrier"}); err != nil {
		t.Fatalf("publish barrier: %v", err)
	}
	waitHandled(t, handled, "barrier")
	if got := attemptCount(&attemptsMu, attempts, "outbox"); got != 1 {
		t.Fatalf("outbox handler attempts = %d, want 1 after duplicate", got)
	}

	dlqClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlqTopic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("create DLQ reader: %v", err)
	}
	defer dlqClient.Close()
	if _, err := publisher.Publish(t.Context(), kafkaPipelinePayload{Case: "terminal"}); err != nil {
		t.Fatalf("publish terminal case: %v", err)
	}
	select {
	case <-terminalFailed:
	case <-time.After(10 * time.Second):
		t.Fatal("terminal handler was not invoked")
	}
	dlqRecord := readKafkaDLQ(t, dlqClient)
	if dlqRecord.FailureKind != permanentFailureKind || dlqRecord.Attempt != 1 || dlqRecord.ConsumerID != consumerID {
		t.Fatalf("DLQ record = %#v", dlqRecord)
	}
	replayResult, err := kafkaadapter.ReplayDLQ(t.Context(), transport, dlqRecord)
	if err != nil || replayResult.Offset < 0 {
		t.Fatalf("replay Kafka DLQ: result=%#v error=%v", replayResult, err)
	}
	waitHandled(t, handled, "terminal")
	if got := attemptCount(&attemptsMu, attempts, "terminal"); got != 2 {
		t.Fatalf("terminal handler attempts after replay = %d, want 2", got)
	}

	const franzGoDefaultBatchMaxBytes = 1_000_012
	largeEnvelope := encodeKafkaIntegrationPayload(t, event, kafkaPipelinePayload{
		Case: "large-terminal",
		Data: strings.Repeat("x", franzGoDefaultBatchMaxBytes),
	})
	if len(largeEnvelope) <= franzGoDefaultBatchMaxBytes {
		t.Fatalf("large envelope = %d bytes, want more than franz-go default batch max", len(largeEnvelope))
	}
	if _, err := route.PublishEnvelope(t.Context(), largeEnvelope); err != nil {
		t.Fatalf("publish large terminal case: %v", err)
	}
	select {
	case <-terminalFailed:
	case <-time.After(10 * time.Second):
		t.Fatal("large terminal handler was not invoked")
	}
	largeDLQRecord := readKafkaDLQ(t, dlqClient)
	if largeDLQRecord.FailureKind != permanentFailureKind ||
		len(largeDLQRecord.OriginalBase64) <= franzGoDefaultBatchMaxBytes {
		t.Fatalf("large DLQ failure kind = %q, originalBase64 bytes = %d",
			largeDLQRecord.FailureKind, len(largeDLQRecord.OriginalBase64))
	}
}

func kafkaIntegrationTopology(source, consumerID, retry, replay, dlq string) kafkaadapter.Topology {
	base := kafkaadapter.TopicSpec{
		Partitions: 1, ReplicationFactor: 1, MinInSyncReplicas: 1,
		RetentionMillis: 86_400_000, RetentionBytes: -1,
		MaxMessageBytes: kafkaadapter.DefaultMaxSourceMessageBytes,
	}
	sourceSpec := base
	sourceSpec.Name = source
	sourceSpec.Role = kafkaadapter.TopicRoleSource
	retrySpec := base
	retrySpec.Name = retry
	retrySpec.Role = kafkaadapter.TopicRoleRetry
	retrySpec.SourceTopic = source
	retrySpec.ConsumerID = consumerID
	retrySpec.RetentionMillis = -1
	retrySpec.RetentionBytes = -1
	replaySpec := base
	replaySpec.Name = replay
	replaySpec.Role = kafkaadapter.TopicRoleReplay
	replaySpec.SourceTopic = source
	replaySpec.ConsumerID = consumerID
	dlqSpec := base
	dlqSpec.Name = dlq
	dlqSpec.Role = kafkaadapter.TopicRoleDLQ
	dlqSpec.SourceTopic = source
	dlqSpec.ConsumerID = consumerID
	dlqSpec.RetentionMillis = 604_800_000
	dlqSpec.MaxMessageBytes = kafkaadapter.DefaultMaxDLQMessageBytes
	return kafkaadapter.Topology{
		SpecVersion: kafkaadapter.TopologySpecVersion,
		Topics:      []kafkaadapter.TopicSpec{sourceSpec, retrySpec, replaySpec, dlqSpec},
	}
}

func encodeKafkaIntegrationEnvelope(
	t *testing.T,
	event messenger.Event[kafkaPipelinePayload],
	caseName string,
) []byte {
	t.Helper()
	return encodeKafkaIntegrationPayload(t, event, kafkaPipelinePayload{Case: caseName})
}

func encodeKafkaIntegrationPayload(
	t *testing.T,
	event messenger.Event[kafkaPipelinePayload],
	payload kafkaPipelinePayload,
) []byte {
	t.Helper()
	id, err := messenger.UUIDv7Generator().New()
	if err != nil {
		t.Fatalf("generate message ID: %v", err)
	}
	wire, err := messenger.EncodeEventEnvelope(event, messenger.Metadata{
		ID: id, Kind: messenger.KindEvent, Name: event.Info().Name, SchemaVersion: event.Info().SchemaVersion,
		Source: "urn:service:kafka-outbox", Time: time.Now().UTC(), CorrelationID: id,
		ContentType: event.Info().ContentType,
	}, payload)
	if err != nil {
		t.Fatalf("encode Kafka envelope: %v", err)
	}
	return wire
}

func readKafkaDLQ(t *testing.T, client *kgo.Client) kafkaadapter.DLQRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll Kafka DLQ: %v", errs)
		}
		iterator := fetches.RecordIter()
		if iterator.Done() {
			continue
		}
		record, err := kafkaadapter.DecodeDLQRecord(iterator.Next().Value)
		if err != nil {
			t.Fatalf("decode Kafka DLQ: %v", err)
		}
		return record
	}
	t.Fatal("Kafka DLQ record was not published")
	return kafkaadapter.DLQRecord{}
}

func waitHandled(t *testing.T, handled <-chan string, expected string) {
	t.Helper()
	select {
	case got := <-handled:
		if got != expected {
			t.Fatalf("handled case = %q, want %q", got, expected)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for handled case %q", expected)
	}
}

func attemptCount(mu *sync.Mutex, attempts map[string]int, key string) int {
	mu.Lock()
	defer mu.Unlock()
	return attempts[key]
}

func eventuallyKafka(t *testing.T, timeout time.Duration, condition func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal(failure)
}
