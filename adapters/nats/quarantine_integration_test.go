package nats_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/assurrussa/gomessenger/adapters/nats"
)

func TestQuarantineAllowsFollowingDelivery(t *testing.T) {
	for _, batchSize := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("batch-%d", batchSize), func(t *testing.T) {
			testQuarantineProgress(t, batchSize)
		})
	}
}

func testQuarantineProgress(t *testing.T, batchSize int) {
	t.Helper()
	connection := startJetStream(t)
	ensureTestStream(t, connection)
	config := testHandlerConfig("quarantine-progress")
	command := messenger.MustCommand("quarantine.progress", 1, messenger.JSON[testPayload]())
	handled := make(chan struct{}, 1)
	var consumer *nats.Consumer
	var err error
	store := openInbox(t)
	if batchSize == 0 {
		consumer, err = nats.NewCommandConsumer(connection, store, command,
			func(context.Context, messenger.Message[testPayload]) error { handled <- struct{}{}; return nil }, config)
	} else {
		consumer, err = nats.NewBatchCommandConsumer(connection, store, command,
			func(_ context.Context, messages []messenger.Message[testPayload]) (messenger.BatchResult, error) {
				result := messenger.BatchResult{}
				for _, message := range messages {
					result.Items = append(result.Items, messenger.BatchItemResult{Key: messenger.BatchItemKey{
						Source: message.Metadata.Source, MessageID: message.Metadata.ID,
					}})
					handled <- struct{}{}
				}
				return result, nil
			}, config, messenger.BatchConfig{MaxMessages: batchSize})
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
	waitReady(t, consumer)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := nats.Subject(testNamespace, command.Info())
	if err != nil {
		t.Fatal(err)
	}
	headers := make(natsio.Header)
	for index := range 65 {
		headers[fmt.Sprintf("X-Test-%02d", index)] = []string{"value"}
	}
	if _, err := js.PublishMsg(ctx, &natsio.Msg{Subject: subject, Header: headers, Data: []byte("invalid")}); err != nil {
		t.Fatal(err)
	}
	publishCommand(t, connection, command, "018f4f2c-4a00-7000-8000-000000000099", "quarantine-valid")
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("poison message blocked valid delivery")
	}
	waitForConsumerEmpty(t, connection, config.ConsumerID)
	stream, err := js.Stream(ctx, testDLQStreamName)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	record, err := nats.DecodeDLQRecord(stored.Data)
	if err != nil || record.Quarantine == nil || record.Quarantine.HeaderCount != 65 {
		t.Fatalf("quarantine: %+v, %v", record, err)
	}
	publisher := &replayPublisher{}
	if _, err := nats.ReplayDLQ(ctx, publisher, record); !errors.Is(err, nats.ErrQuarantineReplay) || publisher.message != nil {
		t.Fatalf("quarantine replay published: %v", err)
	}
}
