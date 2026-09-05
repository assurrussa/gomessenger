package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type quarantinePublisher struct {
	jetstream.JetStream
	calls     int
	confirmed bool
	data      []byte
	cancel    context.CancelFunc
}

func (p *quarantinePublisher) PublishMsg(_ context.Context, message *natsio.Msg,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
		return nil, errors.New("DLQ unavailable")
	}
	if p.calls == 1 {
		return nil, errors.New("temporary DLQ outage")
	}
	p.data = message.Data
	p.confirmed = true
	return &jetstream.PubAck{Stream: "QUARANTINE"}, nil
}

type quarantineMessage struct {
	scriptedJetStreamMessage
	publisher *quarantinePublisher
	headers   natsio.Header
}

func (m *quarantineMessage) Headers() natsio.Header { return m.headers }
func (*quarantineMessage) Data() []byte             { return []byte("malformed wire") }
func (m *quarantineMessage) DoubleAck(ctx context.Context) error {
	if !m.publisher.confirmed {
		return errors.New("ACK preceded PubAck")
	}
	return m.scriptedJetStreamMessage.DoubleAck(ctx)
}

type quarantineBatchMessage struct{ source *quarantineMessage }

func (m quarantineBatchMessage) AckSync(_ ...natsio.AckOpt) error {
	return m.source.DoubleAck(context.Background())
}
func (quarantineBatchMessage) InProgress(_ ...natsio.AckOpt) error                    { return nil }
func (quarantineBatchMessage) NakWithDelay(_ time.Duration, _ ...natsio.AckOpt) error { return nil }

func TestQuarantineHandoffRetriesAndCancellation(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, shutdown := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch_%t_shutdown_%t", batch, shutdown), func(t *testing.T) {
				testQuarantineHandoff(t, batch, shutdown)
			})
		}
	}
}

func testQuarantineHandoff(t *testing.T, batch, shutdown bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	publisher := &quarantinePublisher{}
	if shutdown {
		publisher.cancel = cancel
	}
	message := &quarantineMessage{
		publisher: publisher, headers: make(natsio.Header),
		scriptedJetStreamMessage: scriptedJetStreamMessage{doubleAckErrors: []error{errors.New("lost ACK response"), nil}},
	}
	for index := range 65 {
		message.headers[fmt.Sprintf("X-Quarantine-%02d", index)] = []string{"value"}
	}
	consumer := &Consumer{js: publisher, clock: time.Now, config: HandlerConfig{
		ConsumerID: "quarantine-worker", DLQSubject: "quarantine.dlq", WireMode: WireNative,
		BaseRetry: time.Nanosecond, MaxRetry: time.Nanosecond,
	}}
	var acknowledged bool
	if batch {
		outcome := consumer.deadLetterAndAcknowledgeNATSBatch(ctx,
			&natsBatchDelivery{
				broker:    &natsio.Msg{Subject: message.Subject(), Data: message.Data(), Header: message.Headers()},
				brokerMsg: quarantineBatchMessage{source: message},
			}, natsBatchFinalOutcome{failureKind: testQuarantineFailure, err: messenger.ErrInvalidMessage})
		acknowledged = outcome.acknowledged
	} else {
		acknowledged = consumer.deadLetterAndAcknowledge(
			ctx, message, decodedMessage{}, 1, testQuarantineFailure, messenger.ErrInvalidMessage,
		)
	}
	if shutdown {
		if acknowledged || message.doubleAckCalls != 0 {
			t.Fatal("ACK during failed publication")
		}
		return
	}
	if !acknowledged || publisher.calls != 2 || message.doubleAckCalls != 2 {
		t.Fatalf("handoff: ack=%t publications=%d ACKs=%d", acknowledged, publisher.calls, message.doubleAckCalls)
	}
	record, err := DecodeDLQRecord(publisher.data)
	if err != nil || record.Quarantine == nil {
		t.Fatalf("published quarantine: %+v %v", record, err)
	}
}
