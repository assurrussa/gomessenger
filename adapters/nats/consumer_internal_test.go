package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	testDLQConsumerID  = "worker"
	testDLQSubject     = "messages.one"
	testDLQContentType = "application/json"
	testDLQData        = `{"value":1}`
)

func TestConsumerFinalizationTimeout(t *testing.T) {
	config := HandlerConfig{}
	applyConsumerDefaults(&config)
	if config.FinalizationTimeout != defaultFinalizationTimeout {
		t.Fatalf("default finalization timeout = %s, want %s", config.FinalizationTimeout, defaultFinalizationTimeout)
	}
	if got := handlerTransactionTimeout(30*time.Second, 12*time.Second); got != 42*time.Second {
		t.Fatalf("transaction timeout = %s, want 42s", got)
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if got := handlerTransactionTimeout(maxDuration-time.Second, 2*time.Second); got != maxDuration {
		t.Fatalf("saturated transaction timeout = %s, want %s", got, maxDuration)
	}
}

func TestDLQDedupIDIncludesCompleteSourceWireIdentity(t *testing.T) {
	baseHeaders := map[string][]string{"Ce-Id": {"event-1"}, "Content-Type": {testDLQContentType}}
	base := dlqDedupID(testDLQConsumerID, testDLQSubject, WireCloudEventsBinary, baseHeaders, []byte(testDLQData))
	tests := []struct {
		name       string
		consumerID string
		subject    string
		mode       WireMode
		headers    map[string][]string
		data       []byte
	}{
		{
			name: "consumer", consumerID: "other", subject: testDLQSubject, mode: WireCloudEventsBinary,
			headers: baseHeaders, data: []byte(testDLQData),
		},
		{
			name: "subject", consumerID: testDLQConsumerID, subject: "messages.two", mode: WireCloudEventsBinary,
			headers: baseHeaders, data: []byte(testDLQData),
		},
		{
			name: "wire mode", consumerID: testDLQConsumerID, subject: testDLQSubject, mode: WireNative,
			headers: baseHeaders, data: []byte(testDLQData),
		},
		{
			name: "headers", consumerID: testDLQConsumerID, subject: testDLQSubject, mode: WireCloudEventsBinary,
			headers: map[string][]string{"Ce-Id": {"event-2"}, "Content-Type": {testDLQContentType}},
			data:    []byte(testDLQData),
		},
		{
			name: "data", consumerID: testDLQConsumerID, subject: testDLQSubject, mode: WireCloudEventsBinary,
			headers: baseHeaders, data: []byte(`{"value":2}`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := dlqDedupID(test.consumerID, test.subject, test.mode, test.headers, test.data)
			if got == base {
				t.Fatalf("dedup ID did not change: %q", got)
			}
		})
	}
	caseVariant := map[string][]string{"ce-id": {"event-1"}, "content-type": {testDLQContentType}}
	if got := dlqDedupID(testDLQConsumerID, testDLQSubject, WireCloudEventsBinary, caseVariant, []byte(testDLQData)); got != base {
		t.Fatalf("header case changed dedup ID: %q != %q", got, base)
	}
}

func TestMissingPullHeartbeatIsRecoverable(t *testing.T) {
	if !recoverablePullError(fmt.Errorf("wrapped: %w", jetstream.ErrNoHeartbeat)) {
		t.Fatal("missing heartbeat was classified as terminal")
	}
	if recoverablePullError(errors.New("consumer deleted")) {
		t.Fatal("unrelated pull error was classified as recoverable")
	}
}

func TestConsumerConcurrentRunAndShutdownTransitionIsAtomic(t *testing.T) {
	transitionEntered := make(chan struct{})
	releaseTransition := make(chan struct{})
	consumer := &Consumer{
		state: consumerNew,
		done:  make(chan struct{}),
		beforeShutdownTransition: func() {
			close(transitionEntered)
			<-releaseTransition
		},
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- consumer.Shutdown(t.Context()) }()
	<-transitionEntered

	type result struct {
		err        error
		panicValue any
	}
	runAttempting := make(chan struct{})
	runDone := make(chan result, 1)
	go func() {
		close(runAttempting)
		var runResult result
		defer func() {
			runResult.panicValue = recover()
			runDone <- runResult
		}()
		runResult.err = consumer.Run(t.Context())
	}()
	<-runAttempting
	time.Sleep(10 * time.Millisecond)
	select {
	case runResult := <-runDone:
		close(releaseTransition)
		<-shutdownDone
		t.Fatalf("run crossed the locked shutdown transition: %#v", runResult)
	default:
		close(releaseTransition)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case runResult := <-runDone:
		if runResult.panicValue != nil || !errors.Is(runResult.err, ErrConsumerClosed) {
			t.Fatalf("run result = %#v", runResult)
		}
	case <-time.After(time.Second):
		t.Fatal("run remained blocked after shutdown transition")
	}
}

func TestConsumerRejectsSecondRunWhileDraining(t *testing.T) {
	consumer := &Consumer{state: consumerDraining, runStarted: true, done: make(chan struct{})}
	if err := consumer.Run(t.Context()); !errors.Is(err, messenger.ErrRuntimeRunning) {
		t.Fatalf("second run while draining = %v", err)
	}
}

func TestReplayAttemptGenerationIsConsumerScoped(t *testing.T) {
	replayID := replayIDPrefix + strings.Repeat("a", 64)
	headers := natsio.Header{
		replayIDHeader:       {replayID},
		replayConsumerHeader: {testDLQConsumerID},
	}
	if got := replayAttemptGeneration(headers, testDLQConsumerID); got != replayID {
		t.Fatalf("generation = %q, want %q", got, replayID)
	}
	if got := replayAttemptGeneration(headers, "other"); got != "" {
		t.Fatalf("generation for another consumer = %q", got)
	}
	headers[replayIDHeader] = []string{"gm-replay-invalid"}
	if got := replayAttemptGeneration(headers, testDLQConsumerID); got != "" {
		t.Fatalf("invalid generation = %q", got)
	}
}

func TestTerminalRecordAttemptFallsBackToBrokerDelivery(t *testing.T) {
	if got := terminalRecordAttempt(2, 7); got != 2 {
		t.Fatalf("handler attempt = %d, want 2", got)
	}
	if got := terminalRecordAttempt(0, 7); got != 7 {
		t.Fatalf("delivery fallback = %d, want 7", got)
	}
	if got := terminalRecordAttempt(0, 0); got != 1 {
		t.Fatalf("minimum fallback = %d, want 1", got)
	}
}

func TestDeadLetterAcknowledgementWaitsForBrokerConfirmation(t *testing.T) {
	message := &scriptedJetStreamMessage{doubleAckErrors: []error{errors.New("confirmation lost"), nil}}
	consumer := &Consumer{config: HandlerConfig{
		ConsumerID: testDLQConsumerID,
		BaseRetry:  time.Nanosecond,
		MaxRetry:   time.Nanosecond,
	}, clock: time.Now}
	if !consumer.acknowledgeDeadLetteredMessage(t.Context(), message, messenger.MessageID{}, 1) {
		t.Fatal("dead-letter acknowledgement was not confirmed")
	}
	if message.doubleAckCalls != 2 {
		t.Fatalf("DoubleAck calls = %d, want 2", message.doubleAckCalls)
	}
	if message.termCalls != 0 {
		t.Fatalf("asynchronous TERM calls = %d, want 0", message.termCalls)
	}
	if !message.sawDeadline {
		t.Fatal("DoubleAck did not receive a bounded context")
	}
	alreadyAcknowledged := &scriptedJetStreamMessage{doubleAckErrors: []error{jetstream.ErrMsgAlreadyAckd}}
	if !consumer.acknowledgeDeadLetteredMessage(
		t.Context(), alreadyAcknowledged, messenger.MessageID{}, 1,
	) {
		t.Fatal("already-acknowledged message was treated as a failed DLQ hand-off")
	}
	if alreadyAcknowledged.doubleAckCalls != 1 {
		t.Fatalf("already-acknowledged DoubleAck calls = %d, want 1", alreadyAcknowledged.doubleAckCalls)
	}
}

type scriptedJetStreamMessage struct {
	doubleAckErrors []error
	doubleAckCalls  int
	termCalls       int
	sawDeadline     bool
}

func (*scriptedJetStreamMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{}, nil
}

func (*scriptedJetStreamMessage) Data() []byte                     { return nil }
func (*scriptedJetStreamMessage) Headers() natsio.Header           { return nil }
func (*scriptedJetStreamMessage) Subject() string                  { return testDLQSubject }
func (*scriptedJetStreamMessage) Reply() string                    { return "reply" }
func (*scriptedJetStreamMessage) Ack() error                       { return nil }
func (*scriptedJetStreamMessage) Nak() error                       { return nil }
func (*scriptedJetStreamMessage) NakWithDelay(time.Duration) error { return nil }
func (*scriptedJetStreamMessage) InProgress() error                { return nil }

func (message *scriptedJetStreamMessage) DoubleAck(ctx context.Context) error {
	message.doubleAckCalls++
	_, message.sawDeadline = ctx.Deadline()
	index := message.doubleAckCalls - 1
	if index < len(message.doubleAckErrors) {
		return message.doubleAckErrors[index]
	}
	return nil
}

func (message *scriptedJetStreamMessage) Term() error {
	message.termCalls++
	return nil
}

func (message *scriptedJetStreamMessage) TermWithReason(string) error {
	message.termCalls++
	return nil
}
