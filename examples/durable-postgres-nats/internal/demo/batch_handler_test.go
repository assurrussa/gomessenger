//nolint:testpackage // Tests exercise the batch handler's package-local classification boundary.
package demo

import (
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestClassifyBatchOrderOutcomes(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	tests := []struct {
		name      string
		orderID   string
		scenario  string
		permanent bool
		retry     bool
	}{
		{name: "successful item", orderID: "order-success", scenario: ScenarioSuccess},
		{name: "retry first attempt", orderID: "retry", scenario: ScenarioRetry, retry: true},
		{name: "permanent first attempt", orderID: "permanent", scenario: ScenarioDLQ, permanent: true},
		{name: "unsupported scenario", orderID: "order-unknown", scenario: "unexpected", permanent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			message := messenger.Message[OrderCreated]{Payload: OrderCreated{
				OrderID: test.orderID, Scenario: test.scenario,
			}}
			decision, err := application.classifyBatchOrder(message)
			if decision.message.Payload.OrderID != test.orderID {
				t.Fatalf("decision = %#v", decision)
			}
			if messenger.IsPermanent(err) != test.permanent {
				t.Fatalf("permanent error = %v, want %t", err, test.permanent)
			}
			delay, retry := messenger.RetryDelay(err)
			if retry != test.retry {
				t.Fatalf("retry error = %v, want %t", err, test.retry)
			}
			if retry && delay != 300*time.Millisecond {
				t.Fatalf("retry delay = %s, want 300ms", delay)
			}
		})
	}
}

func TestClassifyBatchOrderRetryAndReplayCanSucceed(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	for _, scenario := range []string{ScenarioRetry, ScenarioDLQ} {
		message := messenger.Message[OrderCreated]{Payload: OrderCreated{
			OrderID: "replay-" + scenario, Scenario: scenario,
		}}
		if _, err := application.classifyBatchOrder(message); err == nil {
			t.Fatalf("first %s classification error = nil", scenario)
		}
		decision, err := application.classifyBatchOrder(message)
		if err != nil || decision.attempt != 2 {
			t.Fatalf("second %s classification = (%#v, %v)", scenario, decision, err)
		}
	}
}
