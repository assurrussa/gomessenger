package messenger_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type orderedChange struct {
	OrderID          string `json:"orderId"`
	AggregateVersion int64  `json:"aggregateVersion"`
	Status           string `json:"status"`
}

type orderedProjection struct {
	version int64
	status  string
}

// nextOrderedProjection is sample application policy, not a broker guarantee.
// The host must load and lock the state for change.OrderID, then persist the
// returned state in the same transaction as Inbox completion. This pure helper
// cannot prove those database boundaries or serialize concurrent consumers.
func nextOrderedProjection(current orderedProjection, change orderedChange) (orderedProjection, error) {
	if change.OrderID == "" || change.AggregateVersion <= 0 ||
		(change.Status != "created" && change.Status != "paid") {
		return current, messenger.Permanent(errors.New("invalid aggregate change"))
	}
	if change.AggregateVersion <= current.version {
		// The Inbox suppresses completed message identities before the handler.
		// A different message with an old version needs explicit reconciliation.
		return current, messenger.Permanent(errors.New("stale or conflicting aggregate version"))
	}
	if change.AggregateVersion != current.version+1 {
		return current, messenger.DeferAfter(errors.New("waiting for predecessor"), time.Second)
	}
	if (current.version == 0 && change.Status == "created") ||
		(current.status == "created" && change.Status == "paid") {
		return orderedProjection{version: change.AggregateVersion, status: change.Status}, nil
	}
	return current, messenger.Permanent(errors.New("invalid order transition"))
}

func ExampleDeferAfter_aggregateVersion() {
	changed := messenger.MustEvent("orders.changed", 2, messenger.JSON[orderedChange]())
	created := orderedChange{OrderID: "order-42", AggregateVersion: 1, Status: "created"}
	paid := orderedChange{OrderID: "order-42", AggregateVersion: 2, Status: "paid"}
	fmt.Println("schema", changed.Info().SchemaVersion, "aggregate versions", created.AggregateVersion, paid.AggregateVersion)

	// Assume Created failed without committing and moved to a retry topic.
	// Paid can arrive first even with the same Kafka key and one worker.
	// Calls below model that delivery order; no broker, timer or SQL is run.
	current := orderedProjection{}
	next, err := nextOrderedProjection(current, paid)
	delay, deferred := messenger.DeferDelay(err)
	fmt.Println("paid deferred", deferred, delay, "unchanged", next == current)

	current, err = nextOrderedProjection(current, created)
	if err != nil {
		panic(err)
	}
	fmt.Println(current.status, current.version)

	current, err = nextOrderedProjection(current, paid)
	if err != nil {
		panic(err)
	}
	fmt.Println(current.status, current.version)

	// Output:
	// schema 2 aggregate versions 1 2
	// paid deferred true 1s unchanged true
	// created 1
	// paid 2
}

func TestAggregateVersionRetryOrder(t *testing.T) {
	t.Parallel()
	created := orderedChange{OrderID: "order-42", AggregateVersion: 1, Status: "created"}
	paid := orderedChange{OrderID: "order-42", AggregateVersion: 2, Status: "paid"}
	current := orderedProjection{}

	// Repeated successor delivery must not advance or otherwise mutate state.
	for range 2 {
		next, err := nextOrderedProjection(current, paid)
		delay, deferred := messenger.DeferDelay(err)
		if !deferred || delay != time.Second || next != current {
			t.Fatalf("gap: next=%+v error=%v delay=%v", next, err, delay)
		}
		if _, retry := messenger.RetryDelay(err); retry || messenger.IsPermanent(err) {
			t.Fatalf("gap must defer rather than consume a failed attempt: %v", err)
		}
	}
	for _, change := range []orderedChange{created, paid} {
		next, err := nextOrderedProjection(current, change)
		if err != nil {
			t.Fatal(err)
		}
		if next.version != change.AggregateVersion || next.status != change.Status {
			t.Fatalf("applied state=%+v change=%+v", next, change)
		}
		current = next
	}

	// This is application reconciliation, not a second Inbox deduplication key.
	next, err := nextOrderedProjection(current, created)
	if !messenger.IsPermanent(err) || next != current {
		t.Fatalf("stale version: next=%+v error=%v", next, err)
	}
}

func TestAggregateVersionRejectsInvalidTransitions(t *testing.T) {
	t.Parallel()
	for _, change := range []orderedChange{
		{OrderID: "", AggregateVersion: 1, Status: "created"},
		{OrderID: "order-42", AggregateVersion: 0, Status: "created"},
		{OrderID: "order-42", AggregateVersion: -1, Status: "created"},
		{OrderID: "order-42", AggregateVersion: 1, Status: "paid"},
		{OrderID: "order-42", AggregateVersion: 2, Status: "unknown"},
	} {
		current := orderedProjection{}
		next, err := nextOrderedProjection(current, change)
		if !messenger.IsPermanent(err) || next != current {
			t.Fatalf("invalid change=%+v next=%+v error=%v", change, next, err)
		}
	}
}

func TestAggregateVersionIsNotSchemaVersion(t *testing.T) {
	t.Parallel()
	changed := messenger.MustEvent("orders.changed", 2, messenger.JSON[orderedChange]())
	olderSchema := messenger.MustEvent("orders.changed", 1, messenger.JSON[orderedChange]())
	info := changed.Info()
	ids := [...]messenger.MessageID{{15: 1}, {15: 2}}
	current := orderedProjection{}
	for index, change := range []orderedChange{
		{OrderID: "order-42", AggregateVersion: 1, Status: "created"},
		{OrderID: "order-42", AggregateVersion: 2, Status: "paid"},
	} {
		metadata := messenger.Metadata{
			ID: ids[index], CorrelationID: ids[index], Kind: info.Kind, Name: info.Name,
			SchemaVersion: info.SchemaVersion, ContentType: info.ContentType,
			Source: "urn:service:orders", Key: change.OrderID,
			Time: time.Unix(1_700_000_000, 0).UTC(),
		}
		data, err := messenger.EncodeEventEnvelope(changed, metadata, change)
		if err != nil {
			t.Fatal(err)
		}
		message, err := messenger.DecodeEvent(changed, data)
		if err != nil {
			t.Fatal(err)
		}
		if message.Metadata.SchemaVersion != 2 || message.Payload != change ||
			message.Metadata.ID != metadata.ID || message.Metadata.Key != change.OrderID {
			t.Fatalf("decoded message=%+v", message)
		}
		if _, err := messenger.DecodeEvent(olderSchema, data); !errors.Is(err, messenger.ErrDescriptorConflict) {
			t.Fatalf("wrong wire schema accepted: %v", err)
		}
		current, err = nextOrderedProjection(current, message.Payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	if current.version != 2 || current.status != "paid" {
		t.Fatalf("final projection=%+v", current)
	}
}
