package kafka

import (
	"strings"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

const testNamespace = "prod"

const (
	testSourceTopic = "prod.event.orders.created.v1"
	testConsumerID  = "billing"
	testWorkerID    = "worker"
	testMessageID   = "018f3f31-7bf2-7cc3-98c1-2b5f9b1f77ab"
	testDomainKey   = "order-42"
)

func TestTopicAndServiceNamesAreDeterministic(t *testing.T) {
	descriptor := messenger.MustEvent("orders.created", 2, messenger.JSON[struct{}]()).Info()
	source, err := Topic(testNamespace, descriptor)
	if err != nil {
		t.Fatalf("Topic: %v", err)
	}
	if source != "prod.event.orders.created.v2" {
		t.Fatalf("source topic = %q", source)
	}
	dotted, err := Topic("company.prod", descriptor)
	if err != nil || dotted != "company.prod.event.orders.created.v2" {
		t.Fatalf("dotted namespace topic = %q, %v", dotted, err)
	}
	retry, err := RetryTopic(source, testConsumerID, 3)
	if err != nil || retry != source+".gm."+testConsumerID+".retry.t3" {
		t.Fatalf("retry topic = %q, %v", retry, err)
	}
	replay, err := ReplayTopic(source, testConsumerID)
	if err != nil || replay != source+".gm."+testConsumerID+".replay" {
		t.Fatalf("replay topic = %q, %v", replay, err)
	}
	dlq, err := DLQTopic(source, testConsumerID)
	if err != nil || dlq != source+".gm."+testConsumerID+".dlq" {
		t.Fatalf("DLQ topic = %q, %v", dlq, err)
	}
}

func TestTopicRejectsQueriesAndUnreadableFallbacks(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		info      messenger.DescriptorInfo
	}{
		{name: "query", namespace: testNamespace, info: messenger.MustQuery[struct{}, struct{}](
			"orders.find", 1, messenger.JSON[struct{}]()).Info()},
		{name: "invalid namespace", namespace: "prod_cluster", info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "non ASCII namespace", namespace: "прод", info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "reserved event namespace segment", namespace: "company.event.prod", info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "reserved command namespace segment", namespace: "company.command", info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "reserved service namespace segment", namespace: "company.gm.prod", info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "reserved service descriptor segment", namespace: testNamespace, info: messenger.MustEvent(
			"orders.gm.created", 1, messenger.JSON[struct{}]()).Info()},
		{name: "too long", namespace: strings.Repeat("a", 230), info: messenger.MustEvent(
			"orders.created", 1, messenger.JSON[struct{}]()).Info()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Topic(test.namespace, test.info); err == nil {
				t.Fatal("Topic unexpectedly accepted invalid input")
			}
		})
	}
}

func TestConsumerAndTransactionIdentitiesUseStableInstance(t *testing.T) {
	t.Parallel()
	group, err := ConsumerGroup(testSourceTopic, testConsumerID)
	if err != nil {
		t.Fatalf("ConsumerGroup: %v", err)
	}
	if group != testSourceTopic+".gm."+testConsumerID {
		t.Fatalf("group = %q", group)
	}
	instance, err := groupInstanceID("host-a", 2)
	if err != nil || instance != "host-a.w2" {
		t.Fatalf("group instance = %q, %v", instance, err)
	}
	transaction, err := transactionalID(group, "host-a", 2)
	const expectedTransaction = "gm.tx.v1.91c7e60eb396a445c73a0d50951341be2174d77c4cff2aa6e90a8618657e218c.w2"
	if err != nil || transaction != expectedTransaction {
		t.Fatalf("transaction ID = %q, %v", transaction, err)
	}
	repeated, err := transactionalID(group, "host-a", 2)
	if err != nil || repeated != transaction {
		t.Fatalf("repeated transaction ID = %q, %v", repeated, err)
	}
}

func TestConsumerServiceNamesRejectAmbiguousSourceBoundary(t *testing.T) {
	t.Parallel()
	ambiguousSource := testSourceTopic + ".gm.audit.event.orders.created.v1"
	canonicalGroup, err := ConsumerGroup(testSourceTopic, "audit.event.orders.created.v1.gm.worker")
	if err != nil {
		t.Fatalf("canonical ConsumerGroup: %v", err)
	}
	if collided := ambiguousSource + serviceMarker + testWorkerID; canonicalGroup != collided {
		t.Fatalf("invalid collision premise: %q != %q", canonicalGroup, collided)
	}
	checks := []struct {
		name string
		call func() (string, error)
	}{
		{name: "retry", call: func() (string, error) { return RetryTopic(ambiguousSource, testWorkerID, 0) }},
		{name: "replay", call: func() (string, error) { return ReplayTopic(ambiguousSource, testWorkerID) }},
		{name: "DLQ", call: func() (string, error) { return DLQTopic(ambiguousSource, testWorkerID) }},
		{name: "group", call: func() (string, error) { return ConsumerGroup(ambiguousSource, testWorkerID) }},
	}
	for _, check := range checks {
		check := check
		t.Run(check.name, func(t *testing.T) {
			t.Parallel()
			if _, err := check.call(); err == nil {
				t.Fatal("service name unexpectedly accepted a source containing the reserved marker")
			}
		})
	}
	if _, err := ConsumerGroup(testSourceTopic, ""); err == nil {
		t.Fatal("consumer group unexpectedly accepted an empty consumer ID")
	}
}

func TestTransactionalIDSeparatesTupleBoundaries(t *testing.T) {
	t.Parallel()
	first, err := transactionalID(testSourceTopic+".gm.alpha", "blue.green", 1)
	if err != nil {
		t.Fatalf("first transactionalID: %v", err)
	}
	second, err := transactionalID(testSourceTopic+".gm.alpha.blue", "green", 1)
	if err != nil {
		t.Fatalf("second transactionalID: %v", err)
	}
	if first == second {
		t.Fatalf("transactional IDs collide: %q", first)
	}
}

func TestExpectedRecordKeyUsesDomainKeyOrMessageID(t *testing.T) {
	id := mustMessageID(t, testMessageID)
	metadata := messenger.Metadata{ID: id}
	if got := string(expectedRecordKey(metadata)); got != id.String() {
		t.Fatalf("fallback key = %q", got)
	}
	metadata.Key = testDomainKey
	if got := string(expectedRecordKey(metadata)); got != testDomainKey {
		t.Fatalf("domain key = %q", got)
	}
}

func mustMessageID(t *testing.T, value string) messenger.MessageID {
	t.Helper()
	id, err := messenger.ParseMessageID(value)
	if err != nil {
		t.Fatalf("ParseMessageID: %v", err)
	}
	return id
}
