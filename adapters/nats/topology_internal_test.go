package nats

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	testExactSubject          = "events.media.done"
	testSingleWildcardSubject = "events.*.done"
	testTailWildcardSubject   = "events.>"
	testConsumerFilterSubject = "events.*"
	testTopologyStream        = "MESSAGES"
	testTopologyConsumer      = "worker"
)

func TestSubjectPatternCovers(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
		want    bool
	}{
		{name: "exact", desired: testExactSubject, current: testExactSubject, want: true},
		{name: "single wildcard covers literal", desired: testSingleWildcardSubject, current: testExactSubject, want: true},
		{name: "single wildcard covers wildcard", desired: testSingleWildcardSubject, current: testSingleWildcardSubject, want: true},
		{name: "terminal wildcard covers literal suffix", desired: testTailWildcardSubject, current: testExactSubject, want: true},
		{name: "terminal wildcard covers narrower wildcard", desired: testTailWildcardSubject, current: "events.media.*", want: true},
		{name: "terminal wildcard needs another token", desired: "events.media.>", current: "events.media", want: false},
		{name: "literal cannot cover wildcard", desired: testExactSubject, current: testSingleWildcardSubject, want: false},
		{
			name:    "single wildcard cannot cover terminal wildcard",
			desired: testConsumerFilterSubject, current: testTailWildcardSubject, want: false,
		},
		{name: "different prefix", desired: "commands.>", current: testExactSubject, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := subjectPatternCovers(test.desired, test.current); got != test.want {
				t.Fatalf("subjectPatternCovers(%q, %q) = %t, want %t", test.desired, test.current, got, test.want)
			}
		})
	}
}

func TestValidateTopologyAcceptsUnlimitedDeliveryAndRejectsInvalidPatterns(t *testing.T) {
	topology := Topology{
		SpecVersion: TopologySpecVersion,
		Streams:     []StreamSpec{DevStream(testTopologyStream, testTailWildcardSubject)},
		Consumers: []ConsumerSpec{{
			Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: testConsumerFilterSubject, AckWait: 1,
			MaxDeliver: -1, MaxAckPending: 1,
		}},
	}
	if err := ValidateTopology(topology); err != nil {
		t.Fatalf("unlimited delivery topology: %v", err)
	}
	topology.Streams[0].Subjects[0] = "events.>.invalid"
	if err := ValidateTopology(topology); err == nil {
		t.Fatal("invalid stream subject pattern accepted")
	}
}

func TestValidateTopologyRejectsConsumerFilterOutsideDeclaredStream(t *testing.T) {
	topology := Topology{
		SpecVersion: TopologySpecVersion,
		Streams:     []StreamSpec{DevStream(testTopologyStream, "orders.>")},
		Consumers: []ConsumerSpec{{
			Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: "payments.>",
			AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1,
		}},
	}
	if err := ValidateTopology(topology); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("out-of-scope consumer filter error = %v", err)
	}
}

func TestValidateTopologyRejectsInvalidJetStreamResourceNames(t *testing.T) {
	validTopology := func() Topology {
		return Topology{
			SpecVersion: TopologySpecVersion,
			Streams:     []StreamSpec{DevStream(testTopologyStream, testTailWildcardSubject)},
			Consumers: []ConsumerSpec{{
				Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: testConsumerFilterSubject,
				AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1,
			}},
		}
	}
	invalidNames := []struct {
		name  string
		value string
	}{
		{name: "dot", value: "BAD.NAME"},
		{name: "space", value: "BAD NAME"},
		{name: "tab", value: "BAD\tNAME"},
		{name: "wildcard", value: "BAD*NAME"},
		{name: "terminal wildcard", value: "BAD>NAME"},
		{name: "slash", value: "BAD/NAME"},
		{name: "backslash", value: `BAD\NAME`},
		{name: "too long", value: strings.Repeat("A", maxJetStreamResourceNameBytes+1)},
	}
	for _, invalid := range invalidNames {
		t.Run("stream/"+invalid.name, func(t *testing.T) {
			topology := validTopology()
			topology.Streams[0].Name = invalid.value
			if err := ValidateTopology(topology); err == nil {
				t.Fatalf("invalid stream name %q accepted", invalid.value)
			}
		})
		t.Run("consumer stream/"+invalid.name, func(t *testing.T) {
			topology := validTopology()
			topology.Consumers[0].Stream = invalid.value
			if err := ValidateTopology(topology); err == nil {
				t.Fatalf("invalid consumer stream name %q accepted", invalid.value)
			}
		})
		t.Run("consumer/"+invalid.name, func(t *testing.T) {
			topology := validTopology()
			topology.Consumers[0].Name = invalid.value
			if err := ValidateTopology(topology); err == nil {
				t.Fatalf("invalid consumer name %q accepted", invalid.value)
			}
		})
	}
}

func TestValidateTopologyRejectsServerNormalizedStreamLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*StreamSpec)
	}{
		{name: "zero max messages", mutate: func(stream *StreamSpec) { stream.MaxMessages = 0 }},
		{name: "max messages below unlimited", mutate: func(stream *StreamSpec) { stream.MaxMessages = -2 }},
		{name: "zero max bytes", mutate: func(stream *StreamSpec) { stream.MaxBytes = 0 }},
		{name: "max bytes below unlimited", mutate: func(stream *StreamSpec) { stream.MaxBytes = -2 }},
		{name: "zero duplicate window", mutate: func(stream *StreamSpec) { stream.Duplicates = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := DevStream(testTopologyStream, testTailWildcardSubject)
			test.mutate(&stream)
			topology := Topology{SpecVersion: TopologySpecVersion, Streams: []StreamSpec{stream}}
			if err := ValidateTopology(topology); err == nil {
				t.Fatal("server-normalized stream limit accepted")
			}
		})
	}
}

func TestValidateTopologyRejectsInvalidReplicaCounts(t *testing.T) {
	validTopology := func() Topology {
		return Topology{
			SpecVersion: TopologySpecVersion,
			Streams:     []StreamSpec{DevStream(testTopologyStream, testTailWildcardSubject)},
			Consumers: []ConsumerSpec{{
				Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: testConsumerFilterSubject,
				AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1,
			}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*Topology)
	}{
		{
			name: "stream above server maximum",
			mutate: func(topology *Topology) {
				topology.Streams[0].Replicas = maxJetStreamReplicas + 1
			},
		},
		{
			name: "negative consumer replicas",
			mutate: func(topology *Topology) {
				topology.Consumers[0].Replicas = -1
			},
		},
		{
			name: "consumer above server maximum",
			mutate: func(topology *Topology) {
				topology.Streams = nil
				topology.Consumers[0].Replicas = maxJetStreamReplicas + 1
			},
		},
		{
			name: "consumer exceeds declared stream",
			mutate: func(topology *Topology) {
				topology.Consumers[0].Replicas = topology.Streams[0].Replicas + 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topology := validTopology()
			test.mutate(&topology)
			if err := ValidateTopology(topology); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("replica validation error = %v", err)
			}
		})
	}
}

func TestCompareStreamTreatsUnlimitedMessageSizeAsAConflict(t *testing.T) {
	desired := DevStream(testTopologyStream, testTailWildcardSubject)
	current := streamConfig(desired)
	current.MaxMsgSize = -1
	action, _ := compareStream(current, desired)
	if action != ChangeConflict {
		t.Fatalf("action = %q, want %q", action, ChangeConflict)
	}
}

func TestDevDLQStreamReservesRecordAndTransportCapacity(t *testing.T) {
	stream := DevDLQStream(testTopologyStream, "messages.dlq")
	if stream.MaxMsgSize != DefaultMaxDLQMessageBytes ||
		stream.MaxMsgSize <= DevStream(testTopologyStream, "messages.>").MaxMsgSize {
		t.Fatalf("DLQ max message size = %d, want %d", stream.MaxMsgSize, DefaultMaxDLQMessageBytes)
	}
}

func TestCompareConsumerRejectsUndeclaredDeliveryOptions(t *testing.T) {
	desired := ConsumerSpec{
		Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: testConsumerFilterSubject,
		AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 2, Replicas: 1,
	}
	pausedUntil := time.Now().Add(time.Hour)
	tests := []struct {
		name   string
		mutate func(*jetstream.ConsumerConfig)
	}{
		{name: "delivery policy", mutate: func(config *jetstream.ConsumerConfig) {
			config.DeliverPolicy = jetstream.DeliverNewPolicy
		}},
		{name: "server backoff", mutate: func(config *jetstream.ConsumerConfig) {
			config.BackOff = []time.Duration{time.Second}
		}},
		{name: "headers only", mutate: func(config *jetstream.ConsumerConfig) { config.HeadersOnly = true }},
		{name: "push delivery", mutate: func(config *jetstream.ConsumerConfig) {
			config.DeliverSubject = "_INBOX.push"
		}},
		{name: "pause", mutate: func(config *jetstream.ConsumerConfig) { config.PauseUntil = &pausedUntil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := consumerConfig(desired)
			test.mutate(&current)
			if action, _ := compareConsumer(current, desired); action != ChangeConflict {
				t.Fatalf("action = %q, want %q", action, ChangeConflict)
			}
		})
	}
}

func TestCompareConsumerPreservesObservabilityAndServerPullCapacity(t *testing.T) {
	desired := ConsumerSpec{
		Stream: testTopologyStream, Name: testTopologyConsumer, FilterSubject: testConsumerFilterSubject,
		AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 2, Replicas: 1,
	}
	current := consumerConfig(desired)
	current.Metadata = map[string]string{"owner": "platform"}
	current.SampleFrequency = "10%"
	current.MaxWaiting = 512
	if action, _ := compareConsumer(current, desired); action != ChangeNoop {
		t.Fatalf("action = %q, want %q", action, ChangeNoop)
	}
}
