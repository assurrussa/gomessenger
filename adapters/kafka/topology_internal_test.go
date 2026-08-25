package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestValidateTopology(t *testing.T) {
	t.Parallel()
	topology := validTestTopology()
	if err := ValidateTopology(topology); err != nil {
		t.Fatalf("ValidateTopology() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Topology)
		want   string
	}{
		{
			name: "retry retention is bounded",
			mutate: func(topology *Topology) {
				topology.Topics[1].RetentionMillis = 10_000
			},
			want: "unlimited retention",
		},
		{
			name: "service partitions differ",
			mutate: func(topology *Topology) {
				topology.Topics[1].Partitions++
			},
			want: "partition count differs",
		},
		{
			name: "retry tier is missing",
			mutate: func(topology *Topology) {
				topology.Topics[2].Name = strings.Replace(topology.Topics[2].Name, "retry.t1", "retry.t2", 1)
			},
			want: "non-contiguous retry tiers",
		},
		{
			name: "DLQ is missing",
			mutate: func(topology *Topology) {
				topology.Topics = topology.Topics[:len(topology.Topics)-1]
			},
			want: "incomplete service topics",
		},
		{
			name: "source name is not descriptor derived",
			mutate: func(topology *Topology) {
				topology.Topics[0].Name = "orders"
			},
			want: "namespace.kind.descriptor.vN",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topology := validTestTopology()
			test.mutate(&topology)
			err := ValidateTopology(topology)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateTopology() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBuildTopologyPlanAllowsOnlyMonotonicConfigUpdates(t *testing.T) {
	t.Parallel()
	topology := validTestTopology()
	state := make(map[string]currentTopic, len(topology.Topics))
	for _, topic := range topology.Topics {
		state[topic.Name] = currentTopic{
			exists: true, partitions: topic.Partitions, replicationFactor: topic.ReplicationFactor,
			configs: desiredConfigs(topic),
		}
	}

	source := topology.Topics[0]
	retry := topology.Topics[1]
	replay := topology.Topics[len(topology.Topics)-2]
	dlq := topology.Topics[len(topology.Topics)-1]

	currentSource := state[source.Name]
	currentSource.configs[configRetentionMillis] = "60000"
	state[source.Name] = currentSource
	currentRetry := state[retry.Name]
	currentRetry.partitions = 1
	state[retry.Name] = currentRetry
	state[replay.Name] = currentTopic{}
	currentDLQ := state[dlq.Name]
	currentDLQ.replicationFactor = 2
	state[dlq.Name] = currentDLQ

	plan := buildTopologyPlan(topology, state)
	changes := make(map[string]TopologyChange, len(plan.Changes))
	for _, change := range plan.Changes {
		changes[change.Topic] = change
	}
	if got := changes[source.Name]; got.Action != TopologyActionUpdate ||
		!containsAll(got.Fields, configRetentionMillis) {
		t.Fatalf("source change = %#v, want retention update", got)
	}
	if got := changes[retry.Name]; got.Action != TopologyActionConflict ||
		!strings.Contains(got.Reason, "partitions") {
		t.Fatalf("retry change = %#v, want partition conflict", got)
	}
	if got := changes[replay.Name].Action; got != TopologyActionCreate {
		t.Fatalf("replay action = %q, want %q", got, TopologyActionCreate)
	}
	if got := changes[dlq.Name]; got.Action != TopologyActionConflict ||
		!strings.Contains(got.Reason, "replicationFactor") {
		t.Fatalf("DLQ change = %#v, want replication conflict", got)
	}
	if !plan.HasChanges() || !plan.HasConflicts() {
		t.Fatalf("plan flags = changes:%v conflicts:%v, want both", plan.HasChanges(), plan.HasConflicts())
	}
}

func TestUniformReplicationFactor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		partitions  kadm.PartitionDetails
		want        int
		homogeneous bool
	}{
		{name: "empty", partitions: kadm.PartitionDetails{}, want: 0, homogeneous: true},
		{
			name: "uniform",
			partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1, 2, 3}},
				1: {Partition: 1, Replicas: []int32{2, 3, 4}},
			},
			want: 3, homogeneous: true,
		},
		{
			name: "heterogeneous",
			partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1, 2, 3}},
				1: {Partition: 1, Replicas: []int32{2, 3}},
			},
			homogeneous: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, homogeneous := uniformReplicationFactor(test.partitions)
			if got != test.want || homogeneous != test.homogeneous {
				t.Fatalf("uniformReplicationFactor() = (%d, %v), want (%d, %v)",
					got, homogeneous, test.want, test.homogeneous)
			}
		})
	}
}

func TestBuildTopologyPlanRejectsHeterogeneousReplication(t *testing.T) {
	t.Parallel()
	topology := validTestTopology()
	topic := topology.Topics[0]
	plan := buildTopologyPlan(Topology{SpecVersion: TopologySpecVersion, Topics: []TopicSpec{topic}}, map[string]currentTopic{
		topic.Name: {
			exists: true, partitions: topic.Partitions, replicationFactor: topic.ReplicationFactor,
			heterogeneousReplication: true, configs: desiredConfigs(topic),
		},
	})
	if len(plan.Changes) != 1 || plan.Changes[0].Action != TopologyActionConflict ||
		!strings.Contains(plan.Changes[0].Reason, "differs across partitions") {
		t.Fatalf("plan = %#v, want heterogeneous replication conflict", plan)
	}
}

func TestBuildTopologyPlanRejectsConfigWeakening(t *testing.T) {
	t.Parallel()
	topology := validTestTopology()
	topic := topology.Topics[0]
	current := desiredConfigs(topic)
	current[configRetentionMillis] = "-1"
	current[configMaxMessageBytes] = "99999999"
	plan := buildTopologyPlan(Topology{SpecVersion: TopologySpecVersion, Topics: []TopicSpec{topic}}, map[string]currentTopic{
		topic.Name: {
			exists: true, partitions: topic.Partitions, replicationFactor: topic.ReplicationFactor, configs: current,
		},
	})
	if len(plan.Changes) != 1 || plan.Changes[0].Action != TopologyActionConflict ||
		!strings.Contains(plan.Changes[0].Reason, configRetentionMillis) ||
		!strings.Contains(plan.Changes[0].Reason, configMaxMessageBytes) {
		t.Fatalf("plan = %#v, want retention and max-byte conflicts", plan)
	}
}

func validTestTopology() Topology {
	const source = "orders.command.orders-create.v1"
	base := TopicSpec{
		Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: 2,
		RetentionMillis: 604_800_000, RetentionBytes: -1, MaxMessageBytes: DefaultMaxSourceMessageBytes,
	}
	sourceSpec := base
	sourceSpec.Name = source
	sourceSpec.Role = TopicRoleSource
	topics := make([]TopicSpec, 1, 5)
	topics[0] = sourceSpec
	for tier := range 2 {
		retry := base
		retry.Name, _ = retryTopic(source, testConsumerID, tier)
		retry.Role = TopicRoleRetry
		retry.SourceTopic = source
		retry.ConsumerID = testConsumerID
		retry.RetentionMillis = -1
		retry.RetentionBytes = -1
		topics = append(topics, retry)
	}
	replay := base
	replay.Name, _ = replayTopic(source, testConsumerID)
	replay.Role = TopicRoleReplay
	replay.SourceTopic = source
	replay.ConsumerID = testConsumerID
	topics = append(topics, replay)
	dlq := base
	dlq.Name, _ = dlqTopic(source, testConsumerID)
	dlq.Role = TopicRoleDLQ
	dlq.SourceTopic = source
	dlq.ConsumerID = testConsumerID
	dlq.MaxMessageBytes = DefaultMaxDLQMessageBytes
	topics = append(topics, dlq)
	return Topology{SpecVersion: TopologySpecVersion, Topics: topics}
}

func containsAll(values []string, expected ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range expected {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}
