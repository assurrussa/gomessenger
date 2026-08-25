package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

const (
	// TopologySpecVersion is the supported declarative topology schema.
	TopologySpecVersion = "1.0"

	configCleanupPolicy     = "cleanup.policy"
	configMinInSyncReplicas = "min.insync.replicas"
	configRetentionMillis   = "retention.ms"
	configRetentionBytes    = "retention.bytes"
	configMaxMessageBytes   = "max.message.bytes"
)

// TopicRole identifies the purpose and validation rules of a managed topic.
type TopicRole string

const (
	// TopicRoleSource is a descriptor source topic.
	TopicRoleSource TopicRole = "source"
	// TopicRoleRetry is one consumer-specific durable retry tier.
	TopicRoleRetry TopicRole = "retry"
	// TopicRoleReplay is one consumer-specific protected replay ingress.
	TopicRoleReplay TopicRole = "replay"
	// TopicRoleDLQ is one consumer-specific dead-letter topic.
	TopicRoleDLQ TopicRole = "dlq"
)

// TopicSpec declares the exact managed subset of one Kafka topic. Service
// topics refer to their source topic and stable consumer ID. Retry topics must
// use unlimited time and size retention so delayed records cannot expire.
type TopicSpec struct {
	Name              string    `json:"name"`
	Role              TopicRole `json:"role"`
	SourceTopic       string    `json:"sourceTopic,omitempty"`
	ConsumerID        string    `json:"consumerId,omitempty"`
	Partitions        int       `json:"partitions"`
	ReplicationFactor int       `json:"replicationFactor"`
	MinInSyncReplicas int       `json:"minInSyncReplicas"`
	RetentionMillis   int64     `json:"retentionMillis"`
	RetentionBytes    int64     `json:"retentionBytes"`
	MaxMessageBytes   int       `json:"maxMessageBytes"`
}

// Topology is the complete declarative Kafka topic contract managed by the
// adapter. Topics outside this declaration are never changed or deleted.
type Topology struct {
	SpecVersion string      `json:"specVersion"`
	Topics      []TopicSpec `json:"topics"`
}

// TopologyAction is one safe topology reconciliation outcome.
type TopologyAction string

const (
	// TopologyActionNone means the topic already matches the declaration.
	TopologyActionNone TopologyAction = "none"
	// TopologyActionCreate means the topic is absent and can be created.
	TopologyActionCreate TopologyAction = "create"
	// TopologyActionUpdate means only monotonic managed configurations need strengthening.
	TopologyActionUpdate TopologyAction = "update"
	// TopologyActionConflict means applying the declaration would be unsafe.
	TopologyActionConflict TopologyAction = "conflict"
)

// TopologyChange describes the action and fields for one declared topic.
type TopologyChange struct {
	Topic  string         `json:"topic"`
	Action TopologyAction `json:"action"`
	Fields []string       `json:"fields,omitempty"`
	Reason string         `json:"reason,omitempty"`
}

// TopologyPlan is a deterministic, payload-free reconciliation plan.
type TopologyPlan struct {
	SpecVersion string           `json:"specVersion"`
	Changes     []TopologyChange `json:"changes"`
}

// HasConflicts reports whether the plan contains unsafe drift.
func (p TopologyPlan) HasConflicts() bool {
	return slices.ContainsFunc(p.Changes, func(change TopologyChange) bool {
		return change.Action == TopologyActionConflict
	})
}

// HasChanges reports whether create or update work remains.
func (p TopologyPlan) HasChanges() bool {
	return slices.ContainsFunc(p.Changes, func(change TopologyChange) bool {
		return change.Action == TopologyActionCreate || change.Action == TopologyActionUpdate
	})
}

type currentTopic struct {
	exists                   bool
	partitions               int
	replicationFactor        int
	heterogeneousReplication bool
	configs                  map[string]string
}

type topologyFamilyKey struct {
	source     string
	consumerID string
}

type topologyFamilyState struct {
	retryTiers map[int]struct{}
	replay     bool
	dlq        bool
}

// ValidateTopology validates naming, completeness, bounded record sizes, and
// equal partition counts across each source and its service topics.
func ValidateTopology(topology Topology) error {
	if topology.SpecVersion != TopologySpecVersion || len(topology.Topics) == 0 || len(topology.Topics) > 4096 {
		return fmt.Errorf("%w: Kafka topology spec", ErrInvalidConfig)
	}
	topics, families, err := indexTopology(topology.Topics)
	if err != nil {
		return err
	}
	return validateTopologyFamilies(topology.Topics, topics, families)
}

func indexTopology(
	topicSpecs []TopicSpec,
) (map[string]TopicSpec, map[topologyFamilyKey]*topologyFamilyState, error) {
	topics := make(map[string]TopicSpec, len(topicSpecs))
	families := make(map[topologyFamilyKey]*topologyFamilyState)
	for _, topic := range topicSpecs {
		if _, duplicate := topics[topic.Name]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate Kafka topic %q", ErrInvalidConfig, topic.Name)
		}
		if err := validateTopicSpec(topic); err != nil {
			return nil, nil, err
		}
		topics[topic.Name] = topic
		if err := recordFamilyTopic(families, topic); err != nil {
			return nil, nil, err
		}
	}
	return topics, families, nil
}

func recordFamilyTopic(families map[topologyFamilyKey]*topologyFamilyState, topic TopicSpec) error {
	if topic.Role == TopicRoleSource {
		return nil
	}
	key := topologyFamilyKey{source: topic.SourceTopic, consumerID: topic.ConsumerID}
	family := families[key]
	if family == nil {
		family = &topologyFamilyState{retryTiers: make(map[int]struct{})}
		families[key] = family
	}
	switch topic.Role {
	case TopicRoleSource:
		return nil
	case TopicRoleRetry:
		tier, err := retryTierFromTopic(topic.Name, topic.SourceTopic, topic.ConsumerID)
		if err != nil {
			return err
		}
		if _, duplicate := family.retryTiers[tier]; duplicate {
			return fmt.Errorf("%w: duplicate retry tier %d for %s", ErrInvalidConfig, tier, topic.ConsumerID)
		}
		family.retryTiers[tier] = struct{}{}
	case TopicRoleReplay:
		if family.replay {
			return fmt.Errorf("%w: duplicate replay topic for %s", ErrInvalidConfig, topic.ConsumerID)
		}
		family.replay = true
	case TopicRoleDLQ:
		if family.dlq {
			return fmt.Errorf("%w: duplicate DLQ topic for %s", ErrInvalidConfig, topic.ConsumerID)
		}
		family.dlq = true
	default:
		return fmt.Errorf("%w: topic role %q", ErrInvalidConfig, topic.Role)
	}
	return nil
}

func validateTopologyFamilies(
	topicSpecs []TopicSpec,
	topics map[string]TopicSpec,
	families map[topologyFamilyKey]*topologyFamilyState,
) error {
	for key, family := range families {
		source, ok := topics[key.source]
		if !ok || source.Role != TopicRoleSource {
			return fmt.Errorf("%w: undeclared source topic %q", ErrInvalidConfig, key.source)
		}
		if err := validateCompleteFamily(key, family); err != nil {
			return err
		}
		if err := validateFamilyPartitions(topicSpecs, key, source.Partitions); err != nil {
			return err
		}
	}
	return nil
}

func validateCompleteFamily(key topologyFamilyKey, family *topologyFamilyState) error {
	if len(family.retryTiers) == 0 || !family.replay || !family.dlq {
		return fmt.Errorf("%w: incomplete service topics for %s", ErrInvalidConfig, key.consumerID)
	}
	for tier := range len(family.retryTiers) {
		if _, ok := family.retryTiers[tier]; !ok {
			return fmt.Errorf("%w: non-contiguous retry tiers for %s", ErrInvalidConfig, key.consumerID)
		}
	}
	return nil
}

func validateFamilyPartitions(topicSpecs []TopicSpec, key topologyFamilyKey, sourcePartitions int) error {
	for _, topic := range topicSpecs {
		if topic.SourceTopic == key.source && topic.ConsumerID == key.consumerID &&
			topic.Partitions != sourcePartitions {
			return fmt.Errorf("%w: service topic %q partition count differs from source", ErrInvalidConfig, topic.Name)
		}
	}
	return nil
}

func validateTopicSpec(topic TopicSpec) error {
	if err := validateTopicName(topic.Name); err != nil {
		return err
	}
	if err := validateTopicBounds(topic); err != nil {
		return err
	}
	switch topic.Role {
	case TopicRoleSource:
		return validateSourceTopicSpec(topic)
	case TopicRoleRetry, TopicRoleReplay, TopicRoleDLQ:
		return validateServiceTopicSpec(topic)
	default:
		return fmt.Errorf("%w: topic role %q", ErrInvalidConfig, topic.Role)
	}
}

func validateTopicBounds(topic TopicSpec) error {
	if topic.Partitions <= 0 || topic.Partitions > int(^uint32(0)>>1) ||
		topic.ReplicationFactor <= 0 || topic.ReplicationFactor > int(^uint16(0)>>1) ||
		topic.MinInSyncReplicas <= 0 || topic.MinInSyncReplicas > topic.ReplicationFactor ||
		topic.MaxMessageBytes <= 0 ||
		(topic.RetentionMillis != -1 && topic.RetentionMillis <= 0) ||
		(topic.RetentionBytes != -1 && topic.RetentionBytes <= 0) {
		return fmt.Errorf("%w: managed topic %q", ErrInvalidConfig, topic.Name)
	}
	return nil
}

func validateSourceTopicSpec(topic TopicSpec) error {
	if topic.SourceTopic != "" || topic.ConsumerID != "" || topic.MaxMessageBytes < DefaultMaxSourceMessageBytes {
		return fmt.Errorf("%w: source topic %q", ErrInvalidConfig, topic.Name)
	}
	return validateSourceTopicName(topic.Name)
}

func validateServiceTopicSpec(topic TopicSpec) error {
	if err := validateTopicName(topic.SourceTopic); err != nil {
		return err
	}
	if err := validateConsumerID(topic.ConsumerID); err != nil {
		return err
	}
	if err := validateServiceMessageSize(topic); err != nil {
		return err
	}
	if err := validateServiceTopicName(topic); err != nil {
		return err
	}
	if topic.Role == TopicRoleRetry && (topic.RetentionMillis != -1 || topic.RetentionBytes != -1) {
		return fmt.Errorf("%w: retry topic %q must have unlimited retention", ErrInvalidConfig, topic.Name)
	}
	return nil
}

func validateServiceMessageSize(topic TopicSpec) error {
	if topic.Role == TopicRoleDLQ {
		if topic.MaxMessageBytes < DefaultMaxDLQMessageBytes {
			return fmt.Errorf("%w: DLQ max message bytes for %q", ErrInvalidConfig, topic.Name)
		}
		return nil
	}
	if topic.MaxMessageBytes < DefaultMaxSourceMessageBytes {
		return fmt.Errorf("%w: service max message bytes for %q", ErrInvalidConfig, topic.Name)
	}
	return nil
}

func validateServiceTopicName(topic TopicSpec) error {
	var expected string
	var err error
	switch topic.Role {
	case TopicRoleSource:
		return fmt.Errorf("%w: source topic %q is not a service topic", ErrInvalidConfig, topic.Name)
	case TopicRoleRetry:
		_, err = retryTierFromTopic(topic.Name, topic.SourceTopic, topic.ConsumerID)
	case TopicRoleReplay:
		expected, err = replayTopic(topic.SourceTopic, topic.ConsumerID)
	case TopicRoleDLQ:
		expected, err = dlqTopic(topic.SourceTopic, topic.ConsumerID)
	default:
		return fmt.Errorf("%w: topic role %q", ErrInvalidConfig, topic.Role)
	}
	if err != nil {
		return err
	}
	if expected != "" && topic.Name != expected {
		return fmt.Errorf("%w: non-deterministic service topic %q", ErrInvalidConfig, topic.Name)
	}
	return nil
}

func retryTierFromTopic(name, source, consumerID string) (int, error) {
	prefix, err := consumerTopic(source, consumerID, "retry.t")
	if err != nil {
		return 0, err
	}
	if !strings.HasPrefix(name, prefix) {
		return 0, fmt.Errorf("%w: non-deterministic retry topic %q", ErrInvalidConfig, name)
	}
	tier, err := strconv.Atoi(strings.TrimPrefix(name, prefix))
	if err != nil || tier < 0 {
		return 0, fmt.Errorf("%w: retry topic tier %q", ErrInvalidConfig, name)
	}
	expected, err := retryTopic(source, consumerID, tier)
	if err != nil || expected != name {
		return 0, fmt.Errorf("%w: non-deterministic retry topic %q", ErrInvalidConfig, name)
	}
	return tier, nil
}

// PlanTopology reads only the declared topics and returns a deterministic
// create/update/conflict plan. It never mutates broker state.
func PlanTopology(ctx context.Context, transport *Transport, topology Topology) (TopologyPlan, error) {
	if ctx == nil || transport == nil {
		return TopologyPlan{}, fmt.Errorf("%w: Kafka topology planner", ErrInvalidConfig)
	}
	if err := ValidateTopology(topology); err != nil {
		transport.logFailure(ctx, messenger.LogError, "Kafka topology plan failed", "topology_plan", err)
		return TopologyPlan{}, err
	}
	transport.topologyMu.Lock()
	defer transport.topologyMu.Unlock()
	plan, err := planTopology(ctx, transport, topology)
	if err != nil {
		transport.logFailure(ctx, messenger.LogError, "Kafka topology plan failed", "topology_plan", err)
		return TopologyPlan{}, err
	}
	changes, conflicts := topologyPlanCounts(plan)
	transport.logInfrastructure(ctx, messenger.LogDebug, "Kafka topology planned",
		messenger.LogAttr{Key: logAttrOperation, Value: "topology_plan"},
		messenger.LogAttr{Key: logAttrTopicCount, Value: len(topology.Topics)},
		messenger.LogAttr{Key: logAttrChangeCount, Value: changes},
		messenger.LogAttr{Key: logAttrConflictCount, Value: conflicts})
	return plan, nil
}

// ApplyTopology creates missing topics and applies only monotonic managed-config
// increases. Partition drift requires an explicit operator migration. The
// entire plan is refused when any conflict exists.
func ApplyTopology(ctx context.Context, transport *Transport, topology Topology) (TopologyPlan, error) {
	if ctx == nil || transport == nil {
		return TopologyPlan{}, fmt.Errorf("%w: Kafka topology apply", ErrInvalidConfig)
	}
	if err := ValidateTopology(topology); err != nil {
		transport.logFailure(ctx, messenger.LogError, "Kafka topology apply failed", "topology_apply", err)
		return TopologyPlan{}, err
	}
	transport.topologyMu.Lock()
	defer transport.topologyMu.Unlock()
	plan, err := planTopology(ctx, transport, topology)
	if err != nil {
		transport.logFailure(ctx, messenger.LogError, "Kafka topology apply failed", "topology_apply", err)
		return TopologyPlan{}, err
	}
	if plan.HasConflicts() {
		_, conflicts := topologyPlanCounts(plan)
		transport.logFailure(ctx, messenger.LogWarn, "Kafka topology apply refused", "topology_apply",
			ErrTopologyDrift, messenger.LogAttr{Key: logAttrConflictCount, Value: conflicts})
		return plan, ErrTopologyDrift
	}
	byName := make(map[string]TopicSpec, len(topology.Topics))
	for _, topic := range topology.Topics {
		byName[topic.Name] = topic
	}
	for _, change := range plan.Changes {
		topic := byName[change.Topic]
		switch change.Action {
		case TopologyActionNone:
			continue
		case TopologyActionCreate:
			if err := createManagedTopic(ctx, transport, topic); err != nil {
				transport.logFailure(ctx, messenger.LogError, "Kafka topology change failed", "topology_apply", err,
					messenger.LogAttr{Key: logAttrTopic, Value: change.Topic},
					messenger.LogAttr{Key: logAttrAction, Value: change.Action})
				return plan, err
			}
		case TopologyActionUpdate:
			if err := updateManagedTopic(ctx, transport, topic, change.Fields); err != nil {
				transport.logFailure(ctx, messenger.LogError, "Kafka topology change failed", "topology_apply", err,
					messenger.LogAttr{Key: logAttrTopic, Value: change.Topic},
					messenger.LogAttr{Key: logAttrAction, Value: change.Action},
					messenger.LogAttr{Key: logAttrFieldsCount, Value: len(change.Fields)})
				return plan, err
			}
		case TopologyActionConflict:
			transport.logFailure(ctx, messenger.LogWarn, "Kafka topology apply refused", "topology_apply",
				ErrTopologyDrift, messenger.LogAttr{Key: logAttrTopic, Value: change.Topic},
				messenger.LogAttr{Key: logAttrAction, Value: change.Action})
			return plan, ErrTopologyDrift
		default:
			failure := fmt.Errorf("%w: topology action %q", ErrInvalidConfig, change.Action)
			transport.logFailure(ctx, messenger.LogError, "Kafka topology change failed", "topology_apply", failure,
				messenger.LogAttr{Key: logAttrTopic, Value: change.Topic})
			return plan, failure
		}
		transport.logInfrastructure(ctx, messenger.LogInfo, "Kafka topology change applied",
			messenger.LogAttr{Key: logAttrOperation, Value: "topology_apply"},
			messenger.LogAttr{Key: logAttrTopic, Value: change.Topic},
			messenger.LogAttr{Key: logAttrAction, Value: change.Action},
			messenger.LogAttr{Key: logAttrFieldsCount, Value: len(change.Fields)})
	}
	return plan, nil
}

func topologyPlanCounts(plan TopologyPlan) (changes int, conflicts int) {
	for _, change := range plan.Changes {
		switch change.Action {
		case TopologyActionNone:
		case TopologyActionCreate, TopologyActionUpdate:
			changes++
		case TopologyActionConflict:
			conflicts++
		}
	}
	return changes, conflicts
}

func planTopology(ctx context.Context, transport *Transport, topology Topology) (TopologyPlan, error) {
	if err := transport.ensureTopologyUsable(); err != nil {
		return TopologyPlan{}, err
	}
	state, err := loadCurrentTopology(ctx, transport, topology)
	if err != nil {
		return TopologyPlan{}, err
	}
	plan := buildTopologyPlan(topology, state)
	return plan, nil
}

func (t *Transport) ensureTopologyUsable() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == transportClosed {
		return ErrTransportClosed
	}
	return nil
}

func loadCurrentTopology(ctx context.Context, transport *Transport, topology Topology) (map[string]currentTopic, error) {
	names := make([]string, 0, len(topology.Topics))
	for _, topic := range topology.Topics {
		names = append(names, topic.Name)
	}
	adminContext, cancel := context.WithTimeout(ctx, transport.config.OperationTimeout)
	defer cancel()
	details, err := transport.admin.ListTopics(adminContext, names...)
	if err != nil {
		return nil, fmt.Errorf("messenger/kafka: list managed topics: %w", err)
	}
	state := make(map[string]currentTopic, len(names))
	existing := make([]string, 0, len(names))
	for _, name := range names {
		detail, ok := details[name]
		if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			state[name] = currentTopic{}
			continue
		}
		if detail.Err != nil {
			return nil, fmt.Errorf("messenger/kafka: inspect topic %s: %w", name, detail.Err)
		}
		replicationFactor, homogeneous := uniformReplicationFactor(detail.Partitions)
		state[name] = currentTopic{
			exists: true, partitions: len(detail.Partitions), replicationFactor: replicationFactor,
			heterogeneousReplication: !homogeneous, configs: make(map[string]string),
		}
		existing = append(existing, name)
	}
	if len(existing) == 0 {
		return state, nil
	}
	configs, err := transport.admin.DescribeTopicConfigs(adminContext, existing...)
	if err != nil {
		return nil, fmt.Errorf("messenger/kafka: describe managed topic configs: %w", err)
	}
	for _, resource := range configs {
		if resource.Err != nil {
			return nil, fmt.Errorf("messenger/kafka: describe topic %s: %w", resource.Name, resource.Err)
		}
		current := state[resource.Name]
		for _, config := range resource.Configs {
			if managedConfig(config.Key) {
				current.configs[config.Key] = config.MaybeValue()
			}
		}
		state[resource.Name] = current
	}
	return state, nil
}

func uniformReplicationFactor(partitions kadm.PartitionDetails) (int, bool) {
	replicationFactor := -1
	for _, partition := range partitions {
		current := len(partition.Replicas)
		if replicationFactor == -1 {
			replicationFactor = current
			continue
		}
		if current != replicationFactor {
			return 0, false
		}
	}
	return max(0, replicationFactor), true
}

func buildTopologyPlan(topology Topology, state map[string]currentTopic) TopologyPlan {
	changes := make([]TopologyChange, 0, len(topology.Topics))
	for _, desired := range topology.Topics {
		current := state[desired.Name]
		if !current.exists {
			changes = append(changes, TopologyChange{Topic: desired.Name, Action: TopologyActionCreate})
			continue
		}
		updates := make([]string, 0, 6)
		conflicts := make([]string, 0, 6)
		if current.partitions != desired.Partitions {
			conflicts = append(conflicts, fmt.Sprintf("partitions is %d, declared %d",
				current.partitions, desired.Partitions))
		}
		if current.heterogeneousReplication {
			conflicts = append(conflicts, "replicationFactor differs across partitions")
		} else if current.replicationFactor != desired.ReplicationFactor {
			conflicts = append(conflicts, fmt.Sprintf("replicationFactor is %d, declared %d",
				current.replicationFactor, desired.ReplicationFactor))
		}
		compareExactConfig(current.configs, configCleanupPolicy, "delete", &conflicts)
		compareConfig(current.configs, configMinInSyncReplicas, int64(desired.MinInSyncReplicas), false, &updates, &conflicts)
		compareConfig(current.configs, configRetentionMillis, desired.RetentionMillis, true, &updates, &conflicts)
		compareConfig(current.configs, configRetentionBytes, desired.RetentionBytes, true, &updates, &conflicts)
		compareConfig(current.configs, configMaxMessageBytes, int64(desired.MaxMessageBytes), false, &updates, &conflicts)
		switch {
		case len(conflicts) > 0:
			changes = append(changes, TopologyChange{
				Topic: desired.Name, Action: TopologyActionConflict,
				Reason: strings.Join(conflicts, "; "),
			})
		case len(updates) > 0:
			sort.Strings(updates)
			changes = append(changes, TopologyChange{Topic: desired.Name, Action: TopologyActionUpdate, Fields: updates})
		default:
			changes = append(changes, TopologyChange{Topic: desired.Name, Action: TopologyActionNone})
		}
	}
	sort.Slice(changes, func(left, right int) bool { return changes[left].Topic < changes[right].Topic })
	return TopologyPlan{SpecVersion: TopologySpecVersion, Changes: changes}
}

func compareExactConfig(configs map[string]string, field, desired string, conflicts *[]string) {
	current, ok := configs[field]
	if !ok {
		*conflicts = append(*conflicts, field+" is unavailable")
		return
	}
	if current != desired {
		*conflicts = append(*conflicts, fmt.Sprintf("%s is %q, declared %q", field, current, desired))
	}
}

func compareConfig(
	configs map[string]string,
	field string,
	desired int64,
	infinite bool,
	updates, conflicts *[]string,
) {
	raw, ok := configs[field]
	if !ok {
		*conflicts = append(*conflicts, field+" is unavailable")
		return
	}
	current, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		*conflicts = append(*conflicts, fmt.Sprintf("%s has invalid value %q", field, raw))
		return
	}
	if current == desired {
		return
	}
	if infinite && desired == -1 && current != -1 || current != -1 && current < desired {
		*updates = append(*updates, field)
		return
	}
	*conflicts = append(*conflicts, fmt.Sprintf("%s is %d, declared %d", field, current, desired))
}

func managedConfig(name string) bool {
	switch name {
	case configCleanupPolicy, configMinInSyncReplicas, configRetentionMillis,
		configRetentionBytes, configMaxMessageBytes:
		return true
	default:
		return false
	}
}

func desiredConfigs(topic TopicSpec) map[string]string {
	return map[string]string{
		configCleanupPolicy:     "delete",
		configMinInSyncReplicas: strconv.Itoa(topic.MinInSyncReplicas),
		configRetentionMillis:   strconv.FormatInt(topic.RetentionMillis, 10),
		configRetentionBytes:    strconv.FormatInt(topic.RetentionBytes, 10),
		configMaxMessageBytes:   strconv.Itoa(topic.MaxMessageBytes),
	}
}

func createManagedTopic(ctx context.Context, transport *Transport, topic TopicSpec) error {
	configs := make(map[string]*string, 5)
	for name, raw := range desiredConfigs(topic) {
		value := raw
		configs[name] = &value
	}
	adminContext, cancel := context.WithTimeout(ctx, transport.config.OperationTimeout)
	defer cancel()
	partitions := int32(topic.Partitions)               //nolint:gosec // Validated against the Kafka int32 limit.
	replicationFactor := int16(topic.ReplicationFactor) //nolint:gosec // Validated against the Kafka int16 limit.
	_, err := transport.admin.CreateTopic(adminContext, partitions, replicationFactor, configs, topic.Name)
	if err != nil {
		return fmt.Errorf("messenger/kafka: create topic %s: %w", topic.Name, err)
	}
	return nil
}

func updateManagedTopic(ctx context.Context, transport *Transport, topic TopicSpec, fields []string) error {
	desired := desiredConfigs(topic)
	configs := make([]kadm.AlterConfig, 0, len(fields))
	for _, field := range fields {
		value, ok := desired[field]
		if !ok {
			return fmt.Errorf("%w: unsupported managed topic update field %q", ErrInvalidConfig, field)
		}
		configs = append(configs, kadm.AlterConfig{Op: kadm.SetConfig, Name: field, Value: kadm.StringPtr(value)})
	}
	if len(configs) == 0 {
		return nil
	}
	adminContext, cancel := context.WithTimeout(ctx, transport.config.OperationTimeout)
	defer cancel()
	responses, err := transport.admin.AlterTopicConfigs(adminContext, configs, topic.Name)
	if err != nil {
		return fmt.Errorf("messenger/kafka: update configs for %s: %w", topic.Name, err)
	}
	response, err := responses.On(topic.Name, nil)
	if err != nil {
		return fmt.Errorf("messenger/kafka: update configs for %s: %w", topic.Name, err)
	}
	if response.Err != nil {
		return fmt.Errorf("messenger/kafka: update configs for %s: %w", topic.Name, response.Err)
	}
	return nil
}
