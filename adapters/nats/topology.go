package nats

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TopologySpecVersion is the current declarative topology version.
const (
	TopologySpecVersion  = "1.0"
	resourceStream       = "stream"
	resourceConsumer     = "consumer"
	maxJetStreamReplicas = 5
)

// Topology is the versionable safe subset managed by gomessengerctl.
type Topology struct {
	SpecVersion string         `json:"specVersion"`
	Streams     []StreamSpec   `json:"streams,omitempty"`
	Consumers   []ConsumerSpec `json:"consumers,omitempty"`
}

// StreamSpec declares a stream without destructive management operations.
type StreamSpec struct {
	Name        string                    `json:"name"`
	Description string                    `json:"description,omitempty"`
	Subjects    []string                  `json:"subjects"`
	Retention   jetstream.RetentionPolicy `json:"retention"`
	Storage     jetstream.StorageType     `json:"storage"`
	Replicas    int                       `json:"replicas"`
	MaxMessages int64                     `json:"maxMessages"`
	MaxBytes    int64                     `json:"maxBytes"`
	MaxAge      time.Duration             `json:"maxAge"`
	MaxMsgSize  int32                     `json:"maxMessageSize"`
	Duplicates  time.Duration             `json:"duplicateWindow"`
}

// ConsumerSpec declares one durable pull consumer.
type ConsumerSpec struct {
	Stream        string        `json:"stream"`
	Name          string        `json:"name"`
	Description   string        `json:"description,omitempty"`
	FilterSubject string        `json:"filterSubject"`
	AckWait       time.Duration `json:"ackWait"`
	MaxDeliver    int           `json:"maxDeliver"`
	MaxAckPending int           `json:"maxAckPending"`
	Replicas      int           `json:"replicas,omitempty"`
	MemoryStorage bool          `json:"memoryStorage,omitempty"`
}

// ChangeAction is one non-destructive topology action.
type ChangeAction string

const (
	// ChangeCreate creates a missing stream or consumer.
	ChangeCreate ChangeAction = "create"
	// ChangeUpdate applies a compatible update.
	ChangeUpdate ChangeAction = "update"
	// ChangeNoop means current topology already matches.
	ChangeNoop ChangeAction = "noop"
	// ChangeConflict means applying would be destructive or semantically unsafe.
	ChangeConflict ChangeAction = "conflict"
)

// Change is one planned resource action.
type Change struct {
	Resource string       `json:"resource"`
	Name     string       `json:"name"`
	Action   ChangeAction `json:"action"`
	Reason   string       `json:"reason,omitempty"`
}

// DevStream returns an explicit memory-backed, single-replica stream spec.
func DevStream(name string, subjects ...string) StreamSpec {
	return StreamSpec{
		Name: name, Subjects: append([]string(nil), subjects...), Retention: jetstream.LimitsPolicy,
		Storage: jetstream.MemoryStorage, Replicas: 1, MaxMessages: -1, MaxBytes: -1,
		MaxAge: 24 * time.Hour, MaxMsgSize: 1 << 20, Duplicates: 10 * time.Minute,
	}
}

// DevDLQStream returns a development stream sized for the largest supported
// DLQ record plus its bounded transport headers.
func DevDLQStream(name string, subjects ...string) StreamSpec {
	stream := DevStream(name, subjects...)
	stream.MaxMsgSize = DefaultMaxDLQMessageBytes
	return stream
}

// PlanTopology compares declared topology with JetStream without mutating it.
func PlanTopology(ctx context.Context, connection *natsio.Conn, topology Topology) ([]Change, error) {
	js, err := jetStream(connection, topology)
	if err != nil {
		return nil, err
	}
	return planTopology(ctx, js, topology)
}

// ApplyTopology creates missing resources and applies only compatible updates.
// Any conflict aborts before the first mutation.
func ApplyTopology(ctx context.Context, connection *natsio.Conn, topology Topology) ([]Change, error) {
	js, err := jetStream(connection, topology)
	if err != nil {
		return nil, err
	}
	changes, err := planTopology(ctx, js, topology)
	if err != nil {
		return nil, err
	}
	for _, change := range changes {
		if change.Action == ChangeConflict {
			return changes, fmt.Errorf("%w: %s %s: %s", ErrTopologyDrift,
				change.Resource, change.Name, change.Reason)
		}
	}
	streams := make(map[string]StreamSpec, len(topology.Streams))
	for _, stream := range topology.Streams {
		streams[stream.Name] = stream
	}
	consumers := make(map[string]ConsumerSpec, len(topology.Consumers))
	for _, consumer := range topology.Consumers {
		consumers[consumer.Stream+"/"+consumer.Name] = consumer
	}
	for _, resource := range []string{resourceStream, resourceConsumer} {
		for _, change := range changes {
			if change.Resource != resource {
				continue
			}
			switch change.Resource {
			case resourceStream:
				if err := applyStreamChange(ctx, js, change, streams[change.Name]); err != nil {
					return changes, err
				}
			case resourceConsumer:
				if err := applyConsumerChange(ctx, js, change, consumers[change.Name]); err != nil {
					return changes, err
				}
			}
		}
	}
	return changes, nil
}

func applyStreamChange(
	ctx context.Context,
	js jetstream.JetStream,
	change Change,
	spec StreamSpec,
) error {
	switch change.Action {
	case ChangeCreate:
		if _, err := js.CreateStream(ctx, streamConfig(spec)); err != nil {
			return fmt.Errorf("messenger/nats: create stream %s: %w", change.Name, err)
		}
	case ChangeUpdate:
		stream, err := js.Stream(ctx, spec.Name)
		if err != nil {
			return fmt.Errorf("messenger/nats: inspect stream %s before update: %w", change.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("messenger/nats: inspect stream %s info before update: %w", change.Name, err)
		}
		action, reason := compareStream(info.Config, spec)
		if action == ChangeConflict {
			return fmt.Errorf("%w: stream %s: %s", ErrTopologyDrift, change.Name, reason)
		}
		if action == ChangeUpdate {
			if _, err := js.UpdateStream(ctx, mergeStreamConfig(info.Config, spec)); err != nil {
				return fmt.Errorf("messenger/nats: update stream %s: %w", change.Name, err)
			}
		}
	case ChangeNoop:
		return nil
	case ChangeConflict:
		return fmt.Errorf("%w: stream %s: %s", ErrTopologyDrift, change.Name, change.Reason)
	}
	return nil
}

func applyConsumerChange(
	ctx context.Context,
	js jetstream.JetStream,
	change Change,
	spec ConsumerSpec,
) error {
	switch change.Action {
	case ChangeCreate:
		if _, err := js.CreateConsumer(ctx, spec.Stream, consumerConfig(spec)); err != nil {
			return fmt.Errorf("messenger/nats: create consumer %s: %w", change.Name, err)
		}
	case ChangeUpdate:
		consumer, err := js.Consumer(ctx, spec.Stream, spec.Name)
		if err != nil {
			return fmt.Errorf("messenger/nats: inspect consumer %s before update: %w", change.Name, err)
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			return fmt.Errorf("messenger/nats: inspect consumer %s info before update: %w", change.Name, err)
		}
		action, reason := compareConsumer(info.Config, spec)
		if action == ChangeConflict {
			return fmt.Errorf("%w: consumer %s: %s", ErrTopologyDrift, change.Name, reason)
		}
		if action == ChangeUpdate {
			if _, err := js.UpdateConsumer(ctx, spec.Stream, mergeConsumerConfig(info.Config, spec)); err != nil {
				return fmt.Errorf("messenger/nats: update consumer %s: %w", change.Name, err)
			}
		}
	case ChangeNoop:
		return nil
	case ChangeConflict:
		return fmt.Errorf("%w: consumer %s: %s", ErrTopologyDrift, change.Name, change.Reason)
	}
	return nil
}

// ValidateTopology checks the declarative safe subset without connecting to a
// broker.
func ValidateTopology(topology Topology) error { return validateTopology(topology) }

func jetStream(connection *natsio.Conn, topology Topology) (jetstream.JetStream, error) {
	if connection == nil {
		return nil, fmt.Errorf("%w: nil NATS connection", ErrInvalidConfig)
	}
	if err := validateTopology(topology); err != nil {
		return nil, err
	}
	js, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: create JetStream context: %w", err)
	}
	return js, nil
}

func validateTopology(topology Topology) error {
	if topology.SpecVersion != TopologySpecVersion {
		return fmt.Errorf("%w: topology specVersion must be 1.0", ErrInvalidConfig)
	}
	streams := make(map[string]StreamSpec, len(topology.Streams))
	for _, stream := range topology.Streams {
		if err := validateJetStreamResourceName(stream.Name); err != nil {
			return fmt.Errorf("%w: stream: %w", ErrInvalidConfig, err)
		}
		if len(stream.Subjects) == 0 || stream.Replicas < 1 || stream.Replicas > maxJetStreamReplicas ||
			stream.MaxMessages == 0 || stream.MaxMessages < -1 ||
			stream.MaxBytes == 0 || stream.MaxBytes < -1 || stream.MaxAge < 0 ||
			stream.MaxMsgSize <= 0 || stream.Duplicates <= 0 {
			return fmt.Errorf("%w: stream %q", ErrInvalidConfig, stream.Name)
		}
		for _, subject := range stream.Subjects {
			if err := validateSubjectPattern(subject); err != nil {
				return fmt.Errorf("%w: stream %q subject: %w", ErrInvalidConfig, stream.Name, err)
			}
		}
		if _, exists := streams[stream.Name]; exists {
			return fmt.Errorf("%w: duplicate stream %s", ErrInvalidConfig, stream.Name)
		}
		streams[stream.Name] = stream
	}
	consumers := make(map[string]struct{}, len(topology.Consumers))
	for _, consumer := range topology.Consumers {
		if err := validateJetStreamResourceName(consumer.Stream); err != nil {
			return fmt.Errorf("%w: consumer stream: %w", ErrInvalidConfig, err)
		}
		if err := validateJetStreamResourceName(consumer.Name); err != nil {
			return fmt.Errorf("%w: consumer name: %w", ErrInvalidConfig, err)
		}
		key := consumer.Stream + "/" + consumer.Name
		if consumer.FilterSubject == "" ||
			consumer.AckWait <= 0 || consumer.MaxDeliver == 0 || consumer.MaxDeliver < -1 ||
			consumer.MaxAckPending <= 0 || consumer.Replicas < 0 ||
			consumer.Replicas > maxJetStreamReplicas {
			return fmt.Errorf("%w: consumer %q", ErrInvalidConfig, key)
		}
		if err := validateSubjectPattern(consumer.FilterSubject); err != nil {
			return fmt.Errorf("%w: consumer %q subject: %w", ErrInvalidConfig, key, err)
		}
		if err := validateConsumerAgainstDeclaredStream(streams, consumer, key); err != nil {
			return err
		}
		if _, exists := consumers[key]; exists {
			return fmt.Errorf("%w: duplicate consumer %s", ErrInvalidConfig, key)
		}
		consumers[key] = struct{}{}
	}
	return nil
}

func validateConsumerAgainstDeclaredStream(
	streams map[string]StreamSpec,
	consumer ConsumerSpec,
	key string,
) error {
	stream, declared := streams[consumer.Stream]
	if !declared {
		return nil
	}
	if consumer.Replicas > stream.Replicas {
		return fmt.Errorf("%w: consumer %q replicas exceed stream %q replicas",
			ErrInvalidConfig, key, stream.Name)
	}
	if !streamSubjectsCoverFilter(stream.Subjects, consumer.FilterSubject) {
		return fmt.Errorf("%w: consumer %q filter %q is outside stream subjects",
			ErrInvalidConfig, key, consumer.FilterSubject)
	}
	return nil
}

func planTopology(ctx context.Context, js jetstream.JetStream, topology Topology) ([]Change, error) {
	changes := make([]Change, 0, len(topology.Streams)+len(topology.Consumers))
	missingStreams := make(map[string]struct{})
	streamSubjects := make(map[string][]string, len(topology.Streams))
	for _, desired := range topology.Streams {
		streamSubjects[desired.Name] = desired.Subjects
		stream, err := js.Stream(ctx, desired.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			changes = append(changes, Change{Resource: resourceStream, Name: desired.Name, Action: ChangeCreate})
			missingStreams[desired.Name] = struct{}{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("messenger/nats: inspect stream %s: %w", desired.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("messenger/nats: inspect stream %s info: %w", desired.Name, err)
		}
		action, reason := compareStream(info.Config, desired)
		changes = append(changes, Change{Resource: resourceStream, Name: desired.Name, Action: action, Reason: reason})
	}
	for _, desired := range topology.Consumers {
		subjects, streamExists, err := resolveConsumerStreamSubjects(ctx, js, streamSubjects, desired.Stream)
		if err != nil {
			return nil, err
		}
		if !streamExists {
			changes = append(changes, Change{
				Resource: resourceConsumer, Name: desired.Stream + "/" + desired.Name,
				Action: ChangeConflict, Reason: "referenced stream does not exist",
			})
			continue
		}
		if !streamSubjectsCoverFilter(subjects, desired.FilterSubject) {
			changes = append(changes, Change{
				Resource: resourceConsumer, Name: desired.Stream + "/" + desired.Name,
				Action: ChangeConflict, Reason: "filter subject is outside stream subjects",
			})
			continue
		}
		if _, missing := missingStreams[desired.Stream]; missing {
			changes = append(changes, Change{
				Resource: resourceConsumer, Name: desired.Stream + "/" + desired.Name, Action: ChangeCreate,
			})
			continue
		}
		consumer, err := js.Consumer(ctx, desired.Stream, desired.Name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			changes = append(changes, Change{
				Resource: resourceConsumer, Name: desired.Stream + "/" + desired.Name, Action: ChangeCreate,
			})
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("messenger/nats: inspect consumer %s/%s: %w",
				desired.Stream, desired.Name, err)
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("messenger/nats: inspect consumer %s/%s info: %w",
				desired.Stream, desired.Name, err)
		}
		action, reason := compareConsumer(info.Config, desired)
		changes = append(changes, Change{
			Resource: resourceConsumer, Name: desired.Stream + "/" + desired.Name, Action: action, Reason: reason,
		})
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Resource != changes[j].Resource {
			return changes[i].Resource < changes[j].Resource
		}
		return changes[i].Name < changes[j].Name
	})
	return changes, nil
}

func resolveConsumerStreamSubjects(
	ctx context.Context,
	js jetstream.JetStream,
	known map[string][]string,
	streamName string,
) ([]string, bool, error) {
	if subjects, exists := known[streamName]; exists {
		return subjects, true, nil
	}
	stream, err := js.Stream(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("messenger/nats: inspect consumer stream %s: %w", streamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("messenger/nats: inspect consumer stream %s info: %w", streamName, err)
	}
	known[streamName] = info.Config.Subjects
	return info.Config.Subjects, true, nil
}

func compareStream(current jetstream.StreamConfig, desired StreamSpec) (ChangeAction, string) {
	if current.Retention != desired.Retention || current.Storage != desired.Storage || current.Replicas != desired.Replicas {
		return ChangeConflict, "retention, storage, or replicas differ"
	}
	if !isSubjectSuperset(desired.Subjects, current.Subjects) {
		return ChangeConflict, "declared subjects would remove an existing subject"
	}
	if decreasesLimit(current.MaxMsgs, desired.MaxMessages) || decreasesLimit(current.MaxBytes, desired.MaxBytes) ||
		decreasesDuration(current.MaxAge, desired.MaxAge) ||
		decreasesLimit(int64(current.MaxMsgSize), int64(desired.MaxMsgSize)) ||
		desired.Duplicates < current.Duplicates {
		return ChangeConflict, "declared limits could remove retained messages"
	}
	if current.Description != desired.Description || !sameStrings(current.Subjects, desired.Subjects) ||
		current.MaxMsgs != desired.MaxMessages || current.MaxBytes != desired.MaxBytes ||
		current.MaxAge != desired.MaxAge || current.MaxMsgSize != desired.MaxMsgSize ||
		current.Duplicates != desired.Duplicates {
		return ChangeUpdate, "compatible additive change"
	}
	return ChangeNoop, ""
}

func compareConsumer(current jetstream.ConsumerConfig, desired ConsumerSpec) (ChangeAction, string) {
	if current.Durable != desired.Name || current.Name != desired.Name ||
		current.AckPolicy != jetstream.AckExplicitPolicy || current.FilterSubject != desired.FilterSubject ||
		current.AckWait != desired.AckWait || current.MaxDeliver != desired.MaxDeliver ||
		current.MaxAckPending != desired.MaxAckPending || current.Replicas != desired.Replicas ||
		current.MemoryStorage != desired.MemoryStorage || consumerDeliveryOptionsDiffer(current) {
		return ChangeConflict, "delivery or acknowledgement contract differs"
	}
	if current.Description != desired.Description {
		return ChangeUpdate, "description change"
	}
	return ChangeNoop, ""
}

func consumerDeliveryOptionsDiffer(current jetstream.ConsumerConfig) bool {
	return current.DeliverPolicy != jetstream.DeliverAllPolicy || current.OptStartSeq != 0 ||
		current.OptStartTime != nil || len(current.BackOff) != 0 || len(current.FilterSubjects) != 0 ||
		current.ReplayPolicy != jetstream.ReplayInstantPolicy || current.RateLimit != 0 ||
		current.HeadersOnly || current.MaxRequestBatch != 0 || current.MaxRequestExpires != 0 ||
		current.MaxRequestMaxBytes != 0 || current.InactiveThreshold != 0 || current.PauseUntil != nil ||
		current.PriorityPolicy != 0 || current.PinnedTTL != 0 || len(current.PriorityGroups) != 0 ||
		current.DeliverSubject != "" || current.DeliverGroup != "" || current.FlowControl ||
		current.IdleHeartbeat != 0
}

func streamConfig(spec StreamSpec) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: spec.Name, Description: spec.Description, Subjects: append([]string(nil), spec.Subjects...),
		Retention: spec.Retention, Storage: spec.Storage, Replicas: spec.Replicas,
		MaxMsgs: spec.MaxMessages, MaxBytes: spec.MaxBytes, MaxAge: spec.MaxAge,
		MaxMsgSize: spec.MaxMsgSize, Duplicates: spec.Duplicates,
	}
}

func consumerConfig(spec ConsumerSpec) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name: spec.Name, Durable: spec.Name, Description: spec.Description,
		DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy, AckWait: spec.AckWait,
		MaxDeliver: spec.MaxDeliver, MaxAckPending: spec.MaxAckPending,
		FilterSubject: spec.FilterSubject, ReplayPolicy: jetstream.ReplayInstantPolicy,
		Replicas: spec.Replicas, MemoryStorage: spec.MemoryStorage,
	}
}

func mergeStreamConfig(current jetstream.StreamConfig, desired StreamSpec) jetstream.StreamConfig {
	managed := streamConfig(desired)
	current.Name = managed.Name
	current.Description = managed.Description
	current.Subjects = managed.Subjects
	current.Retention = managed.Retention
	current.Storage = managed.Storage
	current.Replicas = managed.Replicas
	current.MaxMsgs = managed.MaxMsgs
	current.MaxBytes = managed.MaxBytes
	current.MaxAge = managed.MaxAge
	current.MaxMsgSize = managed.MaxMsgSize
	current.Duplicates = managed.Duplicates
	return current
}

func mergeConsumerConfig(current jetstream.ConsumerConfig, desired ConsumerSpec) jetstream.ConsumerConfig {
	managed := consumerConfig(desired)
	current.Name = managed.Name
	current.Durable = managed.Durable
	current.Description = managed.Description
	current.AckPolicy = managed.AckPolicy
	current.AckWait = managed.AckWait
	current.MaxDeliver = managed.MaxDeliver
	current.MaxAckPending = managed.MaxAckPending
	current.FilterSubject = managed.FilterSubject
	current.Replicas = managed.Replicas
	current.MemoryStorage = managed.MemoryStorage
	return current
}

func isSubjectSuperset(desired, current []string) bool {
	for _, existing := range current {
		covered := false
		for _, candidate := range desired {
			if subjectPatternCovers(candidate, existing) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func streamSubjectsCoverFilter(subjects []string, filter string) bool {
	for _, subject := range subjects {
		if subjectPatternCovers(subject, filter) {
			return true
		}
	}
	return false
}

func subjectPatternCovers(desired, current string) bool {
	desiredTokens := strings.Split(desired, ".")
	currentTokens := strings.Split(current, ".")
	for index, desiredToken := range desiredTokens {
		if desiredToken == ">" {
			return index == len(desiredTokens)-1 && index < len(currentTokens)
		}
		if index >= len(currentTokens) {
			return false
		}
		currentToken := currentTokens[index]
		if currentToken == ">" {
			return false
		}
		if desiredToken == "*" {
			continue
		}
		if currentToken == "*" || desiredToken != currentToken {
			return false
		}
	}
	return len(desiredTokens) == len(currentTokens)
}

func sameStrings(left, right []string) bool {
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	return slices.Equal(leftCopy, rightCopy)
}

func decreasesLimit(current, desired int64) bool {
	if current < 0 {
		return desired >= 0
	}
	return desired >= 0 && desired < current
}

func decreasesDuration(current, desired time.Duration) bool {
	if current == 0 {
		return desired > 0
	}
	return desired > 0 && desired < current
}
