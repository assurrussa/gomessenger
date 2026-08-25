package kafka

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	maxKafkaNameBytes   = 249
	serviceSegment      = "gm"
	serviceMarker       = "." + serviceSegment + "."
	transactionIDPrefix = "gm.tx.v1."
)

// Topic returns the deterministic source topic for a descriptor. The gm segment
// is reserved in namespaces and descriptor names for consumer service names.
func Topic(namespace string, descriptor messenger.DescriptorInfo) (string, error) {
	if err := validateNamespace(namespace); err != nil {
		return "", err
	}
	if descriptor.Kind != messenger.KindCommand && descriptor.Kind != messenger.KindEvent ||
		descriptor.Name == "" || descriptor.SchemaVersion <= 0 {
		return "", fmt.Errorf("%w: descriptor topic", ErrInvalidConfig)
	}
	if err := validateDescriptorName(descriptor.Name); err != nil {
		return "", err
	}
	topic := namespace + "." + string(descriptor.Kind) + "." + descriptor.Name + ".v" +
		strconv.Itoa(descriptor.SchemaVersion)
	if err := validateTopicName(topic); err != nil {
		return "", err
	}
	return topic, nil
}

func consumerTopic(sourceTopic, consumerID, suffix string) (string, error) {
	if err := validateSourceTopicName(sourceTopic); err != nil {
		return "", err
	}
	if err := validateConsumerID(consumerID); err != nil {
		return "", err
	}
	topic := sourceTopic + serviceMarker + consumerID + "." + suffix
	if err := validateTopicName(topic); err != nil {
		return "", err
	}
	return topic, nil
}

func retryTopic(sourceTopic, consumerID string, tier int) (string, error) {
	if tier < 0 {
		return "", fmt.Errorf("%w: negative retry tier", ErrInvalidConfig)
	}
	return consumerTopic(sourceTopic, consumerID, "retry.t"+strconv.Itoa(tier))
}

// RetryTopic returns one deterministic consumer-specific retry tier topic.
func RetryTopic(sourceTopic, consumerID string, tier int) (string, error) {
	return retryTopic(sourceTopic, consumerID, tier)
}

func replayTopic(sourceTopic, consumerID string) (string, error) {
	return consumerTopic(sourceTopic, consumerID, "replay")
}

// ReplayTopic returns the deterministic protected replay-ingress topic.
func ReplayTopic(sourceTopic, consumerID string) (string, error) {
	return replayTopic(sourceTopic, consumerID)
}

func dlqTopic(sourceTopic, consumerID string) (string, error) {
	return consumerTopic(sourceTopic, consumerID, "dlq")
}

// DLQTopic returns the deterministic consumer-specific dead-letter topic.
func DLQTopic(sourceTopic, consumerID string) (string, error) {
	return dlqTopic(sourceTopic, consumerID)
}

func consumerGroup(sourceTopic, consumerID string) (string, error) {
	if err := validateSourceTopicName(sourceTopic); err != nil {
		return "", err
	}
	if err := validateConsumerID(consumerID); err != nil {
		return "", err
	}
	group := sourceTopic + serviceMarker + consumerID
	if err := validateKafkaIdentifier("consumer group", group); err != nil {
		return "", err
	}
	return group, nil
}

// ConsumerGroup returns the stable consumer group for one canonical source and consumer ID.
func ConsumerGroup(sourceTopic, consumerID string) (string, error) {
	return consumerGroup(sourceTopic, consumerID)
}

func transactionalID(group, instanceID string, worker int) (string, error) {
	if worker < 0 {
		return "", fmt.Errorf("%w: negative worker index", ErrInvalidConfig)
	}
	if err := validateKafkaIdentifier("transactional group", group); err != nil {
		return "", err
	}
	if err := validateInstanceID(instanceID); err != nil {
		return "", err
	}
	workerID := strconv.Itoa(worker)
	digest := sha256.Sum256([]byte(group + "\x00" + instanceID + "\x00" + workerID))
	identifier := transactionIDPrefix + hex.EncodeToString(digest[:]) + ".w" + workerID
	if err := validateKafkaIdentifier("transactional ID", identifier); err != nil {
		return "", err
	}
	return identifier, nil
}

func groupInstanceID(instanceID string, worker int) (string, error) {
	if worker < 0 {
		return "", fmt.Errorf("%w: negative worker index", ErrInvalidConfig)
	}
	if err := validateInstanceID(instanceID); err != nil {
		return "", err
	}
	identifier := instanceID + ".w" + strconv.Itoa(worker)
	if err := validateKafkaIdentifier("group instance ID", identifier); err != nil {
		return "", err
	}
	return identifier, nil
}

func expectedRecordKey(metadata messenger.Metadata) []byte {
	if metadata.Key != "" {
		return []byte(metadata.Key)
	}
	return []byte(metadata.ID.String())
}

func validateRecordKey(key []byte, metadata messenger.Metadata) error {
	if !bytes.Equal(key, expectedRecordKey(metadata)) {
		return fmt.Errorf("%w: Kafka record key does not match envelope key", messenger.ErrInvalidMessage)
	}
	return nil
}

func validateConsumerID(value string) error {
	return validateKafkaToken("consumer ID", value)
}

func validateInstanceID(value string) error {
	return validateKafkaIdentifier("instance ID", value)
}

func validateNamespace(value string) error {
	if err := validateKafkaToken("namespace", value); err != nil {
		return err
	}
	for _, segment := range strings.Split(value, ".") {
		if segment == string(messenger.KindCommand) || segment == string(messenger.KindEvent) ||
			segment == serviceSegment {
			return fmt.Errorf("%w: reserved namespace segment %q", ErrInvalidConfig, segment)
		}
	}
	return nil
}

func validateDescriptorName(value string) error {
	for _, segment := range strings.Split(value, ".") {
		if segment == serviceSegment {
			return fmt.Errorf("%w: reserved descriptor segment %q", ErrInvalidConfig, segment)
		}
	}
	return nil
}

func validateKafkaToken(role, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s %q", ErrInvalidConfig, role, value)
	}
	for _, character := range []byte(value) {
		if kafkaIdentifierByte(character) {
			continue
		}
		return fmt.Errorf("%w: %s %q", ErrInvalidConfig, role, value)
	}
	return nil
}

func validateTopicName(value string) error {
	if err := validateKafkaIdentifier("topic", value); err != nil {
		return err
	}
	if value == "." || value == ".." {
		return fmt.Errorf("%w: reserved topic %q", ErrInvalidConfig, value)
	}
	return nil
}

func validateKafkaIdentifier(role, value string) error {
	if len(value) == 0 || len(value) > maxKafkaNameBytes || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s %q", ErrInvalidConfig, role, value)
	}
	for _, character := range []byte(value) {
		if kafkaIdentifierByte(character) {
			continue
		}
		return fmt.Errorf("%w: %s %q", ErrInvalidConfig, role, value)
	}
	return nil
}

func kafkaIdentifierByte(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' || character == '.' || character == '-'
}

func validateSourceTopicName(value string) error {
	for _, kind := range []messenger.Kind{messenger.KindCommand, messenger.KindEvent} {
		marker := "." + string(kind) + "."
		for offset := 0; offset < len(value); {
			relative := strings.Index(value[offset:], marker)
			if relative < 0 {
				break
			}
			index := offset + relative
			namespace := value[:index]
			tail := value[index+len(marker):]
			versionIndex := strings.LastIndex(tail, ".v")
			if versionIndex > 0 {
				version, err := strconv.Atoi(tail[versionIndex+2:])
				descriptor := tail[:versionIndex]
				if err == nil && version > 0 {
					expected, topicErr := Topic(namespace, messenger.DescriptorInfo{
						Kind: kind, Name: descriptor, SchemaVersion: version,
					})
					if topicErr == nil && expected == value {
						return nil
					}
				}
			}
			offset = index + len(marker)
		}
	}
	return fmt.Errorf("%w: source topic %q does not match namespace.kind.descriptor.vN", ErrInvalidConfig, value)
}

func sourceTopicMatchesMetadata(sourceTopic string, metadata messenger.Metadata) error {
	expectedSuffix := "." + string(metadata.Kind) + "." + metadata.Name + ".v" +
		strconv.Itoa(metadata.SchemaVersion)
	if !strings.HasSuffix(sourceTopic, expectedSuffix) {
		return fmt.Errorf("%w: Kafka source topic does not match envelope descriptor", messenger.ErrInvalidMessage)
	}
	namespace := strings.TrimSuffix(sourceTopic, expectedSuffix)
	expected, err := Topic(namespace, messenger.DescriptorInfo{
		Kind: metadata.Kind, Name: metadata.Name, SchemaVersion: metadata.SchemaVersion,
	})
	if err != nil || expected != sourceTopic {
		return fmt.Errorf("%w: Kafka source topic does not match envelope descriptor", messenger.ErrInvalidMessage)
	}
	return nil
}
