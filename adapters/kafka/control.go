package kafka

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerSourceTopic       = "gomessenger-source-topic"
	headerSourcePartition   = "gomessenger-source-partition"
	headerSourceOffset      = "gomessenger-source-offset"
	headerAttempt           = "gomessenger-attempt"
	headerNotBefore         = "gomessenger-not-before"
	headerAttemptGeneration = "gomessenger-attempt-generation"
	replayIDPrefix          = "gm-replay-"
)

var reservedHeaders = map[string]struct{}{
	headerSourceTopic: {}, headerSourcePartition: {}, headerSourceOffset: {},
	headerAttempt: {}, headerNotBefore: {}, headerAttemptGeneration: {},
}

type sourcePosition struct {
	topic     string
	partition int32
	offset    int64
}

type controlMetadata struct {
	source            sourcePosition
	attempt           uint64
	notBefore         time.Time
	attemptGeneration string
}

func sourceControl(record *kgo.Record) controlMetadata {
	return controlMetadata{source: sourcePosition{
		topic: record.Topic, partition: record.Partition, offset: record.Offset,
	}}
}

func parseControl(record *kgo.Record, sourceTopic, replayTopic string, retryTopics map[string]struct{}) (controlMetadata, error) {
	if record == nil {
		return controlMetadata{}, fmt.Errorf("%w: nil Kafka record", messenger.ErrInvalidMessage)
	}
	switch record.Topic {
	case sourceTopic:
		for _, header := range record.Headers {
			if _, reserved := reservedHeaders[strings.ToLower(header.Key)]; reserved {
				return controlMetadata{}, fmt.Errorf("%w: reserved control header on source topic",
					messenger.ErrInvalidMessage)
			}
		}
		return sourceControl(record), nil
	case replayTopic:
		metadata, err := decodeControlHeaders(record.Headers)
		if err != nil {
			return controlMetadata{}, err
		}
		if !strings.HasPrefix(metadata.attemptGeneration, replayIDPrefix) {
			return controlMetadata{}, fmt.Errorf("%w: replay has no valid attempt generation",
				messenger.ErrInvalidMessage)
		}
		return metadata, nil
	default:
		if _, ok := retryTopics[record.Topic]; !ok {
			return controlMetadata{}, fmt.Errorf("%w: unexpected consumer topic %q", messenger.ErrInvalidMessage, record.Topic)
		}
		return decodeControlHeaders(record.Headers)
	}
}

func decodeControlHeaders(headers []kgo.RecordHeader) (controlMetadata, error) {
	values := make(map[string]string, len(headers))
	for _, header := range headers {
		key := strings.ToLower(header.Key)
		if _, reserved := reservedHeaders[key]; !reserved {
			return controlMetadata{}, fmt.Errorf("%w: unexpected Kafka record header %q",
				messenger.ErrInvalidMessage, header.Key)
		}
		if _, exists := values[key]; exists {
			return controlMetadata{}, fmt.Errorf("%w: duplicate Kafka record header %q",
				messenger.ErrInvalidMessage, header.Key)
		}
		if len(header.Value) > 512 {
			return controlMetadata{}, fmt.Errorf("%w: Kafka control header is too large", messenger.ErrInvalidMessage)
		}
		values[key] = string(header.Value)
	}
	for _, required := range []string{headerSourceTopic, headerSourcePartition, headerSourceOffset, headerAttempt} {
		if values[required] == "" {
			return controlMetadata{}, fmt.Errorf("%w: missing Kafka control header %q",
				messenger.ErrInvalidMessage, required)
		}
	}
	if err := validateTopicName(values[headerSourceTopic]); err != nil {
		return controlMetadata{}, err
	}
	partition, err := strconv.ParseInt(values[headerSourcePartition], 10, 32)
	if err != nil || partition < 0 {
		return controlMetadata{}, fmt.Errorf("%w: invalid source partition", messenger.ErrInvalidMessage)
	}
	offset, err := strconv.ParseInt(values[headerSourceOffset], 10, 64)
	if err != nil || offset < 0 {
		return controlMetadata{}, fmt.Errorf("%w: invalid source offset", messenger.ErrInvalidMessage)
	}
	attempt, err := strconv.ParseUint(values[headerAttempt], 10, 64)
	if err != nil {
		return controlMetadata{}, fmt.Errorf("%w: invalid handler attempt", messenger.ErrInvalidMessage)
	}
	var notBefore time.Time
	if value := values[headerNotBefore]; value != "" {
		notBefore, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return controlMetadata{}, fmt.Errorf("%w: invalid retry not-before", messenger.ErrInvalidMessage)
		}
		notBefore = notBefore.UTC()
	}
	generation := values[headerAttemptGeneration]
	if strings.TrimSpace(generation) != generation || len(generation) > 128 {
		return controlMetadata{}, fmt.Errorf("%w: invalid attempt generation", messenger.ErrInvalidMessage)
	}
	return controlMetadata{
		source:  sourcePosition{topic: values[headerSourceTopic], partition: int32(partition), offset: offset},
		attempt: attempt, notBefore: notBefore, attemptGeneration: generation,
	}, nil
}

func controlHeaders(metadata controlMetadata) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: headerSourceTopic, Value: []byte(metadata.source.topic)},
		{Key: headerSourcePartition, Value: []byte(strconv.FormatInt(int64(metadata.source.partition), 10))},
		{Key: headerSourceOffset, Value: []byte(strconv.FormatInt(metadata.source.offset, 10))},
		{Key: headerAttempt, Value: []byte(strconv.FormatUint(metadata.attempt, 10))},
	}
	if !metadata.notBefore.IsZero() {
		headers = append(headers, kgo.RecordHeader{
			Key: headerNotBefore, Value: []byte(metadata.notBefore.UTC().Format(time.RFC3339Nano)),
		})
	}
	if metadata.attemptGeneration != "" {
		headers = append(headers, kgo.RecordHeader{
			Key: headerAttemptGeneration, Value: []byte(metadata.attemptGeneration),
		})
	}
	return headers
}

func retryDelay(base, maximum time.Duration, attempt uint64) time.Duration {
	return retryDelayWithReader(rand.Reader, base, maximum, attempt)
}

func retryDelayWithReader(random io.Reader, base, maximum time.Duration, attempt uint64) time.Duration {
	delay := base
	for current := uint64(1); current < attempt && delay < maximum; current++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	if delay <= 1 {
		return delay
	}
	value, err := rand.Int(random, big.NewInt(int64(delay)))
	if err != nil {
		return delay
	}
	return time.Duration(value.Int64() + 1)
}

func retryTier(tiers []time.Duration, delay time.Duration) int {
	for index, tier := range tiers {
		if delay <= tier {
			return index
		}
	}
	return len(tiers) - 1
}

func newReplayID(record DLQRecord) (string, error) {
	digest, err := recordDigest(record)
	if err != nil {
		return "", err
	}
	return replayIDPrefix + hex.EncodeToString(digest[:]), nil
}

func fetchError(fetches kgo.Fetches, pollErr error) error {
	errs := fetches.Errors()
	if len(errs) == 0 {
		return nil
	}
	joined := make([]error, 0, len(errs))
	for _, fetchErr := range errs {
		if pollErr != nil && errors.Is(fetchErr.Err, pollErr) {
			continue
		}
		joined = append(joined, fmt.Errorf("topic %s partition %d: %w",
			fetchErr.Topic, fetchErr.Partition, fetchErr.Err))
	}
	if len(joined) == 0 {
		return nil
	}
	return errors.Join(joined...)
}
