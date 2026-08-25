package kafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// DLQSpecVersion is the Kafka dead-letter record schema version.
	DLQSpecVersion = "1.0"
	// DefaultMaxDLQRecordBytes bounds encoded Kafka DLQ JSON.
	DefaultMaxDLQRecordBytes = 3 * messenger.DefaultMaxEnvelopeBytes
	// DefaultMaxSourceMessageBytes covers a full envelope, its duplicated Kafka
	// record key, and bounded control-header and record-framing headroom.
	DefaultMaxSourceMessageBytes = 2*messenger.DefaultMaxEnvelopeBytes + messenger.DefaultMaxHeaderBytes
	// DefaultMaxDLQMessageBytes includes bounded Kafka record framing headroom.
	DefaultMaxDLQMessageBytes = DefaultMaxDLQRecordBytes + messenger.DefaultMaxHeaderBytes

	failureKindDecode = "decode"
)

// DLQRecord is the stable Kafka dead-letter record committed atomically with
// the consumed source or retry offset.
type DLQRecord struct {
	SpecVersion       string    `json:"specVersion"`
	ConsumerID        string    `json:"consumerId"`
	SourceTopic       string    `json:"sourceTopic"`
	SourcePartition   int32     `json:"sourcePartition"`
	SourceOffset      int64     `json:"sourceOffset"`
	RecordKeyBase64   string    `json:"recordKeyBase64"`
	OriginalBase64    string    `json:"originalBase64"`
	MessageID         string    `json:"messageId,omitempty"`
	Attempt           uint64    `json:"attempt"`
	AttemptGeneration string    `json:"attemptGeneration,omitempty"`
	FailureKind       string    `json:"failureKind"`
	Error             string    `json:"error"`
	FailedAt          time.Time `json:"failedAt"`
}

// DecodeDLQRecord strictly decodes one bounded Kafka DLQ record.
func DecodeDLQRecord(data []byte) (DLQRecord, error) {
	if len(data) == 0 || len(data) > DefaultMaxDLQRecordBytes {
		return DLQRecord{}, fmt.Errorf("%w: invalid Kafka DLQ record size", messenger.ErrInvalidMessage)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record DLQRecord
	if err := decoder.Decode(&record); err != nil {
		return DLQRecord{}, fmt.Errorf("%w: decode Kafka DLQ record: %w", messenger.ErrInvalidMessage, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return DLQRecord{}, fmt.Errorf("%w: trailing Kafka DLQ JSON", messenger.ErrInvalidMessage)
	}
	var required struct {
		SourcePartition *int32  `json:"sourcePartition"`
		SourceOffset    *int64  `json:"sourceOffset"`
		RecordKeyBase64 *string `json:"recordKeyBase64"`
		OriginalBase64  *string `json:"originalBase64"`
	}
	if err := json.Unmarshal(data, &required); err != nil {
		return DLQRecord{}, fmt.Errorf("%w: decode Kafka DLQ required fields: %w", messenger.ErrInvalidMessage, err)
	}
	if required.SourcePartition == nil || required.SourceOffset == nil {
		return DLQRecord{}, fmt.Errorf("%w: missing Kafka DLQ source position", messenger.ErrInvalidMessage)
	}
	if required.RecordKeyBase64 == nil || required.OriginalBase64 == nil {
		return DLQRecord{}, fmt.Errorf("%w: missing Kafka DLQ original data", messenger.ErrInvalidMessage)
	}
	if err := validateDLQRecord(record); err != nil {
		return DLQRecord{}, err
	}
	return record, nil
}

// ReplayPlan is a deterministic payload-free Kafka DLQ replay description.
type ReplayPlan struct {
	Topic       string `json:"topic"`
	MessageID   string `json:"messageId,omitempty"`
	InputSHA256 string `json:"inputSha256"`
	ReplayID    string `json:"replayId"`
}

// ReplayResult reports the committed Kafka location of a replay ingress record.
type ReplayResult struct {
	Plan      ReplayPlan `json:"plan"`
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
}

// ReplayPublisher is the narrow committed-record surface used by ReplayDLQ.
type ReplayPublisher interface {
	PublishReplay(ctx context.Context, record *kgo.Record) (*kgo.Record, error)
}

// PlanDLQReplay validates a record and returns a deterministic payload-free plan.
func PlanDLQReplay(record DLQRecord) (ReplayPlan, error) {
	if err := validateDLQRecord(record); err != nil {
		return ReplayPlan{}, err
	}
	original, err := base64.StdEncoding.DecodeString(record.OriginalBase64)
	if err != nil {
		return ReplayPlan{}, fmt.Errorf("%w: invalid originalBase64", messenger.ErrInvalidMessage)
	}
	canonical, err := messenger.CanonicalizeEnvelope(original)
	if err != nil {
		return ReplayPlan{}, err
	}
	envelope, err := messenger.UnmarshalEnvelope(canonical)
	if err != nil {
		return ReplayPlan{}, err
	}
	if record.MessageID != "" && record.MessageID != envelope.ID.String() {
		return ReplayPlan{}, fmt.Errorf("%w: Kafka DLQ message identity conflict", messenger.ErrInvalidMessage)
	}
	key, err := base64.StdEncoding.DecodeString(record.RecordKeyBase64)
	if err != nil {
		return ReplayPlan{}, fmt.Errorf("%w: invalid recordKeyBase64", messenger.ErrInvalidMessage)
	}
	if err := validateRecordKey(key, envelope.Metadata()); err != nil {
		return ReplayPlan{}, err
	}
	if err := sourceTopicMatchesMetadata(record.SourceTopic, envelope.Metadata()); err != nil {
		return ReplayPlan{}, err
	}
	replayTopicName, err := replayTopic(record.SourceTopic, record.ConsumerID)
	if err != nil {
		return ReplayPlan{}, err
	}
	replayID, err := newReplayID(record)
	if err != nil {
		return ReplayPlan{}, err
	}
	digest := sha256.Sum256(original)
	return ReplayPlan{
		Topic: replayTopicName, MessageID: envelope.ID.String(), InputSHA256: hex.EncodeToString(digest[:]), ReplayID: replayID,
	}, nil
}

// ReplayDLQ publishes the original canonical bytes to the protected replay topic.
func ReplayDLQ(ctx context.Context, publisher ReplayPublisher, record DLQRecord) (ReplayResult, error) {
	if ctx == nil || nilValue(publisher) {
		return ReplayResult{}, fmt.Errorf("%w: replay publisher", ErrInvalidConfig)
	}
	plan, err := PlanDLQReplay(record)
	if err != nil {
		return ReplayResult{}, err
	}
	value, _ := base64.StdEncoding.DecodeString(record.OriginalBase64)
	key, _ := base64.StdEncoding.DecodeString(record.RecordKeyBase64)
	metadata := controlMetadata{
		source:            sourcePosition{topic: record.SourceTopic, partition: record.SourcePartition, offset: record.SourceOffset},
		attemptGeneration: plan.ReplayID,
	}
	published, err := publisher.PublishReplay(ctx, &kgo.Record{
		Topic: plan.Topic, Key: key, Value: value, Headers: controlHeaders(metadata),
	})
	if err != nil {
		return ReplayResult{}, fmt.Errorf("messenger/kafka: replay %s: %w", plan.Topic, err)
	}
	if published == nil || published.Topic != plan.Topic || published.Partition < 0 || published.Offset < 0 {
		return ReplayResult{}, errors.New("messenger/kafka: replay returned invalid broker metadata")
	}
	return ReplayResult{Plan: plan, Partition: published.Partition, Offset: published.Offset}, nil
}

func makeDLQRecord(
	consumerID string,
	record *kgo.Record,
	control controlMetadata,
	messageID string,
	attempt uint64,
	failureKind string,
	failure error,
	failedAt time.Time,
) DLQRecord {
	failureText := "unspecified failure"
	if failure != nil {
		failureText = truncate(failure.Error(), 1024)
		if failureText == "" {
			failureText = "unspecified failure"
		}
	}
	return DLQRecord{
		SpecVersion: DLQSpecVersion, ConsumerID: consumerID,
		SourceTopic: control.source.topic, SourcePartition: control.source.partition, SourceOffset: control.source.offset,
		RecordKeyBase64: base64.StdEncoding.EncodeToString(record.Key),
		OriginalBase64:  base64.StdEncoding.EncodeToString(record.Value),
		MessageID:       messageID, Attempt: attempt, AttemptGeneration: control.attemptGeneration,
		FailureKind: failureKind, Error: failureText, FailedAt: failedAt.UTC(),
	}
}

func encodeDLQRecord(record DLQRecord) ([]byte, error) {
	if err := validateDLQRecord(record); err != nil {
		return nil, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("messenger/kafka: encode DLQ record: %w", err)
	}
	if len(data) > DefaultMaxDLQRecordBytes {
		return nil, fmt.Errorf("%w: Kafka DLQ record exceeds %d bytes", messenger.ErrInvalidMessage,
			DefaultMaxDLQRecordBytes)
	}
	return data, nil
}

func validateDLQRecord(record DLQRecord) error {
	if record.SpecVersion != DLQSpecVersion || record.ConsumerID == "" || record.SourceTopic == "" ||
		record.SourcePartition < 0 || record.SourceOffset < 0 || record.Attempt == 0 || record.FailureKind == "" ||
		record.Error == "" || record.FailedAt.IsZero() || len(record.Error) > 1024 ||
		strings.TrimSpace(record.AttemptGeneration) != record.AttemptGeneration || len(record.AttemptGeneration) > 128 {
		return fmt.Errorf("%w: incomplete Kafka DLQ record", messenger.ErrInvalidMessage)
	}
	if err := validateConsumerID(record.ConsumerID); err != nil {
		return err
	}
	if err := validateTopicName(record.SourceTopic); err != nil {
		return err
	}
	if _, err := base64.StdEncoding.DecodeString(record.RecordKeyBase64); err != nil {
		return fmt.Errorf("%w: invalid recordKeyBase64", messenger.ErrInvalidMessage)
	}
	if _, err := base64.StdEncoding.DecodeString(record.OriginalBase64); err != nil {
		return fmt.Errorf("%w: invalid originalBase64", messenger.ErrInvalidMessage)
	}
	return nil
}

func recordDigest(record DLQRecord) ([sha256.Size]byte, error) {
	if err := validateDLQRecord(record); err != nil {
		return [sha256.Size]byte{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 {
		return ""
	}
	if len(value) > limit {
		end := limit
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		value = value[:end]
	}
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) > limit {
		end := limit
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		value = value[:end]
	}
	return value
}
