package nats

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	replayIDPrefix       = "gm-replay-"
	replayIDHeader       = "Gomessenger-Replay-Id"
	replayConsumerHeader = "Gomessenger-Replay-Consumer"
	// Binary CloudEvents can expand a valid native header map by up to eight
	// times through JSON escaping and RawURL base64 encoding. Envelope-derived
	// CloudEvents attributes share the native envelope's one-megabyte bound.
	maxReplayHeaderBytes = messenger.DefaultMaxEnvelopeBytes + 8*messenger.DefaultMaxHeaderBytes
)

// ReplayPlan is the safe, payload-free description of one DLQ replay.
type ReplayPlan struct {
	Subject     string   `json:"subject"`
	WireMode    WireMode `json:"wireMode"`
	MessageID   string   `json:"messageId,omitempty"`
	InputSHA256 string   `json:"inputSha256"`
	ReplayID    string   `json:"replayId"`
}

// ReplayResult reports the broker acknowledgement for a confirmed replay.
type ReplayResult struct {
	Plan      ReplayPlan `json:"plan"`
	Stream    string     `json:"stream"`
	Sequence  uint64     `json:"sequence"`
	Duplicate bool       `json:"duplicate"`
}

// ReplayPublisher is the narrow JetStream surface required by ReplayDLQ.
type ReplayPublisher interface {
	PublishMsg(
		ctx context.Context,
		message *natsio.Msg,
		options ...jetstream.PublishOpt,
	) (*jetstream.PubAck, error)
}

type replayMessage struct {
	subject   string
	mode      WireMode
	messageID string
	headers   map[string][]string
	data      []byte
}

// PlanDLQReplay validates a record and returns a deterministic replay plan
// without connecting to NATS or exposing its payload and headers.
func PlanDLQReplay(record DLQRecord) (ReplayPlan, error) {
	input, err := replayInput(record)
	if err != nil {
		return ReplayPlan{}, err
	}
	inputDigest := replayDigest(input.subject, input.mode, input.headers, input.data)
	hexDigest := hex.EncodeToString(inputDigest[:])
	replayDigest := replayRecordDigest(record, inputDigest)
	return ReplayPlan{
		Subject:     input.subject,
		WireMode:    input.mode,
		MessageID:   input.messageID,
		InputSHA256: hexDigest,
		ReplayID:    replayIDPrefix + hex.EncodeToString(replayDigest[:]),
	}, nil
}

// ReplayDLQ publishes one original wire message with a deterministic
// JetStream deduplication ID and waits for PubAck.
func ReplayDLQ(
	ctx context.Context,
	publisher ReplayPublisher,
	record DLQRecord,
) (ReplayResult, error) {
	if ctx == nil || nilValue(publisher) {
		return ReplayResult{}, fmt.Errorf("%w: invalid replay publisher or context", ErrInvalidConfig)
	}
	plan, err := PlanDLQReplay(record)
	if err != nil {
		return ReplayResult{}, err
	}
	input, err := replayInput(record)
	if err != nil {
		return ReplayResult{}, err
	}
	message := &natsio.Msg{Subject: plan.Subject, Header: natsio.Header(input.headers), Data: input.data}
	message.Header.Set(replayIDHeader, plan.ReplayID)
	message.Header.Set(replayConsumerHeader, record.ConsumerID)
	ack, err := publisher.PublishMsg(ctx, message, jetstream.WithMsgID(plan.ReplayID))
	if err != nil {
		return ReplayResult{}, fmt.Errorf("messenger/nats: replay %s: %w", plan.Subject, err)
	}
	if ack == nil || ack.Stream == "" {
		return ReplayResult{}, errors.New("messenger/nats: replay broker returned an empty publish acknowledgement")
	}
	return ReplayResult{
		Plan:      plan,
		Stream:    ack.Stream,
		Sequence:  ack.Sequence,
		Duplicate: ack.Duplicate,
	}, nil
}

func replayInput(record DLQRecord) (replayMessage, error) {
	if err := validateDLQRecord(record); err != nil {
		return replayMessage{}, err
	}
	if err := validateSubjectToken(record.Subject, true); err != nil {
		return replayMessage{}, fmt.Errorf("%w: replay subject: %w", ErrInvalidConfig, err)
	}
	data, err := base64.StdEncoding.DecodeString(record.OriginalBase64)
	if err != nil {
		return replayMessage{}, fmt.Errorf("%w: invalid originalBase64", messenger.ErrInvalidMessage)
	}
	headers, err := copyReplayHeaders(record.OriginalHeaders)
	if err != nil {
		return replayMessage{}, err
	}
	removeHeader(headers, natsio.MsgIdHdr)
	removeHeader(headers, replayIDHeader)
	removeHeader(headers, replayConsumerHeader)
	switch record.WireMode {
	case WireNative:
		setHeaderIfMissing(headers, "Content-Type", "application/vnd.gomessenger+json; version=1.0")
		envelope, envelopeErr := messenger.UnmarshalEnvelope(data)
		if envelopeErr != nil {
			return replayMessage{}, envelopeErr
		}
		messageID := envelope.ID.String()
		return replayMessageFromRecord(record, messageID, headers, data), nil
	case WireCloudEventsStructured:
		setHeaderIfMissing(headers, "Content-Type", "application/cloudevents+json; charset=utf-8")
		envelope, envelopeErr := decodeCloudEventStructured(data)
		if envelopeErr != nil {
			return replayMessage{}, envelopeErr
		}
		return replayMessageFromRecord(record, envelope.metadata.ID.String(), headers, data), nil
	case WireCloudEventsBinary:
		if len(headers) == 0 {
			return replayMessage{}, fmt.Errorf(
				"%w: legacy binary CloudEvent record has no original headers", messenger.ErrInvalidMessage,
			)
		}
		envelope, envelopeErr := decodeCloudEventBinary(data, natsio.Header(headers))
		if envelopeErr != nil {
			return replayMessage{}, envelopeErr
		}
		return replayMessageFromRecord(record, envelope.metadata.ID.String(), headers, data), nil
	default:
		return replayMessage{}, fmt.Errorf("%w: unsupported replay wire mode", ErrInvalidConfig)
	}
}

func replayMessageFromRecord(
	record DLQRecord,
	messageID string,
	headers map[string][]string,
	data []byte,
) replayMessage {
	return replayMessage{
		subject: record.Subject, mode: record.WireMode, messageID: messageID,
		headers: headers, data: data,
	}
}

func validateDLQRecord(record DLQRecord) error {
	if record.SpecVersion != DLQSpecVersion || record.ConsumerID == "" ||
		record.Subject == "" || record.Attempt == 0 || record.FailureKind == "" ||
		record.Error == "" || record.FailedAt.IsZero() || !record.WireMode.valid() {
		return fmt.Errorf("%w: incomplete DLQ record", messenger.ErrInvalidMessage)
	}
	if _, err := base64.StdEncoding.DecodeString(record.OriginalBase64); err != nil {
		return fmt.Errorf("%w: invalid originalBase64", messenger.ErrInvalidMessage)
	}
	_, err := copyReplayHeaders(record.OriginalHeaders)
	return err
}

func copyReplayHeaders(source map[string][]string) (map[string][]string, error) {
	if len(source) > messenger.DefaultMaxHeaders {
		return nil, fmt.Errorf("%w: too many DLQ headers", messenger.ErrInvalidMessage)
	}
	if len(source) == 0 {
		return make(map[string][]string), nil
	}
	total := 0
	result := make(map[string][]string, len(source))
	seen := make(map[string]struct{}, len(source))
	for key, values := range source {
		if key == "" || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\r\n") {
			return nil, fmt.Errorf("%w: invalid DLQ header", messenger.ErrInvalidMessage)
		}
		canonicalKey := strings.ToLower(key)
		if _, exists := seen[canonicalKey]; exists {
			return nil, fmt.Errorf("%w: duplicate DLQ header", messenger.ErrInvalidMessage)
		}
		seen[canonicalKey] = struct{}{}
		total += len(key)
		cloned := make([]string, len(values))
		for index, value := range values {
			if strings.ContainsAny(value, "\r\n") {
				return nil, fmt.Errorf("%w: invalid DLQ header", messenger.ErrInvalidMessage)
			}
			total += len(value)
			cloned[index] = value
		}
		result[key] = cloned
	}
	if total > maxReplayHeaderBytes {
		return nil, fmt.Errorf("%w: DLQ headers exceed byte limit", messenger.ErrInvalidMessage)
	}
	return result, nil
}

func replayDigest(subject string, mode WireMode, headers map[string][]string, data []byte) [sha256.Size]byte {
	hash := sha256.New()
	writeLength := func(length int) {
		_, _ = hash.Write([]byte(strconv.Itoa(length)))
		_, _ = hash.Write([]byte{':'})
	}
	writeBytes := func(value []byte) {
		writeLength(len(value))
		_, _ = hash.Write(value)
	}
	writeBytes([]byte(subject))
	writeBytes([]byte(mode))
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		return strings.ToLower(keys[left]) < strings.ToLower(keys[right])
	})
	writeLength(len(keys))
	for _, key := range keys {
		writeBytes([]byte(strings.ToLower(key)))
		writeLength(len(headers[key]))
		for _, value := range headers[key] {
			writeBytes([]byte(value))
		}
	}
	writeBytes(data)
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func replayRecordDigest(record DLQRecord, inputDigest [sha256.Size]byte) [sha256.Size]byte {
	hash := sha256.New()
	writeField := func(value string) {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(value))
	}
	writeField(record.SpecVersion)
	writeField(record.ConsumerID)
	writeField(strconv.FormatUint(record.Attempt, 10))
	writeField(record.FailureKind)
	writeField(record.Error)
	writeField(record.FailedAt.UTC().Format(time.RFC3339Nano))
	_, _ = hash.Write(inputDigest[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func replayAttemptGeneration(headers natsio.Header, consumerID string) string {
	target, ok := singleHeaderValue(headers, replayConsumerHeader)
	if !ok || target != consumerID {
		return ""
	}
	replayID, ok := singleHeaderValue(headers, replayIDHeader)
	if !ok || len(replayID) != len(replayIDPrefix)+sha256.Size*2 || !strings.HasPrefix(replayID, replayIDPrefix) {
		return ""
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(replayID, replayIDPrefix)); err != nil {
		return ""
	}
	return replayID
}

func singleHeaderValue(headers natsio.Header, name string) (string, bool) {
	var value string
	found := false
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		if found || len(values) != 1 {
			return "", false
		}
		value = values[0]
		found = true
	}
	return value, found
}

func removeHeader(headers map[string][]string, name string) {
	for key := range headers {
		if strings.EqualFold(key, name) {
			delete(headers, key)
		}
	}
}

func setHeaderIfMissing(headers map[string][]string, name, value string) {
	for key := range headers {
		if strings.EqualFold(key, name) {
			return
		}
	}
	headers[name] = []string{value}
}

var _ ReplayPublisher = jetstream.JetStream(nil)
