package nats

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strconv"
	"unicode/utf8"

	messenger "github.com/assurrussa/gomessenger"
)

// QuarantineSpecVersion identifies bounded records that must never be replayed.
const QuarantineSpecVersion = "2.0"

// ErrQuarantineReplay reports an explicitly non-replayable terminal record.
var ErrQuarantineReplay = errors.New("messenger/nats: quarantine records cannot be replayed")

// QuarantineInfo describes a complete or explicitly omitted source capture.
// HeaderBytes counts key and value bytes without protocol framing. InputSHA256
// covers length-prefixed subject, wire mode, exact sorted header keys, ordered
// values, and payload bytes. Replayable is always false.
type QuarantineInfo struct {
	Reason          string `json:"reason"`
	InputSHA256     string `json:"inputSha256"`
	OriginalBytes   int    `json:"originalBytes"`
	HeaderCount     int    `json:"headerCount"`
	HeaderBytes     int    `json:"headerBytes"`
	OriginalOmitted bool   `json:"originalOmitted"`
	Replayable      bool   `json:"replayable"`
}

type preparedDLQ struct {
	data        []byte
	dedupID     string
	contentType string
}

func prepareDLQ(record DLQRecord, headers map[string][]string, data []byte) (preparedDLQ, error) {
	if record.Error == "" {
		record.Error = "unspecified failure"
	}
	if record.FailureKind == "" {
		record.FailureKind = "unknown"
	}
	if !json.Valid(record.Envelope) {
		record.Envelope = nil
	}
	cloned, headerErr := copyReplayHeaders(headers)
	if headerErr != nil || !losslessHeaders(headers) {
		return prepareQuarantine(record, headers, data, "headers_unreplayable")
	}
	if base64.StdEncoding.EncodedLen(len(data)) > DefaultMaxDLQRecordBytes {
		return prepareQuarantine(record, headers, data, "record_too_large")
	}
	record.OriginalHeaders = cloned
	record.OriginalBase64 = base64.StdEncoding.EncodeToString(data)
	if err := validateDLQRecord(record); err != nil {
		return preparedDLQ{}, err
	}
	encoded, err := marshalBoundedDLQ(record)
	if err != nil {
		return preparedDLQ{}, err
	}
	if len(encoded) > DefaultMaxDLQRecordBytes {
		return prepareQuarantine(record, headers, data, "record_too_large")
	}
	return preparedDLQ{data: encoded, dedupID: dlqDedupID(record.ConsumerID, record.Subject,
		record.WireMode, headers, data), contentType: dlqContentType(DLQSpecVersion)}, nil
}

func marshalBoundedDLQ(record DLQRecord) ([]byte, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: encode terminal record: %w", err)
	}
	if len(encoded) > DefaultMaxDLQRecordBytes && len(record.Envelope) != 0 {
		record.Envelope = nil
		encoded, err = json.Marshal(record)
	}
	return encoded, err
}

func prepareQuarantine(record DLQRecord, headers map[string][]string, data []byte, reason string) (preparedDLQ, error) {
	record.SpecVersion = QuarantineSpecVersion
	record.Envelope = nil
	record.OriginalHeaders = nil
	record.OriginalBase64 = ""
	record.Quarantine = &QuarantineInfo{
		Reason: reason, InputSHA256: quarantineDigest(record.Subject, record.WireMode, headers, data),
		OriginalBytes: len(data), HeaderCount: len(headers), HeaderBytes: headerByteCount(headers),
		OriginalOmitted: true,
	}
	// Avoid expanding a source that cannot fit even before JSON escaping.
	sourceBytes := base64.StdEncoding.EncodedLen(len(data)) + record.Quarantine.HeaderBytes
	if losslessHeaders(headers) && sourceBytes <= DefaultMaxDLQRecordBytes {
		record.OriginalHeaders = headers
		record.OriginalBase64 = base64.StdEncoding.EncodeToString(data)
		record.Quarantine.OriginalOmitted = false
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return preparedDLQ{}, fmt.Errorf("messenger/nats: encode quarantine: %w", err)
	}
	if len(encoded) > DefaultMaxDLQRecordBytes {
		record.OriginalHeaders = nil
		record.OriginalBase64 = ""
		record.Quarantine.OriginalOmitted = true
		encoded, err = json.Marshal(record)
		if err != nil {
			return preparedDLQ{}, fmt.Errorf("messenger/nats: encode quarantine summary: %w", err)
		}
	}
	if len(encoded) > DefaultMaxDLQRecordBytes {
		return preparedDLQ{}, fmt.Errorf("%w: quarantine summary exceeds DLQ capacity", messenger.ErrInvalidMessage)
	}
	if err := validateDLQRecord(record); err != nil {
		return preparedDLQ{}, err
	}
	digest := sha256.Sum256([]byte(record.ConsumerID + "\x00" + record.Quarantine.InputSHA256))
	return preparedDLQ{
		data: encoded, dedupID: "gm-quarantine-" + hex.EncodeToString(digest[:]),
		contentType: dlqContentType(QuarantineSpecVersion),
	}, nil
}

func dlqContentType(version string) string {
	return "application/vnd.gomessenger.dlq+json; version=" + version
}

func validateQuarantine(record DLQRecord) error {
	capture := record.Quarantine
	if capture == nil || capture.Replayable || capture.OriginalBytes < 0 || capture.HeaderCount < 0 ||
		capture.HeaderBytes < 0 || len(record.Envelope) != 0 ||
		(capture.Reason != "headers_unreplayable" && capture.Reason != "record_too_large") {
		return fmt.Errorf("%w: invalid quarantine metadata", messenger.ErrInvalidMessage)
	}
	digest, err := hex.DecodeString(capture.InputSHA256)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("%w: invalid quarantine digest", messenger.ErrInvalidMessage)
	}
	if capture.OriginalOmitted {
		if record.OriginalBase64 != "" || len(record.OriginalHeaders) != 0 {
			return fmt.Errorf("%w: omitted quarantine contains source content", messenger.ErrInvalidMessage)
		}
		return nil
	}
	data, err := base64.StdEncoding.DecodeString(record.OriginalBase64)
	if err != nil || len(data) != capture.OriginalBytes || len(record.OriginalHeaders) != capture.HeaderCount ||
		headerByteCount(record.OriginalHeaders) != capture.HeaderBytes || !losslessHeaders(record.OriginalHeaders) ||
		quarantineDigest(record.Subject, record.WireMode, record.OriginalHeaders, data) != capture.InputSHA256 {
		return fmt.Errorf("%w: inconsistent quarantine capture", messenger.ErrInvalidMessage)
	}
	return nil
}

func headerByteCount(headers map[string][]string) int {
	size := 0
	for key, values := range headers {
		size += len(key)
		for _, value := range values {
			size += len(value)
		}
	}
	return size
}

func losslessHeaders(headers map[string][]string) bool {
	for key, values := range headers {
		if !utf8.ValidString(key) {
			return false
		}
		for _, value := range values {
			if !utf8.ValidString(value) {
				return false
			}
		}
	}
	return true
}

func quarantineDigest(subject string, mode WireMode, headers map[string][]string, data []byte) string {
	digest := sha256.New()
	writeDigestField(digest, []byte("gomessenger/quarantine/v2"))
	writeDigestField(digest, []byte(subject))
	writeDigestField(digest, []byte(mode))
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeDigestField(digest, []byte(strconv.Itoa(len(keys))))
	for _, key := range keys {
		writeDigestField(digest, []byte(key))
		writeDigestField(digest, []byte(strconv.Itoa(len(headers[key]))))
		for _, value := range headers[key] {
			writeDigestField(digest, []byte(value))
		}
	}
	writeDigestField(digest, data)
	return hex.EncodeToString(digest.Sum(nil))
}

func writeDigestField(digest hash.Hash, field []byte) {
	// hash.Hash.Write never returns an error.
	_, _ = digest.Write([]byte(strconv.Itoa(len(field)) + ":"))
	_, _ = digest.Write(field)
}
