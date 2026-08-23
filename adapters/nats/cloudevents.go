package nats

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	natsio "github.com/nats-io/nats.go"
)

const (
	extensionKind          = "kind"
	extensionSchemaVersion = "schemaversion"
	extensionDataEncoding  = "dataencoding"
	extensionCorrelationID = "correlationid"
	extensionCausationID   = "causationid"
	extensionKey           = "key"
	extensionHeaders       = "gmheaders"
	extensionNotBefore     = "notbefore"
	extensionExpiresAt     = "expiresat"
	extensionTraceParent   = "traceparent"
	extensionTraceState    = "tracestate"
)

func encodeCloudEventStructured(native []byte) ([]byte, error) {
	event, _, err := nativeToCloudEvent(native)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: marshal structured CloudEvent: %w", err)
	}
	return data, nil
}

func encodeCloudEventBinary(native []byte, headers natsio.Header) ([]byte, error) {
	event, payload, err := nativeToCloudEvent(native)
	if err != nil {
		return nil, err
	}
	headers.Set("Content-Type", event.DataContentType())
	headers.Set("Ce-Specversion", event.SpecVersion())
	headers.Set("Ce-Id", event.ID())
	headers.Set("Ce-Source", event.Source())
	headers.Set("Ce-Type", event.Type())
	if event.Subject() != "" {
		headers.Set("Ce-Subject", event.Subject())
	}
	if !event.Time().IsZero() {
		headers.Set("Ce-Time", event.Time().UTC().Format(time.RFC3339Nano))
	}
	if event.DataSchema() != "" {
		headers.Set("Ce-Dataschema", event.DataSchema())
	}
	for key, value := range event.Extensions() {
		headers.Set("Ce-"+key, fmt.Sprint(value))
	}
	return payload, nil
}

func nativeToCloudEvent(native []byte) (cloudevents.Event, []byte, error) {
	envelope, err := messenger.UnmarshalEnvelope(native)
	if err != nil {
		return cloudevents.Event{}, nil, err
	}
	if envelope.Kind != messenger.KindEvent {
		return cloudevents.Event{}, nil, fmt.Errorf("%w: CloudEvents only supports events", ErrInvalidConfig)
	}
	payload, encoding, err := envelope.Payload()
	if err != nil {
		return cloudevents.Event{}, nil, err
	}
	event := cloudevents.NewEvent(cloudevents.VersionV1)
	event.SetID(envelope.ID.String())
	event.SetSource(envelope.Source)
	event.SetType(envelope.Name)
	event.SetSubject(envelope.Subject)
	event.SetTime(envelope.Time)
	event.SetDataContentType(envelope.ContentType)
	if envelope.Schema != "" {
		event.SetDataSchema(envelope.Schema)
	}
	event.SetExtension(extensionKind, string(envelope.Kind))
	event.SetExtension(extensionSchemaVersion, envelope.SchemaVersion)
	event.SetExtension(extensionDataEncoding, envelope.DataEncoding.String())
	event.SetExtension(extensionCorrelationID, envelope.CorrelationID.String())
	if !envelope.CausationID.IsZero() {
		event.SetExtension(extensionCausationID, envelope.CausationID.String())
	}
	if envelope.Key != "" {
		event.SetExtension(extensionKey, envelope.Key)
	}
	headers := maps.Clone(envelope.Headers)
	if traceParent := takeHeader(headers, extensionTraceParent); traceParent != "" {
		event.SetExtension(extensionTraceParent, traceParent)
	}
	if traceState := takeHeader(headers, extensionTraceState); traceState != "" {
		event.SetExtension(extensionTraceState, traceState)
	}
	if len(headers) > 0 {
		encodedHeaders, err := json.Marshal(headers)
		if err != nil {
			return cloudevents.Event{}, nil, fmt.Errorf("messenger/nats: encode CloudEvents headers: %w", err)
		}
		event.SetExtension(extensionHeaders, base64.RawURLEncoding.EncodeToString(encodedHeaders))
	}
	if !envelope.NotBefore.IsZero() {
		event.SetExtension(extensionNotBefore, envelope.NotBefore.UTC().Format(time.RFC3339Nano))
	}
	if !envelope.ExpiresAt.IsZero() {
		event.SetExtension(extensionExpiresAt, envelope.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	switch encoding {
	case messenger.DataJSON, messenger.DataText:
		event.DataEncoded = append([]byte(nil), payload...)
	case messenger.DataBinary:
		event.DataEncoded = append([]byte(nil), payload...)
		event.DataBase64 = true
	default:
		return cloudevents.Event{}, nil, fmt.Errorf("%w: unsupported data encoding", ErrInvalidConfig)
	}
	if err := event.Validate(); err != nil {
		return cloudevents.Event{}, nil, fmt.Errorf("messenger/nats: validate CloudEvent: %w", err)
	}
	return event, payload, nil
}

func decodeCloudEventStructured(data []byte) (cloudEventEnvelope, error) {
	if err := validateStructuredSchemaVersion(data); err != nil {
		return cloudEventEnvelope{}, err
	}
	var event cloudevents.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return cloudEventEnvelope{}, fmt.Errorf("messenger/nats: decode structured CloudEvent: %w", err)
	}
	return cloudEventEnvelopeFrom(event, event.Data())
}

func validateStructuredSchemaVersion(data []byte) error {
	var attributes map[string]json.RawMessage
	if err := json.Unmarshal(data, &attributes); err != nil {
		return fmt.Errorf("messenger/nats: decode structured CloudEvent: %w", err)
	}
	raw, ok := attributes[extensionSchemaVersion]
	if !ok {
		return fmt.Errorf("%w: CloudEvent schema version", ErrInvalidConfig)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("%w: CloudEvent schema version", ErrInvalidConfig)
	}
	version, err := extensionInt(value)
	if err != nil || version <= 0 {
		return fmt.Errorf("%w: CloudEvent schema version", ErrInvalidConfig)
	}
	return nil
}

func decodeCloudEventBinary(messageData []byte, headers natsio.Header) (cloudEventEnvelope, error) {
	event := cloudevents.NewEvent(cloudevents.VersionV1)
	event.SetSpecVersion(headers.Get("Ce-Specversion"))
	event.SetID(headers.Get("Ce-Id"))
	event.SetSource(headers.Get("Ce-Source"))
	event.SetType(headers.Get("Ce-Type"))
	event.SetSubject(headers.Get("Ce-Subject"))
	if value := headers.Get("Ce-Time"); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return cloudEventEnvelope{}, fmt.Errorf("messenger/nats: parse CloudEvent time: %w", err)
		}
		event.SetTime(parsed)
	}
	event.SetDataContentType(headers.Get("Content-Type"))
	if value := headers.Get("Ce-Dataschema"); value != "" {
		event.SetDataSchema(value)
	}
	for _, key := range []string{
		extensionKind, extensionSchemaVersion, extensionDataEncoding, extensionCorrelationID, extensionCausationID,
		extensionKey, extensionHeaders, extensionNotBefore, extensionExpiresAt,
		extensionTraceParent, extensionTraceState,
	} {
		if value := headers.Get("Ce-" + key); value != "" {
			event.SetExtension(key, value)
		}
	}
	return cloudEventEnvelopeFrom(event, messageData)
}

type cloudEventEnvelope struct {
	metadata messenger.Metadata
	encoding messenger.DataEncoding
	data     []byte
}

func cloudEventEnvelopeFrom(
	event cloudevents.Event,
	data []byte,
) (cloudEventEnvelope, error) {
	if err := event.Validate(); err != nil {
		return cloudEventEnvelope{}, fmt.Errorf("messenger/nats: invalid CloudEvent: %w", err)
	}
	id, err := messenger.ParseMessageID(event.ID())
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	version, err := extensionInt(event.Extensions()[extensionSchemaVersion])
	if err != nil || version <= 0 {
		return cloudEventEnvelope{}, fmt.Errorf("%w: CloudEvent schema version", ErrInvalidConfig)
	}
	encoding, err := cloudEventDataEncoding(event.Extensions()[extensionDataEncoding])
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	correlationID := id
	if value := extensionString(event.Extensions()[extensionCorrelationID]); value != "" {
		correlationID, err = messenger.ParseMessageID(value)
		if err != nil {
			return cloudEventEnvelope{}, err
		}
	}
	var causationID messenger.MessageID
	if value := extensionString(event.Extensions()[extensionCausationID]); value != "" {
		causationID, err = messenger.ParseMessageID(value)
		if err != nil {
			return cloudEventEnvelope{}, err
		}
	}
	eventTime, err := canonicalCloudEventTime(event.Time(), id)
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	headers, err := decodeHeadersExtension(event.Extensions()[extensionHeaders])
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	if traceParent := extensionString(event.Extensions()[extensionTraceParent]); traceParent != "" {
		setCanonicalHeader(headers, extensionTraceParent, traceParent)
	}
	if traceState := extensionString(event.Extensions()[extensionTraceState]); traceState != "" {
		setCanonicalHeader(headers, extensionTraceState, traceState)
	}
	notBefore, err := extensionTime(event.Extensions()[extensionNotBefore])
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	expiresAt, err := extensionTime(event.Extensions()[extensionExpiresAt])
	if err != nil {
		return cloudEventEnvelope{}, err
	}
	return cloudEventEnvelope{
		metadata: messenger.Metadata{
			ID: id, Kind: messenger.KindEvent, Name: event.Type(), SchemaVersion: version,
			Source: event.Source(), Subject: event.Subject(), Time: eventTime.UTC(),
			CorrelationID: correlationID, CausationID: causationID,
			Key: extensionString(event.Extensions()[extensionKey]), ContentType: event.DataContentType(),
			Schema: event.DataSchema(), Headers: headers, NotBefore: notBefore, ExpiresAt: expiresAt,
		},
		encoding: encoding,
		data:     append([]byte(nil), data...),
	}, nil
}

func cloudEventDataEncoding(value any) (messenger.DataEncoding, error) {
	switch extensionString(value) {
	case messenger.DataJSON.String():
		return messenger.DataJSON, nil
	case messenger.DataText.String():
		return messenger.DataText, nil
	case messenger.DataBinary.String():
		return messenger.DataBinary, nil
	default:
		return 0, fmt.Errorf("%w: CloudEvent data encoding", ErrInvalidConfig)
	}
}

func canonicalCloudEventTime(eventTime time.Time, id messenger.MessageID) (time.Time, error) {
	if !eventTime.IsZero() {
		return eventTime.UTC(), nil
	}
	if id[6]>>4 != 7 {
		return time.Time{}, fmt.Errorf(
			"%w: CloudEvent time is required when id is not UUIDv7", messenger.ErrInvalidMessage,
		)
	}
	milliseconds := int64(id[0])<<40 |
		int64(id[1])<<32 |
		int64(id[2])<<24 |
		int64(id[3])<<16 |
		int64(id[4])<<8 |
		int64(id[5])
	return time.UnixMilli(milliseconds).UTC(), nil
}

func takeHeader(headers map[string]string, name string) string {
	var value string
	for key, candidate := range headers {
		if strings.EqualFold(key, name) {
			if value == "" {
				value = candidate
			}
			delete(headers, key)
		}
	}
	return value
}

func setCanonicalHeader(headers map[string]string, name, value string) {
	for key := range headers {
		if strings.EqualFold(key, name) {
			delete(headers, key)
		}
	}
	headers[name] = value
}

func extensionString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func extensionInt(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		parsed := int(typed)
		if int64(parsed) != typed {
			return 0, fmt.Errorf("invalid extension integer %d", typed)
		}
		return parsed, nil
	case float64:
		parsed := int(typed)
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || float64(parsed) != typed {
			return 0, fmt.Errorf("invalid extension integer %v", typed)
		}
		return parsed, nil
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		return parsed, err
	case string:
		return strconv.Atoi(typed)
	default:
		return 0, fmt.Errorf("unsupported extension integer %T", value)
	}
}

func extensionTime(value any) (time.Time, error) {
	text := extensionString(value)
	if text == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("messenger/nats: parse extension time: %w", err)
	}
	return parsed, nil
}

func decodeHeadersExtension(value any) (map[string]string, error) {
	text := extensionString(value)
	if text == "" {
		return map[string]string{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("messenger/nats: decode header extension: %w", err)
	}
	headers := make(map[string]string)
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, fmt.Errorf("messenger/nats: decode header map: %w", err)
	}
	for key, value := range headers {
		if strings.ContainsAny(key+value, "\r\n") {
			return nil, fmt.Errorf("%w: invalid CloudEvents header map", ErrInvalidConfig)
		}
	}
	return headers, nil
}
