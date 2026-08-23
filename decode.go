package messenger

import "fmt"

// DecodeCommand decodes and verifies a native command envelope.
func DecodeCommand[T any](descriptor Command[T], data []byte) (Message[T], error) {
	return decodeMessage(descriptor.descriptor, data)
}

// DecodeEvent decodes and verifies a native event envelope.
func DecodeEvent[T any](descriptor Event[T], data []byte) (Message[T], error) {
	return decodeMessage(descriptor.descriptor, data)
}

// EncodeCommandEnvelope encodes a typed command with already resolved metadata.
func EncodeCommandEnvelope[T any](descriptor Command[T], metadata Metadata, payload T) ([]byte, error) {
	return encodeTypedEnvelope(descriptor.descriptor, metadata, payload)
}

// EncodeEventEnvelope encodes a typed event with already resolved metadata.
func EncodeEventEnvelope[T any](descriptor Event[T], metadata Metadata, payload T) ([]byte, error) {
	return encodeTypedEnvelope(descriptor.descriptor, metadata, payload)
}

// DecodeCommandPayload decodes codec bytes without an envelope.
func DecodeCommandPayload[T any](descriptor Command[T], data []byte) (T, error) {
	return descriptor.codec.Decode(data)
}

// DecodeEventPayload decodes codec bytes without an envelope.
func DecodeEventPayload[T any](descriptor Event[T], data []byte) (T, error) {
	return descriptor.codec.Decode(data)
}

func encodeTypedEnvelope[T any](descriptor descriptor[T], metadata Metadata, payload T) ([]byte, error) {
	if metadata.Kind != descriptor.info.Kind || metadata.Name != descriptor.info.Name ||
		metadata.SchemaVersion != descriptor.info.SchemaVersion ||
		metadata.ContentType != descriptor.info.ContentType || metadata.Schema != descriptor.info.Schema {
		return nil, fmt.Errorf("%w: metadata does not match %s v%d",
			ErrDescriptorConflict, descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	data, err := descriptor.codec.Encode(payload)
	if err != nil {
		return nil, err
	}
	return MarshalEnvelope(metadata, data, descriptor.codec.Encoding())
}

func decodeMessage[T any](descriptor descriptor[T], data []byte) (Message[T], error) {
	envelope, err := UnmarshalEnvelope(data)
	if err != nil {
		return Message[T]{}, err
	}
	if envelope.Kind != descriptor.info.Kind || envelope.Name != descriptor.info.Name ||
		envelope.SchemaVersion != descriptor.info.SchemaVersion ||
		envelope.ContentType != descriptor.info.ContentType ||
		envelope.DataEncoding != descriptor.info.DataEncoding || envelope.Schema != descriptor.info.Schema {
		return Message[T]{}, fmt.Errorf("%w: envelope does not match %s v%d",
			ErrDescriptorConflict, descriptor.info.Name, descriptor.info.SchemaVersion)
	}
	payload, encoding, err := envelope.Payload()
	if err != nil {
		return Message[T]{}, err
	}
	if encoding != descriptor.codec.Encoding() {
		return Message[T]{}, fmt.Errorf("%w: payload encoding for %s", ErrDescriptorConflict,
			descriptor.info.Name)
	}
	value, err := descriptor.codec.Decode(payload)
	if err != nil {
		return Message[T]{}, err
	}
	return Message[T]{Metadata: envelope.Metadata(), Payload: value}, nil
}
