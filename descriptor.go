package messenger

import (
	"fmt"
	"reflect"
	"regexp"
)

const maxDescriptorNameLength = 200

var descriptorNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)

// DescriptorInfo is the transport-neutral public identity of a descriptor.
type DescriptorInfo struct {
	Kind          Kind         `json:"kind"`
	Name          string       `json:"name"`
	SchemaVersion int          `json:"schemaVersion"`
	ContentType   string       `json:"contentType"`
	DataEncoding  DataEncoding `json:"dataEncoding"`
	Schema        string       `json:"schema,omitempty"`
}

type descriptor[T any] struct {
	info  DescriptorInfo
	codec Codec[T]
}

// Command is an immutable typed command descriptor.
type Command[T any] struct{ descriptor[T] }

// Event is an immutable typed event descriptor.
type Event[T any] struct{ descriptor[T] }

// Query is an immutable typed local query descriptor. Its codec describes the
// request Q only; R is a compile-time result identity and is never serialized.
type Query[Q, R any] struct {
	descriptor[Q]
	resultType reflect.Type
}

// NewCommand constructs a command descriptor.
func NewCommand[T any](name string, schemaVersion int, codec Codec[T]) (Command[T], error) {
	descriptor, err := newDescriptor(KindCommand, name, schemaVersion, "", codec)
	return Command[T]{descriptor: descriptor}, err
}

// MustCommand constructs a command descriptor and panics when its declaration is invalid.
func MustCommand[T any](name string, schemaVersion int, codec Codec[T]) Command[T] {
	command, err := NewCommand(name, schemaVersion, codec)
	if err != nil {
		panic(err)
	}
	return command
}

// NewEvent constructs an event descriptor.
func NewEvent[T any](name string, schemaVersion int, codec Codec[T]) (Event[T], error) {
	descriptor, err := newDescriptor(KindEvent, name, schemaVersion, "", codec)
	return Event[T]{descriptor: descriptor}, err
}

// MustEvent constructs an event descriptor and panics when its declaration is invalid.
func MustEvent[T any](name string, schemaVersion int, codec Codec[T]) Event[T] {
	event, err := NewEvent(name, schemaVersion, codec)
	if err != nil {
		panic(err)
	}
	return event
}

// NewQuery constructs a typed local query descriptor.
func NewQuery[Q, R any](name string, schemaVersion int, codec Codec[Q]) (Query[Q, R], error) {
	descriptor, err := newDescriptor(KindQuery, name, schemaVersion, "", codec)
	return Query[Q, R]{descriptor: descriptor, resultType: reflect.TypeFor[R]()}, err
}

// MustQuery constructs a typed local query descriptor and panics when its declaration is invalid.
func MustQuery[Q, R any](name string, schemaVersion int, codec Codec[Q]) Query[Q, R] {
	query, err := NewQuery[Q, R](name, schemaVersion, codec)
	if err != nil {
		panic(err)
	}
	return query
}

// WithSchema returns a command descriptor with an explicit schema URI.
func (d Command[T]) WithSchema(schema string) Command[T] {
	d.info.Schema = schema
	return d
}

// WithSchema returns an event descriptor with an explicit schema URI.
func (d Event[T]) WithSchema(schema string) Event[T] {
	d.info.Schema = schema
	return d
}

// WithSchema returns a query descriptor with an explicit request schema URI.
func (d Query[Q, R]) WithSchema(schema string) Query[Q, R] {
	d.info.Schema = schema
	return d
}

// Info returns a copy of the command's wire identity.
func (d Command[T]) Info() DescriptorInfo { return d.info }

// Info returns a copy of the event's wire identity.
func (d Event[T]) Info() DescriptorInfo { return d.info }

// Info returns a copy of the query request identity.
func (d Query[Q, R]) Info() DescriptorInfo { return d.info }

func newDescriptor[T any](
	kind Kind,
	name string,
	schemaVersion int,
	schema string,
	codec Codec[T],
) (descriptor[T], error) {
	if !kind.valid() || len(name) == 0 || len(name) > maxDescriptorNameLength ||
		!descriptorNamePattern.MatchString(name) || schemaVersion <= 0 || codecIsNil(codec) {
		return descriptor[T]{}, fmt.Errorf("%w: %s v%d", ErrInvalidDescriptor, name, schemaVersion)
	}
	if codec.ContentType() == "" || !codec.Encoding().valid() {
		return descriptor[T]{}, fmt.Errorf("%w: codec for %s", ErrInvalidDescriptor, name)
	}
	return descriptor[T]{
		info: DescriptorInfo{
			Kind:          kind,
			Name:          name,
			SchemaVersion: schemaVersion,
			ContentType:   codec.ContentType(),
			DataEncoding:  codec.Encoding(),
			Schema:        schema,
		},
		codec: codec,
	}, nil
}

func codecIsNil[T any](codec Codec[T]) bool { return nilInterface(codec) }

type descriptorKey struct {
	kind          Kind
	name          string
	schemaVersion int
}

func keyFor(info DescriptorInfo) descriptorKey {
	return descriptorKey{kind: info.Kind, name: info.Name, schemaVersion: info.SchemaVersion}
}
