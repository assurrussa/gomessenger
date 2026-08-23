package messenger_test

import (
	"errors"
	"strings"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

func TestManifestValidateRejectsDuplicateOrInvalidTopology(t *testing.T) {
	valid := messenger.Manifest{
		SpecVersion: messenger.ManifestSpecVersion,
		Source:      testSource,
		Descriptors: []messenger.ManifestDescriptor{{
			DescriptorInfo: messenger.DescriptorInfo{
				Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
				ContentType: testContentType, DataEncoding: messenger.DataJSON,
			},
			Route: "nats.events", HandlerIDs: []string{testHandlerID},
		}},
		Services: []string{testServiceID},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}

	duplicate := valid
	duplicate.Descriptors = append(append([]messenger.ManifestDescriptor(nil), valid.Descriptors...), valid.Descriptors[0])
	if err := duplicate.Validate(); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("duplicate error = %v", err)
	}

	invalidService := valid
	invalidService.Services = []string{"not valid"}
	if err := invalidService.Validate(); !errors.Is(err, messenger.ErrServiceConflict) {
		t.Fatalf("service error = %v", err)
	}
}

func TestManifestValidateAllIdentityBoundaries(t *testing.T) {
	base := messenger.Manifest{
		SpecVersion: messenger.ManifestSpecVersion,
		Source:      testSource,
		Descriptors: []messenger.ManifestDescriptor{{
			DescriptorInfo: messenger.DescriptorInfo{
				Kind: messenger.KindEvent, Name: testEventName, SchemaVersion: 1,
				ContentType: testContentType, DataEncoding: messenger.DataJSON,
			},
			Route: "nats.events", HandlerIDs: []string{testHandlerID},
		}},
		Services: []string{testServiceID},
	}
	tests := []struct {
		name string
		edit func(*messenger.Manifest)
		want error
	}{
		{
			name: "spec",
			edit: func(value *messenger.Manifest) { value.SpecVersion = "2.0" },
			want: messenger.ErrInvalidMessage,
		},
		{
			name: "source",
			edit: func(value *messenger.Manifest) { value.Source = "" },
			want: messenger.ErrInvalidMessage,
		},
		{
			name: "descriptor",
			edit: func(value *messenger.Manifest) { value.Descriptors[0].Name = "Bad" },
			want: messenger.ErrInvalidDescriptor,
		},
		{
			name: "descriptor name too long",
			edit: func(value *messenger.Manifest) { value.Descriptors[0].Name = strings.Repeat("a", 201) },
			want: messenger.ErrInvalidDescriptor,
		},
		{
			name: "data encoding",
			edit: func(value *messenger.Manifest) { value.Descriptors[0].DataEncoding = 0 },
			want: messenger.ErrInvalidDescriptor,
		},
		{
			name: "route",
			edit: func(value *messenger.Manifest) { value.Descriptors[0].Route = "bad route" },
			want: messenger.ErrRouteConflict,
		},
		{
			name: "handler",
			edit: func(value *messenger.Manifest) { value.Descriptors[0].HandlerIDs = []string{"bad handler"} },
			want: messenger.ErrHandlerConflict,
		},
		{
			name: "handler duplicate",
			edit: func(value *messenger.Manifest) {
				value.Descriptors[0].HandlerIDs = []string{testHandlerID, testHandlerID}
			},
			want: messenger.ErrHandlerConflict,
		},
		{
			name: "command handler cardinality",
			edit: func(value *messenger.Manifest) {
				value.Descriptors[0].Kind = messenger.KindCommand
				value.Descriptors[0].HandlerIDs = []string{testHandlerID, "handler.secondary"}
			},
			want: messenger.ErrHandlerConflict,
		},
		{
			name: "service duplicate",
			edit: func(value *messenger.Manifest) {
				value.Services = []string{testServiceID, testServiceID}
			},
			want: messenger.ErrServiceConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Descriptors = append([]messenger.ManifestDescriptor(nil), base.Descriptors...)
			value.Descriptors[0].HandlerIDs = append([]string(nil), base.Descriptors[0].HandlerIDs...)
			value.Services = append([]string(nil), base.Services...)
			test.edit(&value)
			if err := value.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
