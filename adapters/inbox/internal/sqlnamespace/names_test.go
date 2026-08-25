package sqlnamespace_test

import (
	"strings"
	"testing"

	"github.com/assurrussa/gomessenger/adapters/inbox/internal/sqlnamespace"
)

func TestResolveTablePrefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		prefix    string
		wantInbox string
		wantErr   bool
	}{
		{name: "default", prefix: sqlnamespace.DefaultTablePrefix, wantInbox: "gomessenger_inbox"},
		{name: "replacement", prefix: "site_", wantInbox: "site_inbox"},
		{name: "empty", prefix: "", wantInbox: "inbox"},
		{
			name: "maximum length", prefix: strings.Repeat("a", sqlnamespace.MaxTablePrefixBytes),
			wantInbox: strings.Repeat("a", sqlnamespace.MaxTablePrefixBytes) + "inbox",
		},
		{name: "uppercase", prefix: "Site_", wantErr: true},
		{name: "digit first", prefix: "1site_", wantErr: true},
		{name: "dot", prefix: "site.data_", wantErr: true},
		{name: "too long", prefix: strings.Repeat("a", sqlnamespace.MaxTablePrefixBytes+1), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			names, err := sqlnamespace.Resolve(test.prefix)
			if (err != nil) != test.wantErr {
				t.Fatalf("Resolve(%q) error = %v, wantErr=%t", test.prefix, err, test.wantErr)
			}
			if err == nil && names.Inbox != test.wantInbox {
				t.Fatalf("Resolve(%q).Inbox = %q, want %q", test.prefix, names.Inbox, test.wantInbox)
			}
		})
	}
}

func TestValidateSchema(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{name: "default search path", schema: ""},
		{name: "explicit", schema: "messaging"},
		{name: "underscore", schema: "site_messages_2"},
		{name: "maximum length", schema: strings.Repeat("s", sqlnamespace.MaxSchemaBytes)},
		{name: "quoted injection", schema: `site".public`, wantErr: true},
		{name: "too long", schema: strings.Repeat("s", sqlnamespace.MaxSchemaBytes+1), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := sqlnamespace.ValidateSchema(test.schema); (err != nil) != test.wantErr {
				t.Fatalf("ValidateSchema(%q) error = %v, wantErr=%t", test.schema, err, test.wantErr)
			}
		})
	}
}
