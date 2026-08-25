//nolint:testpackage // Rendering tests verify intentionally private SQL templates.
package sqlite

import (
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

func TestResolveNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		options         []Option
		wantInbox       string
		wantAttempts    string
		wantGenerations string
		wantIndex       string
		wantErr         bool
	}{
		{
			name:            "defaults",
			wantInbox:       `"gomessenger_inbox"`,
			wantAttempts:    `"gomessenger_inbox_attempts"`,
			wantGenerations: `"gomessenger_inbox_attempt_generations"`,
			wantIndex:       `"gomessenger_inbox_completed_at_idx"`,
		},
		{
			name:            "custom prefix",
			options:         []Option{WithTablePrefix("site_")},
			wantInbox:       `"site_inbox"`,
			wantAttempts:    `"site_inbox_attempts"`,
			wantGenerations: `"site_inbox_attempt_generations"`,
			wantIndex:       `"site_inbox_completed_at_idx"`,
		},
		{
			name:            "empty prefix",
			options:         []Option{WithTablePrefix("")},
			wantInbox:       `"inbox"`,
			wantAttempts:    `"inbox_attempts"`,
			wantGenerations: `"inbox_attempt_generations"`,
			wantIndex:       `"inbox_completed_at_idx"`,
		},
		{name: "invalid prefix", options: []Option{WithTablePrefix("site-")}, wantErr: true},
		{name: "nil option", options: []Option{nil}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			names, err := resolveNamespace(test.options...)
			if (err != nil) != test.wantErr {
				t.Fatalf("resolveNamespace() error = %v, wantErr=%t", err, test.wantErr)
			}
			if err != nil {
				return
			}
			if names.inbox != test.wantInbox || names.attempts != test.wantAttempts ||
				names.attemptGenerations != test.wantGenerations || names.completedAtIndex != test.wantIndex {
				t.Fatalf("resolveNamespace() = %#v", names)
			}
		})
	}
}

func TestCustomNamespaceRendersAllStatementsAndMigrations(t *testing.T) {
	t.Parallel()

	names, err := resolveNamespace(WithTablePrefix("site_"))
	if err != nil {
		t.Fatalf("resolve namespace: %v", err)
	}
	statements := reflect.ValueOf(newStatements(names))
	for index := range statements.NumField() {
		statement := statements.Field(index).String()
		if strings.Contains(statement, "{{") || strings.Contains(statement, "gomessenger_inbox") {
			t.Fatalf("statement %d was not rendered for custom namespace: %s", index, statement)
		}
		if !strings.Contains(statement, `"site_inbox`) {
			t.Fatalf("statement %d does not use the custom namespace: %s", index, statement)
		}
	}

	paths, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	var rendered strings.Builder
	for _, path := range paths {
		data, readErr := migrations.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", path, readErr)
		}
		rendered.WriteString(names.render(string(data)))
	}
	sql := rendered.String()
	for _, expected := range []string{
		`"site_inbox"`,
		`"site_inbox_attempts"`,
		`"site_inbox_attempt_generations"`,
		`"site_inbox_completed_at_idx"`,
	} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("rendered migrations do not contain %s:\n%s", expected, sql)
		}
	}
	if strings.Contains(sql, "{{") || strings.Contains(sql, "gomessenger_inbox") {
		t.Fatalf("custom migrations contain unresolved or default names:\n%s", sql)
	}
}
