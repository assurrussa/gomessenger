// Package sqlnamespace resolves portable SQL Inbox table namespaces.
package sqlnamespace

import "fmt"

const (
	// DefaultTablePrefix preserves the original public Inbox table names.
	DefaultTablePrefix = "gomessenger_"
	// MaxTablePrefixBytes keeps the longest derived PostgreSQL identifier within
	// the default 63-byte identifier limit.
	MaxTablePrefixBytes = 38
	// MaxSchemaBytes matches PostgreSQL's default identifier limit.
	MaxSchemaBytes = 63
)

// Names contains the unquoted identifiers derived from one table prefix.
type Names struct {
	Terminal             string
	TerminalHandoffIndex string
	Inbox                string
	Attempts             string
	AttemptGenerations   string
	CompletedAtIndex     string
}

// Resolve validates prefix and derives every Inbox relation identifier.
func Resolve(prefix string) (Names, error) {
	if err := validateIdentifier("table prefix", prefix, MaxTablePrefixBytes, true); err != nil {
		return Names{}, err
	}
	return Names{
		Terminal:             prefix + "inbox_terminal",
		TerminalHandoffIndex: prefix + "inbox_terminal_gc_idx",
		Inbox:                prefix + "inbox",
		Attempts:             prefix + "inbox_attempts",
		AttemptGenerations:   prefix + "inbox_attempt_generations",
		CompletedAtIndex:     prefix + "inbox_completed_at_idx",
	}, nil
}

// ValidateSchema validates one optional PostgreSQL schema identifier.
func ValidateSchema(schema string) error {
	return validateIdentifier("schema", schema, MaxSchemaBytes, true)
}

func validateIdentifier(role, value string, maximum int, allowEmpty bool) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("inbox: invalid %s: empty", role)
	}
	if len(value) > maximum {
		return fmt.Errorf("inbox: invalid %s: exceeds %d bytes", role, maximum)
	}
	for index := range len(value) {
		character := value[index]
		if index == 0 {
			if character != '_' && (character < 'a' || character > 'z') {
				return fmt.Errorf("inbox: invalid %s %q", role, value)
			}
			continue
		}
		if character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return fmt.Errorf("inbox: invalid %s %q", role, value)
		}
	}
	return nil
}
