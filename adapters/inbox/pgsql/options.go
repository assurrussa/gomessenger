package pgsql

import (
	"fmt"
	"strings"

	"github.com/assurrussa/gomessenger/adapters/inbox/internal/sqlnamespace"
)

// Option configures the PostgreSQL Inbox table namespace.
type Option interface {
	apply(configuration *options)
}

type optionFunc func(*options)

func (option optionFunc) apply(configuration *options) { option(configuration) }

type options struct {
	tablePrefix string
	schema      string
}

// WithTablePrefix replaces the default "gomessenger_" table prefix.
// For example, "site_" produces "site_inbox" and its companion tables.
func WithTablePrefix(prefix string) Option {
	return optionFunc(func(configuration *options) { configuration.tablePrefix = prefix })
}

// WithSchema fully qualifies every Inbox relation with schema.
// Migrate requires the schema to exist and never creates it.
func WithSchema(schema string) Option {
	return optionFunc(func(configuration *options) { configuration.schema = schema })
}

type namespace struct {
	terminal             string
	terminalHandoffIndex string
	inbox                string
	attempts             string
	attemptGenerations   string
	completedAtIndex     string
}

func resolveNamespace(configOptions ...Option) (namespace, error) {
	configuration := options{tablePrefix: sqlnamespace.DefaultTablePrefix}
	for index, option := range configOptions {
		if option == nil {
			return namespace{}, fmt.Errorf("inbox/pgsql: nil option at index %d", index)
		}
		option.apply(&configuration)
	}
	if err := sqlnamespace.ValidateSchema(configuration.schema); err != nil {
		return namespace{}, fmt.Errorf("inbox/pgsql: options: %w", err)
	}
	names, err := sqlnamespace.Resolve(configuration.tablePrefix)
	if err != nil {
		return namespace{}, fmt.Errorf("inbox/pgsql: options: %w", err)
	}
	return namespace{
		terminal:             qualifyIdentifier(configuration.schema, names.Terminal),
		terminalHandoffIndex: quoteIdentifier(names.TerminalHandoffIndex),
		inbox:                qualifyIdentifier(configuration.schema, names.Inbox),
		attempts:             qualifyIdentifier(configuration.schema, names.Attempts),
		attemptGenerations:   qualifyIdentifier(configuration.schema, names.AttemptGenerations),
		completedAtIndex:     quoteIdentifier(names.CompletedAtIndex),
	}, nil
}

func qualifyIdentifier(schema, identifier string) string {
	quoted := quoteIdentifier(identifier)
	if schema == "" {
		return quoted
	}
	return quoteIdentifier(schema) + "." + quoted
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func (n namespace) render(statement string) string {
	return strings.NewReplacer(
		"{{terminal}}", n.terminal, "{{terminal_handoff_index}}", n.terminalHandoffIndex,
		"{{inbox}}", n.inbox,
		"{{attempts}}", n.attempts,
		"{{attempt_generations}}", n.attemptGenerations,
		"{{completed_at_index}}", n.completedAtIndex,
	).Replace(statement)
}
