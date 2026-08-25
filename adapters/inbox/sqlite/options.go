package sqlite

import (
	"fmt"
	"strings"

	"github.com/assurrussa/gomessenger/adapters/inbox/internal/sqlnamespace"
)

// Option configures the SQLite Inbox table namespace.
type Option interface {
	apply(configuration *options)
}

type optionFunc func(*options)

func (option optionFunc) apply(configuration *options) { option(configuration) }

type options struct {
	tablePrefix string
}

// WithTablePrefix replaces the default "gomessenger_" table prefix.
// For example, "site_" produces "site_inbox" and its companion tables.
func WithTablePrefix(prefix string) Option {
	return optionFunc(func(configuration *options) { configuration.tablePrefix = prefix })
}

type namespace struct {
	inbox              string
	attempts           string
	attemptGenerations string
	completedAtIndex   string
}

func resolveNamespace(configOptions ...Option) (namespace, error) {
	configuration := options{tablePrefix: sqlnamespace.DefaultTablePrefix}
	for index, option := range configOptions {
		if option == nil {
			return namespace{}, fmt.Errorf("inbox/sqlite: nil option at index %d", index)
		}
		option.apply(&configuration)
	}
	names, err := sqlnamespace.Resolve(configuration.tablePrefix)
	if err != nil {
		return namespace{}, fmt.Errorf("inbox/sqlite: options: %w", err)
	}
	return namespace{
		inbox:              quoteIdentifier(names.Inbox),
		attempts:           quoteIdentifier(names.Attempts),
		attemptGenerations: quoteIdentifier(names.AttemptGenerations),
		completedAtIndex:   quoteIdentifier(names.CompletedAtIndex),
	}, nil
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func (n namespace) render(statement string) string {
	return strings.NewReplacer(
		"{{inbox}}", n.inbox,
		"{{attempts}}", n.attempts,
		"{{attempt_generations}}", n.attemptGenerations,
		"{{completed_at_index}}", n.completedAtIndex,
	).Replace(statement)
}
