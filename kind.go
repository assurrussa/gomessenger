package messenger

// Kind identifies the semantic message category.
type Kind string

const (
	// KindCommand identifies a command with one logical handler.
	KindCommand Kind = "command"
	// KindEvent identifies an event with zero or more subscriptions.
	KindEvent Kind = "event"
	// KindQuery identifies a local request/reply query with one handler.
	KindQuery Kind = "query"
)

func (k Kind) valid() bool {
	return k == KindCommand || k == KindEvent || k == KindQuery
}

func (k Kind) validWire() bool { return k == KindCommand || k == KindEvent }
