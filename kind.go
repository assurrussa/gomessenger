package messenger

// Kind identifies the semantic message category.
type Kind string

const (
	// KindCommand identifies a command with one logical handler.
	KindCommand Kind = "command"
	// KindEvent identifies an event with zero or more subscriptions.
	KindEvent Kind = "event"
)

func (k Kind) valid() bool {
	return k == KindCommand || k == KindEvent
}
