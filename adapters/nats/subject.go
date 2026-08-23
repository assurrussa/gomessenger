package nats

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	messenger "github.com/assurrussa/gomessenger"
)

const maxJetStreamResourceNameBytes = 255

// Subject returns the stable versioned physical subject for a descriptor.
func Subject(namespace string, descriptor messenger.DescriptorInfo) (string, error) {
	if err := validateSubjectToken(namespace, true); err != nil {
		return "", fmt.Errorf("%w: namespace: %w", ErrInvalidConfig, err)
	}
	if !kindAdapter(descriptor.Kind).validForAdapter() || descriptor.Name == "" || descriptor.SchemaVersion <= 0 {
		return "", fmt.Errorf("%w: invalid descriptor", ErrInvalidConfig)
	}
	for _, token := range strings.Split(descriptor.Name, ".") {
		if err := validateSubjectToken(token, false); err != nil {
			return "", fmt.Errorf("%w: descriptor name: %w", ErrInvalidConfig, err)
		}
	}
	return fmt.Sprintf("%s.%s.%s.v%d", namespace, descriptor.Kind, descriptor.Name, descriptor.SchemaVersion), nil
}

func validateSubjectToken(value string, allowDots bool) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "*>") {
		return fmt.Errorf("invalid subject token %q", value)
	}
	if allowDots {
		for _, token := range strings.Split(value, ".") {
			if err := validateSubjectToken(token, false); err != nil {
				return err
			}
		}
		return nil
	}
	if !allowDots && strings.ContainsRune(value, '.') {
		return fmt.Errorf("unexpected dot in token %q", value)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("invalid whitespace in %q", value)
		}
	}
	return nil
}

func validateSubjectPattern(value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("invalid subject pattern %q", value)
	}
	tokens := strings.Split(value, ".")
	for index, token := range tokens {
		switch token {
		case "*":
			continue
		case ">":
			if index == len(tokens)-1 {
				continue
			}
			return fmt.Errorf("terminal wildcard must be the final token in %q", value)
		default:
			if err := validateSubjectToken(token, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateJetStreamResourceName(value string) error {
	if value == "" {
		return errors.New("empty JetStream resource name")
	}
	if len(value) > maxJetStreamResourceNameBytes {
		return fmt.Errorf("JetStream resource name exceeds %d bytes", maxJetStreamResourceNameBytes)
	}
	if strings.ContainsAny(value, " \t\r\n\f.*>\\/") {
		return fmt.Errorf("invalid JetStream resource name %q", value)
	}
	return nil
}

type kindAdapter messenger.Kind

func (kind kindAdapter) validForAdapter() bool {
	return messenger.Kind(kind) == messenger.KindCommand || messenger.Kind(kind) == messenger.KindEvent
}
