package messenger_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

func TestFailureDispositionsPreserveCauses(t *testing.T) {
	cause := errors.New("failure")
	permanent := messenger.Permanent(cause)
	if !errors.Is(permanent, cause) || !messenger.IsPermanent(fmt.Errorf("wrapped: %w", permanent)) ||
		messenger.Permanent(nil) != nil {
		t.Fatalf("permanent = %v", permanent)
	}
	retry := messenger.RetryAfter(cause, 125*time.Millisecond)
	delay, ok := messenger.RetryDelay(fmt.Errorf("wrapped: %w", retry))
	if !errors.Is(retry, cause) || !ok || delay != 125*time.Millisecond {
		t.Fatalf("retry = %v, %s, %v", retry, delay, ok)
	}
	if messenger.RetryAfter(nil, time.Second) != nil {
		t.Fatal("nil retry cause did not remain nil")
	}
	if err := messenger.RetryAfter(cause, 0); !errors.Is(err, messenger.ErrInvalidMessage) || !errors.Is(err, cause) {
		t.Fatalf("invalid retry = %v", err)
	}
	if _, ok := messenger.RetryDelay(cause); ok {
		t.Fatal("ordinary error has retry delay")
	}
}

func TestHandlePayloadAdapter(t *testing.T) {
	if messenger.HandlePayload[int](nil) != nil {
		t.Fatal("nil payload handler did not remain nil")
	}
	var handled int
	handler := messenger.HandlePayload(func(_ context.Context, value int) error {
		handled = value
		return nil
	})
	if err := handler(t.Context(), messenger.Message[int]{Payload: 42}); err != nil || handled != 42 {
		t.Fatalf("handle = %d, %v", handled, err)
	}
}
