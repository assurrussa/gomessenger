package kafka

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestControlHeadersRoundTrip(t *testing.T) {
	due := time.Date(2026, time.August, 25, 12, 30, 0, 123, time.UTC)
	want := controlMetadata{
		source:  sourcePosition{topic: testSourceTopic, partition: 3, offset: 42},
		attempt: 2, notBefore: due, attemptGeneration: "gm-replay-one",
	}
	got, err := decodeControlHeaders(controlHeaders(want))
	if err != nil {
		t.Fatalf("decodeControlHeaders: %v", err)
	}
	if got != want {
		t.Fatalf("control metadata = %#v, want %#v", got, want)
	}
}

func TestParseControlRejectsReservedSourceHeader(t *testing.T) {
	record := &kgo.Record{
		Topic: testSourceTopic, Partition: 0, Offset: 1,
		Headers: []kgo.RecordHeader{{Key: headerAttempt, Value: []byte("1")}},
	}
	if _, err := parseControl(record, record.Topic, "replay", nil); err == nil {
		t.Fatal("reserved source header was accepted")
	}
}

func TestRetryTierUsesNearestBucketAndOverflow(t *testing.T) {
	tiers := []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}
	tests := []struct {
		delay time.Duration
		want  int
	}{
		{500 * time.Millisecond, 0},
		{time.Second, 0},
		{time.Second + 1, 1},
		{45 * time.Second, 2},
		{time.Hour, 3},
	}
	for _, test := range tests {
		if got := retryTier(tiers, test.delay); got != test.want {
			t.Errorf("retryTier(%s) = %d, want %d", test.delay, got, test.want)
		}
	}
}

func TestRetryDelayNeverReturnsZero(t *testing.T) {
	delay := retryDelayWithReader(bytes.NewReader(make([]byte, 8)), 2*time.Nanosecond, 2*time.Nanosecond, 1)
	if delay != time.Nanosecond {
		t.Fatalf("retry delay = %s, want 1ns", delay)
	}
}

func TestRetryDueRejectsWindowAtOrBeyondExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		notBefore time.Time
		expiresAt time.Time
		wantDue   time.Time
		wantErr   bool
	}{
		{
			name: "retry before expiry", notBefore: now.Add(time.Minute),
			expiresAt: now.Add(2 * time.Minute), wantDue: now.Add(time.Minute),
		},
		{name: "retry at expiry", notBefore: now.Add(time.Minute), expiresAt: now.Add(time.Minute), wantErr: true},
		{name: "retry after expiry", notBefore: now.Add(2 * time.Minute), expiresAt: now.Add(time.Minute), wantErr: true},
		{name: "already expired", expiresAt: now, wantErr: true},
		{name: "no retry", expiresAt: now.Add(time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			due, err := retryDue(test.notBefore, test.expiresAt, now)
			if errors.Is(err, ErrMessageExpired) != test.wantErr || !due.Equal(test.wantDue) {
				t.Fatalf("retryDue = %s, %v, want %s, expired=%t", due, err, test.wantDue, test.wantErr)
			}
		})
	}
}
