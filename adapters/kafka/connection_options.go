package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

// ConnectionOption configures a host-owned Kafka connection concern without
// exposing adapter-owned producer, transaction, consumer, or group policy.
type ConnectionOption struct {
	option kgo.Opt
}

// DialTLSConfig enables TLS with a defensive clone of config.
func DialTLSConfig(config *tls.Config) ConnectionOption {
	if config == nil {
		return ConnectionOption{}
	}
	return ConnectionOption{option: kgo.DialTLSConfig(config.Clone())}
}

// Dialer configures the function used to open broker connections.
func Dialer(dial func(context.Context, string, string) (net.Conn, error)) ConnectionOption {
	if dial == nil {
		return ConnectionOption{}
	}
	return ConnectionOption{option: kgo.Dialer(dial)}
}

// DialTimeout configures the broker connection timeout.
func DialTimeout(timeout time.Duration) ConnectionOption {
	if timeout <= 0 {
		return ConnectionOption{}
	}
	return ConnectionOption{option: kgo.DialTimeout(timeout)}
}

// SASL configures broker authentication mechanisms in preference order.
func SASL(mechanisms ...sasl.Mechanism) ConnectionOption {
	if len(mechanisms) == 0 {
		return ConnectionOption{}
	}
	mechanisms = slices.Clone(mechanisms)
	for _, mechanism := range mechanisms {
		if nilValue(mechanism) {
			return ConnectionOption{}
		}
	}
	return ConnectionOption{option: kgo.SASL(mechanisms...)}
}

// Rack configures the physical rack used for follower fetching.
func Rack(rack string) ConnectionOption {
	if rack == "" {
		return ConnectionOption{}
	}
	return ConnectionOption{option: kgo.Rack(rack)}
}

// WithHooks configures franz-go client hooks that cannot receive mutable
// producer or consumer records or expose the live client.
func WithHooks(hooks ...kgo.Hook) ConnectionOption {
	if len(hooks) == 0 {
		return ConnectionOption{}
	}
	hooks = slices.Clone(hooks)
	for _, hook := range hooks {
		if nilValue(hook) || unsafeConnectionHook(hook) {
			return ConnectionOption{}
		}
	}
	return ConnectionOption{option: kgo.WithHooks(hooks...)}
}

func unsafeConnectionHook(hook kgo.Hook) bool {
	switch hook.(type) {
	case kgo.HookNewClient,
		kgo.HookProduceRecordBuffered,
		kgo.HookProduceRecordPartitioned,
		kgo.HookProduceRecordUnbuffered,
		kgo.HookFetchRecordBuffered,
		kgo.HookFetchRecordUnbuffered:
		return true
	default:
		return false
	}
}

// WithClientLogger configures franz-go's internal client logger.
func WithClientLogger(logger kgo.Logger) ConnectionOption {
	if nilValue(logger) {
		return ConnectionOption{}
	}
	return ConnectionOption{option: kgo.WithLogger(logger)}
}
