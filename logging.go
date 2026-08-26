package messenger

import (
	"context"
	"log/slog"
)

// LogLevel is the transport-neutral severity understood by Logger.
type LogLevel uint8

const (
	// LogDebug records diagnostic lifecycle details.
	LogDebug LogLevel = iota
	// LogInfo records ordinary lifecycle transitions.
	LogInfo
	// LogWarn records recoverable infrastructure failures.
	LogWarn
	// LogError records infrastructure failures requiring attention.
	LogError
)

// LogAttr is one structured logging attribute.
type LogAttr struct {
	Key   string
	Value any
}

const (
	logAttrErrorKey     = "error"
	logAttrServiceIDKey = "service_id"
)

// Logger is the minimal structured logging contract used by GoMessenger.
// Implementations must not retain ctx and should return quickly.
type Logger interface {
	Log(ctx context.Context, level LogLevel, message string, attrs ...LogAttr)
}

type noopLogger struct{}

func (noopLogger) Log(context.Context, LogLevel, string, ...LogAttr) {}

type slogAdapter struct{ logger *slog.Logger }

// AdaptSlog adapts a standard slog logger. A nil logger returns a no-op
// adapter, which makes optional host wiring safe.
func AdaptSlog(logger *slog.Logger) Logger {
	if logger == nil {
		return noopLogger{}
	}
	return slogAdapter{logger: logger}
}

func (a slogAdapter) Log(ctx context.Context, level LogLevel, message string, attrs ...LogAttr) {
	values := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}
		values = append(values, slog.Any(attr.Key, attr.Value))
	}
	a.logger.Log(ctx, slogLevel(level), message, values...)
}

func slogLevel(level LogLevel) slog.Level {
	switch level {
	case LogDebug:
		return slog.LevelDebug
	case LogInfo:
		return slog.LevelInfo
	case LogWarn:
		return slog.LevelWarn
	case LogError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func safeLog(ctx context.Context, logger Logger, level LogLevel, message string, attrs ...LogAttr) {
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Log(ctx, level, message, attrs...)
}
