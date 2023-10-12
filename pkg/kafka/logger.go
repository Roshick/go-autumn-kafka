package kafka

import (
	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Logger struct{}

func (l Logger) Level() kgo.LogLevel {
	return kgo.LogLevelInfo // set to Debug to see all output
}

func (l Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	switch level {
	case kgo.LogLevelError:
		aulogging.Logger.NoCtx().Error().Print("kgo error: " + msg)
		return
	case kgo.LogLevelWarn:
		aulogging.Logger.NoCtx().Warn().Print("kgo warning: " + msg)
		return
	case kgo.LogLevelInfo:
		aulogging.Logger.NoCtx().Info().Print("kgo info: " + msg)
		return
	case kgo.LogLevelDebug:
		aulogging.Logger.NoCtx().Debug().Print("kgo debug: " + msg)
		return
	default:
		return
	}
}
