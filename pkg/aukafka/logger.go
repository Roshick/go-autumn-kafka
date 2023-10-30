package aukafka

import (
	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Logger struct {
	Key string
}

func (l Logger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug // set to Debug to see all output
}

func (l Logger) Log(level kgo.LogLevel, msg string, _ ...any) {
	switch level {
	case kgo.LogLevelError:
		aulogging.Logger.NoCtx().Error().Printf("kgo %s error: %s", l.Key, msg)
		return
	case kgo.LogLevelWarn:
		aulogging.Logger.NoCtx().Warn().Printf("kgo %s warning: %s", l.Key, msg)
		return
	case kgo.LogLevelInfo:
		aulogging.Logger.NoCtx().Info().Printf("kgo %s info: %s", l.Key, msg)
		return
	case kgo.LogLevelDebug:
		aulogging.Logger.NoCtx().Debug().Printf("kgo %s debug: %s", l.Key, msg)
		return
	default:
		return
	}
}
