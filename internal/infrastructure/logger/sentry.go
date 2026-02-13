package logger

import (
	"coinhub/internal/infrastructure/configs"
	"log"
	"strings"

	"github.com/TheZeroSlave/zapsentry"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap/zapcore"
)

func InitializeSentry(cfg *configs.Configuration) error {
	if cfg.App.Env != "PRODUCTION" {
		return nil
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              cfg.Observe.SentryDSN,
		Debug:            cfg.App.Debug,
		ServerName:       cfg.App.Name,
		Environment:      cfg.App.Env,
		Release:          cfg.App.Version,
		AttachStacktrace: true,
		EnableTracing:    true,
		TracesSampleRate: 1.0,
		SampleRate:       1.0,
		TracesSampler: sentry.TracesSampler(func(ctx sentry.SamplingContext) float64 {
			if ctx.Span.Name == "GET /health" {
				return 0.0
			}
			return 1.0
		}),
		BeforeSend: func(event *sentry.Event, hint *sentry.EventHint) *sentry.Event {
			// Optional: customize what gets sent to Sentry
			return event
		},
		EnableLogs: true,
	})

	if err != nil {
		log.Printf("Sentry initialization failed: %v\n", err)
		return err
	}

	return nil
}

func newSentryCore(cfg *configs.Configuration, sentryClient *sentry.Client, zapLevel zapcore.Level) (zapcore.Core, error) {
	return zapsentry.NewCore(
		zapsentry.Configuration{
			Level:             zapLevel,
			EnableBreadcrumbs: true,
			BreadcrumbLevel:   zapLevel,
			Tags: map[string]string{
				"component": "aih-service-api",
				"app_name":  cfg.App.Name,
				"app_env":   cfg.App.Env,
				"app_ver":   cfg.App.Version,
				"instance":  cfg.App.InstType,
			},
		},
		zapsentry.NewSentryClientFromClient(sentryClient),
	)
}

func logLevelFromConfig(configLevel string) zapcore.Level {
	level := zapcore.DebugLevel
	switch strings.ToLower(configLevel) {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn", "warning":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	case "d-panic":
		level = zapcore.DPanicLevel
	case "panic":
		level = zapcore.PanicLevel
	case "fatal":
		level = zapcore.FatalLevel
	}
	return level
}
