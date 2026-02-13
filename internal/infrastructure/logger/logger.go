package logger

import (
	"coinhub/internal/infrastructure/configs"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type L interface {
	Logf(format string, args ...interface{})
}

var GlobalLogger L

var zapLogger *zap.Logger

type zapLoggerImpl struct {
	sugar *zap.SugaredLogger
}

func (z *zapLoggerImpl) Logf(format string, args ...interface{}) {
	z.sugar.Infof(format, args...)
}

func InitLogger(cfg *configs.Configuration) error {
	loc, err := time.LoadLocation(cfg.App.TimeZone)
	if err != nil {
		return fmt.Errorf("failed to load timezone: %v", err)
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		MessageKey:     "message",
		CallerKey:      "caller",
		SkipLineEnding: false,
		LineEnding:     "\n\n",
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.In(loc).Format(time.RFC3339))
		},
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	writeSyncer := zapcore.AddSync(os.Stdout)
	core := zapcore.NewCore(encoder, writeSyncer, cfg.Observe.LogLevel)
	zapLogger := zap.New(core, zap.AddCaller())

	// if cfg.App.Env == "PRODUCTION" {
	// 	sentryClient := sentry.CurrentHub().Client()
	// 	if sentryClient == nil {
	// 		// Fallback to console error
	// 		fallback := zap.New(zapcore.NewCore(encoder, writeSyncer, zapcore.ErrorLevel))
	// 		fallback.Error("No Sentry client found")
	// 		return fmt.Errorf("Sentry client not initialized")
	// 	}

	// 	sentryMinLevel := logLevelFromConfig(cfg.Observe.LogLevel.String())
	// 	sentryCore, err := newSentryCore(cfg, sentryClient, sentryMinLevel)
	// 	if err != nil {
	// 		zapLogger.Warn("Failed to init zap Sentry core", zap.Error(err))
	// 	} else {
	// 		zapLogger = zapsentry.AttachCoreToLogger(sentryCore, zapLogger)
	// 		zapLogger.Info("Sentry successfully attached")
	// 	}
	// }

	zap.ReplaceGlobals(zapLogger)

	GlobalLogger = &zapLoggerImpl{
		sugar: zapLogger.Sugar(),
	}

	zap.S().Infow("Logger initialized",
		"environment", cfg.App.Env,
		"debug", cfg.App.Debug,
	)

	return nil
}

func SyncLogger() error {
	if zapLogger != nil {
		return zapLogger.Sync()
	}
	return nil
}
