package main

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/logger"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
)

func main() {
	// initializing configs, DB, redis, Gin server, repositories, context, Wg
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := logger.InitLogger(&configs.C); err != nil {
		zap.S().Error("error in initializing the zap logger")
	}

	wg := &sync.WaitGroup{}
	ctx = context.WithValue(ctx, "wg", wg)

	app := internal.NewApplication(ctx)

	if err := http.SetupRouter(&app); err != nil {
		zap.S().Error("error occurred in setting up the http router\n%s", err.Error())
	}

	closeSignal := make(chan os.Signal, 1)
	signal.Notify(closeSignal, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer signal.Stop(closeSignal)

	select {
	case sig := <-closeSignal:
		zap.S().Infow("terminating by OS signal", "signal", sig.String())
		cancel()
	case <-ctx.Done():
		zap.S().Info("terminating by context cancellation")
	}

	wg.Wait()
	zap.S().Debug("shutdown complete")
}
