package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http"
	"coinhub/internal/infrastructure/configs"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func RunApi(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Run the API server",
		Long:  "Run the API server",
		Run: func(cmd *cobra.Command, args []string) {
			zap.S().Info("running API server")
			// initializing configs, DB, redis, Gin server, repositories, context, Wg
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg := &sync.WaitGroup{}
			ctx = context.WithValue(ctx, "wg", wg)

			app := internal.NewApplication(ctx, configs)

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
		},
	}
	return cmd
}
