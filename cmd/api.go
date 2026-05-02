package cmd

import (
	"coinhub/internal"
	apphttp "coinhub/internal/adapter/handler/http"
	"coinhub/internal/infrastructure/configs"
	"context"
	"errors"
	nethttp "net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg := &sync.WaitGroup{}
			ctx = context.WithValue(ctx, "wg", wg)

			app := internal.NewApplication(ctx, configs, internal.ApplicationOptions{
				CommandName: cmd.Name(),
			})

			srv, err := apphttp.SetupRouter(app)
			if err != nil {
				zap.S().Fatalw("error setting up router", "error", err)
			}

			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
					zap.S().Fatalw("HTTP server error", "error", err)
				}
			}()

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

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				zap.S().Errorw("HTTP server forced to shut down", "error", err)
			}

			wg.Wait()
			app.Shutdown()
			zap.S().Debug("shutdown complete")
		},
	}
	return cmd
}
