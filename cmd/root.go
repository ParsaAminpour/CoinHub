package cmd

import (
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/logger"
	"coinhub/internal/infrastructure/swagger"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   `coinhub-service`,
	Short: `coinhub service application`,
	Long:  `coinhub service application`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := cmd.Help(); err != nil {
			return
		}
	},
}

func init() {
	if err := configs.LoadConfig(".env"); err != nil {
		zap.S().Fatalw("unable to load configuration")
	}
	if err := logger.InitLogger(&configs.C); err != nil {
		zap.S().Fatalw("error in initializing the zap logger")
	}
	if err := swagger.InitSwagger(&configs.C); err != nil {
		zap.S().Fatalf("Error while config swagger setting : %v", err)
	}

	defer func() {
		_ = logger.SyncLogger()
		sentry.Flush(2 * time.Second)
	}()
	zap.S().Info("application configured successfully")

	rootCmd.AddCommand(RunApi(&configs.C))
	rootCmd.AddCommand(RunMigrate(&configs.C))
	rootCmd.AddCommand(RunMatching(&configs.C))
	rootCmd.AddCommand(RunWorker(&configs.C))
}

func Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	ctx = context.WithValue(ctx, "wg", wg)

	closeSignal := make(chan os.Signal, 1)
	signal.Notify(closeSignal, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer signal.Stop(closeSignal)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}

	select {
	case sig := <-closeSignal:
		zap.S().Infow("terminating by OS signal", "signal", sig.String())
		cancel()
	case <-ctx.Done():
		zap.S().Info("terminating by context cancellation")
	}

	if configs.C.App.GracePeriod > 0 {
		zap.S().Infow("grace period before shutdown", "ms", configs.C.App.GracePeriod)
		time.Sleep(time.Duration(configs.C.App.GracePeriod) * time.Millisecond)
	}

	wg.Wait()
	zap.S().Debug("shutdown complete")
}
