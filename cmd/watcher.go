package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/websockets/blockchain"
	"coinhub/internal/infrastructure/configs"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func RunOnchainWatcher(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watcher",
		Short: "Run the onchain watcher",
		Long:  "Run the onchain watcher",
		Run: func(cmd *cobra.Command, args []string) {
			zap.S().Debugw("Starting Watcher command...")

			wg := sync.WaitGroup{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			app := internal.NewApplication(ctx, configs, internal.ApplicationOptions{
				CommandName:       cmd.Name(),
				SkipHDWallet:      true,
				SkipWalletService: true,
				SkipMail:          true,
				SkipMatchEngine:   true,
				SkipMessageBroker: true,
			})

			// TODO : add onchain listener for all available and supporting networks
			if err := blockchain.StartOnchainListener(ctx, app); err != nil {
				zap.S().Fatalw("failed to start onchain listener", "error", err)
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
