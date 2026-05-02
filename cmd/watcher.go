package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/websockets/blockchain"
	"coinhub/internal/infrastructure/configs"
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func RunOnchainWatcher(configs *configs.Configuration) *cobra.Command {
	var fromBlock string
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

			if fromBlock == "" {
				zap.S().Fatalw("fromBlock is not set")
			}
			fromBlockInt, err := strconv.Atoi(fromBlock)
			if err != nil {
				zap.S().Fatalw("invalid fromBlock value", "fromBlock", fromBlock, "error", err)
			}
			if err := blockchain.StartOnchainListener(ctx, fromBlockInt, app); err != nil {
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
			app.Shutdown()
			zap.S().Debug("shutdown complete")
		},
	}
	cmd.Flags().StringVar(&fromBlock, "from-block", "10334076", "the block that we start to listening from")
	return cmd
}
