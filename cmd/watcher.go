package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/websockets/blockchain"
	"coinhub/internal/infrastructure/configs"
	"context"

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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			app := internal.NewApplication(ctx, configs)

			if err := blockchain.StartOnchainListener(ctx, &app); err != nil {
				zap.S().Fatalw("failed to start onchain listener", "error", err)
			}
		},
	}
	return cmd
}
