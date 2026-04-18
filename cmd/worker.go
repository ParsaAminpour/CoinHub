package cmd

import (
	"coinhub/internal"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/worker"
	"context"

	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func RunWorker(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run the worker",
		Long:  "Run the worker",
		Run: func(cmd *cobra.Command, args []string) {
			zap.S().Debugw("Starting worker command")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// register infrastructure
			app := internal.NewApplication(ctx, configs, internal.ApplicationOptions{
				CommandName:       cmd.Name(),
				SkipHDWallet:      true,
				SkipWalletService: true,
				SkipMatchEngine:   true,
				SkipMessageBroker: true,
			})

			server := worker.NewWorker(ctx, *configs)
			mux := asynq.NewServeMux()
			worker.RegisterWorkerHandler(mux, app)

			if err := server.Run(mux); err != nil {
				zap.S().With(zap.Error(err)).Fatal("could not run server")
				panic("could not run server: " + err.Error())
			}
			zap.S().Info("worker is running")
		},
	}
	return cmd
}
