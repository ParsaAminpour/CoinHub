package cmd

import (
	"coinhub/internal/infrastructure/configs"

	"github.com/spf13/cobra"
)

func RunWorker(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run the worker",
		Long:  "Run the worker",
		Run: func(cmd *cobra.Command, args []string) {
			RunWorker(configs)
		},
	}
	return cmd
}
