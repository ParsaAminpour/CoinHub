package cmd

import (
	"coinhub/internal/infrastructure/configs"

	"github.com/spf13/cobra"
)

func RunMatching(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "matching",
		Short: "Run the matching",
		Long:  "Run the matching",
		Run: func(cmd *cobra.Command, args []string) {
			RunMatching(configs)
		},
	}
	return cmd
}
