package commands

import (
	"github.com/cirruslabs/omni-cache/internal/version"
	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "omni-cache",
		Short:         "Omni Cache Sidecar",
		Version:       version.FullVersion,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.AddCommand(newSidecarCmd())
	cmd.AddCommand(newDevCmd())

	return cmd
}
