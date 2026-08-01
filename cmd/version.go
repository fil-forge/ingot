package cmd

import (
	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/internal/build"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the ingot version and build metadata",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Printf("ingot %s\n", build.Version)
			cmd.Printf("  commit:   %s\n", build.Commit)
			cmd.Printf("  date:     %s\n", build.Date)
			cmd.Printf("  built by: %s\n", build.BuiltBy)
			return nil
		},
	}
}
