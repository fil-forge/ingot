// Package cmd is the ingot daemon's cobra/viper CLI:
//   - `serve` runs the S3 gateway;
//   - `whoami` and `version` report the agent and the build;
//   - `blake3` hashes and verifies object data against ingot's BLAKE3 CIDs.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/fil-forge/ingot/config"
	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/internal/build"
)

// cfgFile is bound to the persistent --config flag.
var cfgFile string

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ingot",
		Short:         "ingot — an embeddable S3 gateway over the Forge network",
		Version:       build.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVarP(&cfgFile, "config", "c", "",
		"config file (default: ./config.yaml or /etc/ingot/config.yaml)")
	root.AddCommand(
		newServeCmd(),
		newWhoamiCmd(),
		newVersionCmd(),
		newBlake3Cmd(),
	)
	return root
}

func newWhoamiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Print the agent DID (and key DID) and configured endpoints",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return err
			}
			id, err := loadAgentIdentity(cfg.Identity)
			if err != nil {
				return err
			}
			// A did:web agent prints as "did:web:… (key: z6Mk…)".
			cmd.Printf("agent DID:      %s\n", id)
			cmd.Printf("upload service: %s (%s)\n", cfg.UploadServiceURL, cfg.UploadServiceDID)
			cmd.Printf("auth service:   %s (%s)\n", cfg.AuthServiceURL, cfg.AuthServiceDID)
			return nil
		},
	}
}

// Execute runs the root command. A verification mismatch exits 1; any other
// failure exits 2, so a script can tell bad data from a bad invocation.
func Execute() {
	if err := newRootCmd().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "ingot:", err)
		var mm *mismatchError
		if errors.As(err, &mm) {
			os.Exit(1)
		}
		os.Exit(2)
	}
}
