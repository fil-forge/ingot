// Package cmd is the ingot daemon's cobra/viper CLI: `serve` runs the S3
// gateway (standalone or forge mode), `login`/`space` manage Forge
// authorization and the space key.
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// cfgFile is bound to the persistent --config flag.
var cfgFile string

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ingot",
		Short:         "ingot — an embeddable S3 gateway over the Forge network",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVarP(&cfgFile, "config", "c", "",
		"config file (default: ./config.yaml or /etc/ingot/config.yaml)")
	root.AddCommand(
		newServeCmd(),
		newSpaceCmd(),
		newLoginCmd(),
		newWhoamiCmd(),
	)
	return root
}

func newWhoamiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Print the agent DID and configured endpoints",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
			cmd.Printf("mode:           %s\n", cfg.Mode)
			if cfg.Identity.KeyFile != "" {
				if id, err := loadAgentIdentity(cfg.Identity.KeyFile); err == nil {
					cmd.Printf("agent DID:      %s\n", id.Signer.DID())
				} else {
					cmd.Printf("agent DID:      <unreadable: %v>\n", err)
				}
			}
			if cfg.UploadServiceURL != "" {
				cmd.Printf("upload service: %s (%s)\n", cfg.UploadServiceURL, cfg.UploadServiceDID)
			}
			if cfg.IndexerEndpoint != "" {
				cmd.Printf("indexer:        %s (%s)\n", cfg.IndexerEndpoint, cfg.IndexerDID)
			}
			return nil
		},
	}
}

// Execute runs the root command.
func Execute() {
	if err := newRootCmd().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "ingot:", err)
		os.Exit(1)
	}
}
