package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot"
)

func newSpaceCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "space",
		Short: "Manage the ingot space key (the DID all data is associated with)",
	}
	c.AddCommand(newSpaceGenerateCmd(), newSpaceLsCmd())
	return c
}

func spaceKeyPath(cfg *DaemonConfig) string {
	return filepath.Join(cfg.DataDir, "space.key")
}

func newSpaceGenerateCmd() *cobra.Command {
	var force bool
	c := &cobra.Command{
		Use:     "generate",
		Aliases: []string{"gen"},
		Short:   "Create the space key under data_dir and print its DID",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
				return fmt.Errorf("creating data dir: %w", err)
			}
			path := spaceKeyPath(cfg)
			if force {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("removing existing key: %w", err)
				}
			}
			signer, err := ingot.LoadOrCreateSigner(path)
			if err != nil {
				return err
			}
			cmd.PrintErrf("space key: %s\n", path)
			cmd.Println(signer.DID().String())
			return nil
		},
	}
	c.Flags().BoolVar(&force, "force", false, "overwrite an existing space key")
	return c
}

func newSpaceLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"info"},
		Short:   "Print the current space DID",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
			path := spaceKeyPath(cfg)
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("no space key at %s (run `ingot space generate`)", path)
			}
			signer, err := ingot.LoadOrCreateSigner(path)
			if err != nil {
				return err
			}
			cmd.Println(signer.DID().String())
			return nil
		},
	}
}
