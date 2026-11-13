// Package cmd is the ingot daemon's cobra/viper CLI:
//   - `serve` runs the S3 gateway;
//   - `whoami` and `version` report the agent and the build;
//   - `blake3` hashes and verifies object data against ingot's BLAKE3 CIDs;
//   - `catalog usage` measures each bucket's catalog storage, and `footprint`
//     what the node keeps in Postgres and on disk, for benchmarks and
//     estate-scale runs. Both are left out of the help unless
//     INGOT_DEV_COMMANDS is set to a true value.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/fil-forge/ingot/config"
	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/internal/build"
)

// cfgFile is bound to the persistent --config flag.
var cfgFile string

// devCommandsEnv, set to a true value, lists the measurement commands in the
// help. They run whether or not it is set: it only keeps them out of the way
// of operators who don't need them.
const devCommandsEnv = "INGOT_DEV_COMMANDS"

// devCommandsListed reports whether devCommandsEnv asks for the measurement
// commands in the help.
func devCommandsListed() bool {
	listed, _ := strconv.ParseBool(os.Getenv(devCommandsEnv))
	return listed
}

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
	for _, c := range []*cobra.Command{newCatalogCmd(), newFootprintCmd()} {
		c.Hidden = !devCommandsListed()
		root.AddCommand(c)
	}
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
