package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/libforge/didmailto"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/delegation"
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
	var (
		force       bool
		name        string
		grantTo     string
		provisionTo string
		outputKey   bool
	)
	c := &cobra.Command{
		Use:     "generate",
		Aliases: []string{"gen"},
		Short:   "Create the space key under data_dir, provision it, and grant access",
		Long: `Create (or reuse) the space key under data_dir, provision it to a
customer account on the Forge upload service (sprue), grant the account and this
agent access to it, and print the space DID.

Provisioning and granting require a logged-in account (see ` + "`ingot login`" + `).
--provision-to / --grant-to default to the single logged-in account; specify
them explicitly (by account DID or email) when more than one is logged in.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
				return fmt.Errorf("creating data dir: %w", err)
			}

			// 1. Mint or reuse the persisted space key. Unlike guppy (whose
			// space is ephemeral), ingot's daemon reloads this key every boot,
			// so we persist it and reuse an existing one unless --force.
			path := spaceKeyPath(cfg)
			if force {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("removing existing key: %w", err)
				}
			}
			space, err := ingot.LoadOrCreateSigner(path)
			if err != nil {
				return err
			}
			cmd.PrintErrf("space key: %s\n", path)

			// 2. Optionally surface the raw private key (it is also on disk at
			// the path printed above).
			if outputKey {
				type rawSigner interface{ Raw() []byte }
				rs, ok := space.(rawSigner)
				if !ok {
					return fmt.Errorf("space signer does not expose raw key bytes")
				}
				cmd.PrintErrln("\nWARNING: This is your space private key. Keep it secret and secure!")
				cmd.PrintErrln("Space Key (base64):")
				cmd.Println(base64.StdEncoding.EncodeToString(rs.Raw()))
				cmd.PrintErrln()
			}

			// 3. Build the edge client against sprue (same wiring as `login`).
			if cfg.UploadServiceURL == "" || cfg.UploadServiceDID == "" {
				return fmt.Errorf("upload_service_url and upload_service_did must be configured")
			}
			if cfg.Identity.KeyFile == "" {
				return fmt.Errorf("identity.key_file (agent PEM) must be configured")
			}
			id, err := loadAgentIdentity(cfg.Identity.KeyFile)
			if err != nil {
				return err
			}
			sprueURL, err := url.Parse(cfg.UploadServiceURL)
			if err != nil {
				return fmt.Errorf("parse upload_service_url: %w", err)
			}
			sprueDID, err := did.Parse(cfg.UploadServiceDID)
			if err != nil {
				return fmt.Errorf("parse upload_service_did: %w", err)
			}
			store, err := tokenstore.NewFsStore(cfg.TokenStoreDir)
			if err != nil {
				return err
			}
			c, err := forgeclient.New(id.Signer, sprueDID, *sprueURL, forgeclient.WithTokenStore(store))
			if err != nil {
				return err
			}

			// 4. Resolve the provision/grant accounts (errors if not logged in).
			accounts, err := c.Accounts(ctx)
			if err != nil {
				return fmt.Errorf("listing logged-in accounts: %w", err)
			}
			provisionAccount, err := pickAccount(accounts, provisionTo, "provision-to")
			if err != nil {
				return err
			}
			grantAccount, err := pickAccount(accounts, grantTo, "grant-to")
			if err != nil {
				return err
			}

			// 5. Provision the space to the customer account.
			cmd.PrintErrf("Provisioning %s to %s...\n\n", space.DID(), provisionAccount)
			if _, err := c.ProviderAdd(ctx, provisionAccount, c.ServiceID(), space.DID()); err != nil {
				return fmt.Errorf("provisioning space: %w", err)
			}

			// 6. Grant the account (and this agent) access to the space.
			cmd.PrintErrf("Granting access on %s to %s...\n\n", space.DID(), grantAccount)
			if err := grant(ctx, c, space, grantAccount, name); err != nil {
				return fmt.Errorf("granting capabilities: %w", err)
			}

			// 7. The space DID is the only thing on stdout.
			cmd.PrintErr("Generated space: ")
			cmd.Println(space.DID().String())
			return nil
		},
	}
	c.Flags().BoolVar(&force, "force", false, "overwrite an existing space key")
	c.Flags().StringVar(&name, "name", "", "Name for the space (optional)")
	c.Flags().StringVar(&grantTo, "grant-to", "", "Account DID or email to grant space access to (optional when exactly one account is logged in)")
	c.Flags().StringVar(&provisionTo, "provision-to", "", "Account DID or email to provision the space to (optional when exactly one account is logged in)")
	c.Flags().BoolVarP(&outputKey, "output-key", "k", false, "Output the space key (WARNING: sensitive data)")
	return c
}

// pickAccount resolves the account for a --provision-to/--grant-to flag. An
// explicit value (account DID or email) must already be logged in; otherwise it
// defaults to the single logged-in account, erroring on zero or many.
func pickAccount(accounts []did.DID, flagValue, flagName string) (did.DID, error) {
	if flagValue != "" {
		account, err := parseAccount(flagValue)
		if err != nil {
			return did.Undef, fmt.Errorf("parsing --%s account %q: %w", flagName, flagValue, err)
		}
		if !slices.Contains(accounts, account) {
			return did.Undef, fmt.Errorf("account %s is not logged in; run `ingot login %s`", flagValue, flagValue)
		}
		return account, nil
	}
	switch len(accounts) {
	case 0:
		return did.Undef, fmt.Errorf("no accounts are logged in; run `ingot login <email>`")
	case 1:
		return accounts[0], nil
	default:
		var b strings.Builder
		for _, a := range accounts {
			fmt.Fprintf(&b, "  - %s\n", a)
		}
		return did.Undef, fmt.Errorf("multiple accounts are logged in:\n%s\nspecify one with --%s", b.String(), flagName)
	}
}

// parseAccount accepts either a did:mailto DID or a plain email address and
// returns the corresponding did:mailto account DID.
func parseAccount(input string) (did.DID, error) {
	if strings.HasPrefix(input, "did:mailto:") {
		return did.Parse(input)
	}
	return didmailto.New(input)
}

// grant mirrors guppy's space-grant: it issues no-expiry space→agent and
// space→account Top delegations, stores both locally so the agent can act, then
// registers the account's grant with sprue via /access/delegate.
func grant(ctx context.Context, c *forgeclient.Client, space ucan.Signer, account did.DID, name string) error {
	opts := []delegation.Option{delegation.WithNoExpiration()}
	if name != "" {
		opts = append(opts, delegation.WithMetadata(forgeclient.SpaceNameMetadata(name)))
	}

	agentGrant, err := delegation.Delegate(space, c.Issuer().DID(), space.DID(), command.Top(), opts...)
	if err != nil {
		return fmt.Errorf("creating agent delegation: %w", err)
	}
	accountGrant, err := delegation.Delegate(space, account, space.DID(), command.Top(), opts...)
	if err != nil {
		return fmt.Errorf("creating account delegation: %w", err)
	}

	if err := c.AddProofs(ctx, agentGrant, accountGrant); err != nil {
		return fmt.Errorf("storing delegations: %w", err)
	}
	if _, err := c.AccessDelegate(ctx, space.DID(), accountGrant); err != nil {
		return fmt.Errorf("storing delegation via access/delegate: %w", err)
	}
	return nil
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
