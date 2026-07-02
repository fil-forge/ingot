package cmd

import (
	"fmt"
	"net/url"

	"github.com/fil-forge/libforge/attestation/didmailto"
	"github.com/fil-forge/ucantone/did"
	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/tokenstore"
)

func newLoginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "login <email>",
		Short: "Authorize ingot against the upload service via email",
		Long: `Authorize this ingot agent to act for an account on the Forge upload
service (sprue). Performs the guppy-style flow: /access/request, then polls
/access/claim until you confirm via the emailed link. The resulting
delegations are persisted to the token store (tokens.cbor) for forge-mode
serve to use.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			email := args[0]
			cfg, err := Load(cfgFile)
			if err != nil {
				return err
			}
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
			account, err := didmailto.New(email)
			if err != nil {
				return fmt.Errorf("invalid email %q: %w", email, err)
			}

			store, err := tokenstore.NewFsStore(cfg.TokenStoreDir)
			if err != nil {
				return err
			}
			c, err := forgeclient.New(id.Signer, sprueDID, *sprueURL, forgeclient.WithTokenStore(store))
			if err != nil {
				return err
			}

			ctx := cmd.Context()
			authOK, err := c.RequestAccess(ctx, account)
			if err != nil {
				return fmt.Errorf("requesting access: %w", err)
			}
			cmd.PrintErrf("Check %s and click the authorization link to continue...\n", email)

			res := <-c.PollClaim(ctx, authOK)
			claim, err := res.Unpack()
			if err != nil {
				return fmt.Errorf("polling for claim: %w", err)
			}
			if err := c.AddProofs(ctx, claim.Delegations...); err != nil {
				return fmt.Errorf("saving delegations: %w", err)
			}
			if err := c.AddAttestations(ctx, claim.Attestations...); err != nil {
				return fmt.Errorf("saving attestations: %w", err)
			}

			// Claim the account's own delegations (e.g. its spaces).
			accountDlgs, _, err := c.ClaimAccess(ctx, account)
			if err != nil {
				return fmt.Errorf("claiming account access: %w", err)
			}
			if err := c.AddProofs(ctx, accountDlgs...); err != nil {
				return fmt.Errorf("saving account delegations: %w", err)
			}

			cmd.Printf("Logged in as %s (tokens stored in %s)\n", email, cfg.TokenStoreDir)
			return nil
		},
	}
}
