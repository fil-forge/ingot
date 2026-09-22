// Carried from github.com/fil-forge/guppy/pkg/client/uploadremove.go.
package forgeclient

import (
	"context"
	"fmt"

	uploadcmds "github.com/fil-forge/libforge/commands/upload"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// UploadRemove invokes /upload/remove against the upload service (sprue),
// dropping root's content entry from the space. The space is the invocation
// subject. It does NOT remove the blobs the root covers — that is a separate
// per-digest /blob/remove decision the reference index owns, since a blob can
// be claimed by more than one version. Idempotent: removing an unknown root
// succeeds.
func (c *Client) UploadRemove(ctx context.Context, space did.DID, root cid.Cid, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}

	proofs, proofLinks, err := proofStore.ProofChain(ctx, c.signer.DID(), uploadcmds.Remove.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := uploadcmds.Remove.Invoke(
		c.signer,
		space,
		&uploadcmds.RemoveArguments{Root: root},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*uploadcmds.RemoveOK](
		ctx,
		c.ucanClient,
		inv,
		execution.WithDelegations(proofs...),
	)
	if err != nil {
		return fmt.Errorf("executing invocation: %w", err)
	}
	return nil
}
