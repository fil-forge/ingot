package forgeclient

import (
	"context"
	"fmt"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/multiformats/go-multihash"
)

// BlobRemove invokes /blob/remove against the upload service (sprue),
// releasing the space's claim on the blob. Sprue deregisters the blob and
// forwards a /blob/release to every storage node holding it; piri deletes
// the bytes only when no space claims the digest at all. The space is the
// invocation subject. Idempotent: removing an unknown or already-removed
// blob succeeds.
func (c *Client) BlobRemove(ctx context.Context, space did.DID, digest multihash.Multihash, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}

	proofs, proofLinks, err := proofStore.ProofChain(ctx, c.signer.DID(), blobcmds.Remove.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := blobcmds.Remove.Invoke(
		c.signer,
		space,
		// The space is the invocation subject; it is not repeated in the
		// arguments.
		&blobcmds.RemoveArguments{Digest: digest},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*blobcmds.RemoveOK](
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
