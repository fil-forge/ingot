package forgeclient

import (
	"context"
	"fmt"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/multiformats/go-multihash"
)

// BlobRemove invokes /blob/remove against the upload service (sprue),
// releasing the space's claim on the blob. Sprue deregisters the blob and
// forwards the removal to every storage node holding it; piri deletes the
// bytes only when no space claims the digest at all. Idempotent: removing an
// unknown or already-removed blob succeeds.
func (c *Client) BlobRemove(ctx context.Context, digest multihash.Multihash, space did.DID) error {
	proofs, proofLinks, err := c.ProofChain(ctx, c.signer.DID(), blobcmds.Remove.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := blobcmds.Remove.Invoke(
		c.signer,
		space,
		&blobcmds.RemoveArguments{Space: space, Digest: digest},
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
