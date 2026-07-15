package forgeclient

import (
	"context"
	"fmt"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// BlobUnallocate invokes /blob/unallocate against the upload service (sprue),
// retiring the space's parked (never-accepted) blob. cause is the
// /space/blob/add task link (ParkedBlob.AddTask) — sprue walks its receipt
// chain to locate the storage node holding the parked bytes, which have no
// registration or acceptance to look up by. Idempotent on the node; a blob
// that has been accepted is refused (release it via the reference index /
// /blob/remove instead).
func (c *Client) BlobUnallocate(ctx context.Context, digest multihash.Multihash, space did.DID, cause cid.Cid) error {
	proofs, proofLinks, err := c.ProofChain(ctx, c.signer.DID(), blobcmds.Unallocate.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := blobcmds.Unallocate.Invoke(
		c.signer,
		space,
		&blobcmds.UnallocateArguments{Space: space, Digest: digest, Cause: &cause},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*blobcmds.UnallocateOK](
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
