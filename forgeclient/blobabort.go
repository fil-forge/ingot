package forgeclient

import (
	"context"
	"fmt"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// BlobAbort invokes /blob/abort against the upload service (sprue),
// abandoning the space's in-flight upload of a parked (never-accepted) blob.
// add is the /blob/add task link (AddedBlob.AddTask): sprue walks its receipt
// chain to the storage node holding the upload and forwards a /blob/reject of
// the allocation the add made there. The space is the invocation subject.
// Idempotent on the node; a blob the space has accepted is refused with
// BlobAccepted (release it via the reference index / /blob/remove instead).
func (c *Client) BlobAbort(ctx context.Context, space did.DID, add cid.Cid, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}

	proofs, proofLinks, err := proofStore.ProofChain(ctx, c.signer.DID(), blobcmds.Abort.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := blobcmds.Abort.Invoke(
		c.signer,
		space,
		// The space is the invocation subject; it is not repeated in the
		// arguments.
		&blobcmds.AbortArguments{Add: add},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*blobcmds.AbortOK](
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
