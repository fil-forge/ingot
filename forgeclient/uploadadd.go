// Carried from github.com/fil-forge/guppy/pkg/client/uploadadd.go.
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

// UploadAdd invokes /upload/add against the upload service (sprue), recording
// root as a content entry in the space. The space is the invocation subject.
// The entry is what sprue counts to report the space's object count; it does
// not make the content durable, which /blob/add already did.
//
// Upsert semantics: adding the same root again merges the shards and replaces
// the index, and sprue counts only the first add, so a retry is safe.
//
// The shards argument is deliberately empty from ingot. The capability
// describes shards as the CAR archives containing the content DAG, and ingot's
// object bodies are raw sha256-addressed blobs rather than CARs — the blobs a
// root covers are already registered individually by /blob/add, and repeating
// them here would write a shard row per blob per object version for no reader.
func (c *Client) UploadAdd(ctx context.Context, space did.DID, root cid.Cid, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}

	proofs, proofLinks, err := proofStore.ProofChain(ctx, c.signer.DID(), uploadcmds.Add.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := uploadcmds.Add.Invoke(
		c.signer,
		space,
		&uploadcmds.AddArguments{Root: root},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*uploadcmds.AddOK](
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
