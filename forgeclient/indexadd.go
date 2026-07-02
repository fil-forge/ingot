// Carried from github.com/fil-forge/guppy/pkg/client/indexadd.go.
package forgeclient

import (
	"context"
	"fmt"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	indexcmds "github.com/fil-forge/libforge/commands/index"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/fil-forge/ucantone/ucan/delegation/policy"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// IndexAdd publishes an /index/add for indexCID against the upload
// service. It carries a /content/retrieve delegation (restricted to the
// index digest) in metadata so sprue can re-delegate retrieval of the
// index blob to the indexing service.
func (c *Client) IndexAdd(ctx context.Context, indexCID cid.Cid, space did.DID) error {
	retrievalAuth, err := contentcmds.Retrieve.Delegate(
		c.signer,
		c.serviceID,
		space,
		delegation.WithPolicyBuilder(
			policy.Equal(".blob.digest", []byte(indexCID.Hash())),
		),
	)
	if err != nil {
		return fmt.Errorf("creating retrieval auth delegation: %w", err)
	}
	retrievalProofs, retrievalProofLinks, err := c.ProofChain(ctx, c.signer.DID(), contentcmds.Retrieve.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}

	proofs, proofLinks, err := c.ProofChain(ctx, c.signer.DID(), indexcmds.Add.Command, space)
	if err != nil {
		return fmt.Errorf("building proof chain: %w", err)
	}
	inv, err := indexcmds.Add.Invoke(
		c.signer,
		space,
		&indexcmds.AddArguments{Index: indexCID},
		invocation.WithAudience(c.serviceID),
		invocation.WithMetadata(datamodel.Map{
			"retrievalAuth": append(retrievalProofLinks, retrievalAuth.Link()),
		}),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	_, _, _, err = Execute[*indexcmds.AddOK](
		ctx,
		c.ucanClient,
		inv,
		execution.WithDelegations(proofs...),
		execution.WithDelegations(retrievalProofs...),
		// The leaf delegation (agent → sprue) granting /content/retrieve
		// on this space must travel with the request — metadata only
		// carries CID links.
		execution.WithDelegations(retrievalAuth),
	)
	if err != nil {
		return fmt.Errorf("executing invocation: %w", err)
	}
	return nil
}
