// Carried from github.com/fil-forge/guppy/pkg/client/accessdelegate.go.
package forgeclient

import (
	"context"
	"fmt"

	"github.com/fil-forge/libforge/commands/access"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// AccessDelegate invokes the /access/delegate capability to store delegations
// on the service. This allows an agent to store delegations (like space grants)
// so they can be retrieved later.
func (c *Client) AccessDelegate(ctx context.Context, space did.DID, delegations ...ucan.Delegation) (*access.DelegateOK, error) {
	delegationLinks := make([]cid.Cid, 0, len(delegations))
	for _, d := range delegations {
		delegationLinks = append(delegationLinks, d.Link())
	}
	args := access.DelegateArguments{Delegations: delegationLinks}

	proofs, proofLinks, err := c.ProofChain(ctx, c.signer.DID(), access.Delegate.Command, space)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}

	inv, err := access.Delegate.Invoke(
		c.signer,
		space,
		&args,
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	delegateOK, _, _, err := Execute[*access.DelegateOK](
		ctx,
		c.ucanClient,
		inv,
		execution.WithDelegations(proofs...),
		execution.WithDelegations(delegations...),
	)
	if err != nil {
		return nil, fmt.Errorf("executing delegate invocation: %w", err)
	}

	return delegateOK, nil
}
