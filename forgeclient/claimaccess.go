// Carried from github.com/fil-forge/guppy/pkg/client/claimaccess.go.
package forgeclient

import (
	"context"
	"fmt"
	"slices"

	accesscmds "github.com/fil-forge/libforge/commands/access"
	attestcmds "github.com/fil-forge/libforge/commands/ucan/attest"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
)

// ClaimAccess fetches stored delegations from the service — the second
// step of agent authorization (after the out-of-band email confirmation
// of a prior RequestAccess).
func (c *Client) ClaimAccess(ctx context.Context, sub did.DID) ([]ucan.Delegation, []ucan.Invocation, error) {
	var proofs []ucan.Delegation
	var proofLinks []cid.Cid
	var err error
	if c.signer.DID() != sub {
		proofs, proofLinks, err = c.ProofChain(ctx, c.signer.DID(), accesscmds.Claim.Command, sub)
		if err != nil {
			return nil, nil, fmt.Errorf("building proof chain: %w", err)
		}
	}

	inv, err := accesscmds.Claim.Invoke(
		c.signer,
		sub,
		&accesscmds.ClaimArguments{},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating invocation: %w", err)
	}

	claimOK, _, meta, err := Execute[*accesscmds.ClaimOK](
		ctx,
		c.ucanClient,
		inv,
		execution.WithDelegations(proofs...),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("executing claim invocation: %w", err)
	}

	var dlgs []ucan.Delegation
	for _, d := range meta.Delegations() {
		if slices.Contains(claimOK.Delegations, d.Link()) {
			dlgs = append(dlgs, d)
		}
	}
	var attestations []ucan.Invocation
	for _, inv := range meta.Invocations() {
		if inv.Command() != attestcmds.Proof.Command {
			continue
		}
		if inv.Audience() == c.signer.DID() {
			attestations = append(attestations, inv)
		}
	}

	return dlgs, attestations, nil
}
