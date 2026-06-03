// Carried from github.com/fil-forge/guppy/pkg/client/provideradd.go.
package forgeclient

import (
	"context"
	"fmt"

	providercmds "github.com/fil-forge/libforge/commands/provider"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan/invocation"
)

// ProviderAdd invokes the /provider/add capability to provision a space with a
// customer account. The proof chain is keyed on the account (the agent must
// hold an account→agent delegation from login); the space is carried only as
// the Consumer argument, so no space private key is required here.
func (c *Client) ProviderAdd(ctx context.Context, customerAccount did.DID, provider did.DID, consumer did.DID) (*providercmds.AddOK, error) {
	proofs, proofLinks, err := c.ProofChain(ctx, c.signer.DID(), providercmds.Add.Command, customerAccount)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}
	attestations, err := c.ProofAttestations(ctx, proofs, c.serviceID)
	if err != nil {
		return nil, fmt.Errorf("fetching proof attestations: %w", err)
	}

	inv, err := providercmds.Add.Invoke(
		c.signer,
		customerAccount,
		&providercmds.AddArguments{
			Provider: provider,
			Consumer: consumer,
		},
		invocation.WithAudience(c.serviceID),
		invocation.WithProofs(proofLinks...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	addOK, _, _, err := Execute[*providercmds.AddOK](
		ctx,
		c.ucanClient,
		inv,
		execution.WithDelegations(proofs...),
		execution.WithInvocations(attestations...),
	)
	if err != nil {
		return nil, fmt.Errorf("executing provider add invocation: %w", err)
	}

	return addOK, nil
}
