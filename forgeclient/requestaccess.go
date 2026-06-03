// Carried from github.com/fil-forge/guppy/pkg/client/requestaccess.go.
package forgeclient

import (
	"context"
	"fmt"

	"github.com/fil-forge/libforge/commands/access"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/invocation"
)

// RequestAccess requests access to the service as an Account — the first
// step of agent authorization. The agent (issuer) asks to act as account.
func (c *Client) RequestAccess(ctx context.Context, account did.DID) (*access.RequestOK, error) {
	inv, err := access.Request.Invoke(
		c.signer,
		c.signer.DID(),
		&access.RequestArguments{
			Issuer: account,
			Attenuations: []access.CapabilityRequest{
				{Command: command.Top()},
			},
		},
		invocation.WithAudience(c.serviceID),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	requestOK, _, _, err := Execute[*access.RequestOK](ctx, c.ucanClient, inv)
	if err != nil {
		return nil, fmt.Errorf("executing request invocation: %w", err)
	}

	return requestOK, nil
}
