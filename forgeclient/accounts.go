// Carried from github.com/fil-forge/guppy/pkg/client/accounts.go.
package forgeclient

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan/command"
)

// Accounts returns the did:mailto accounts the agent is logged in as: the
// distinct issuers of the account→agent (Top) delegations held in the token
// store, sorted lexicographically.
func (c *Client) Accounts(ctx context.Context) ([]did.DID, error) {
	accounts := make(map[did.DID]struct{})
	for d, err := range c.tokenStore.ListDelegations(ctx, c.signer.DID(), command.Top(), did.Undef) {
		if err != nil {
			return nil, err
		}
		if d.Issuer().Method() == "mailto" {
			accounts[d.Issuer()] = struct{}{}
		}
	}
	result := slices.Collect(maps.Keys(accounts))
	slices.SortFunc(result, func(a, b did.DID) int {
		return cmp.Compare(a.String(), b.String())
	})
	return result, nil
}
