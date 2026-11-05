package s3frontend

import (
	"context"
	"errors"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
)

// chainStore answers every lookup with its chain, or err.
type chainStore struct {
	links []cid.Cid
	err   error
}

func (s chainStore) ProofChain(context.Context, did.DID, ucan.Command, did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	return make([]ucan.Delegation, len(s.links)), s.links, nil
}

// TestProofStoresReadsInTurn: the first store with a chain answers, so a
// request's fresh chains win over a release's captured ones, and the captured
// ones answer for a command the request holds none for.
func TestProofStoresReadsInTurn(t *testing.T) {
	live := []cid.Cid{cid.NewCidV1(cid.Raw, mustSum([]byte("live")))}
	captured := []cid.Cid{cid.NewCidV1(cid.Raw, mustSum([]byte("captured")))}
	for name, tc := range map[string]struct {
		stores []ucanlib.ProofStore
		want   []cid.Cid
	}{
		"the request's chain first":           {[]ucanlib.ProofStore{chainStore{links: live}, chainStore{links: captured}}, live},
		"the captured chain when it has none": {[]ucanlib.ProofStore{chainStore{err: errors.New("no chain")}, chainStore{links: captured}}, captured},
		"the captured chain when it is empty": {[]ucanlib.ProofStore{chainStore{}, chainStore{links: captured}}, captured},
	} {
		t.Run(name, func(t *testing.T) {
			_, links, err := proofStores(tc.stores).ProofChain(t.Context(), did.Undef, blobcmds.Remove.Command, did.Undef)
			if err != nil || len(links) != 1 || links[0] != tc.want[0] {
				t.Fatalf("ProofChain = %v, %v; want %v", links, err, tc.want)
			}
		})
	}
	if _, _, err := (proofStores{chainStore{err: errors.New("none")}}).ProofChain(t.Context(), did.Undef, blobcmds.Remove.Command, did.Undef); err == nil {
		t.Fatal("ProofChain with no chain anywhere succeeded")
	}
}
