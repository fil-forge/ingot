package blockstore

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/fil-forge/libforge/blobindex"
	commands "github.com/fil-forge/libforge/commands"
	"github.com/fil-forge/libforge/testutil"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore/locator"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// stubLocator returns one canned location for any digest, so retrieve gets
// past the locate step and exercises the proof-chain lookup.
type stubLocator struct{ loc locator.Location }

func (s stubLocator) Locate(_ context.Context, _ []did.DID, _ mh.Multihash) ([]locator.Location, error) {
	return []locator.Location{s.loc}, nil
}

func (s stubLocator) LocateMany(_ context.Context, _ []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]locator.Location], error) {
	m := blobindex.NewMultihashMap[[]locator.Location](len(digests))
	for _, d := range digests {
		m.Set(d, []locator.Location{s.loc})
	}
	return m, nil
}

// TestForgeRetrieveWithoutAuthority: a located block whose space has no
// cached delegation chain must surface an explicit retrieval-authority
// error — an auth gap, not ErrNotFound.
func TestForgeRetrieveWithoutAuthority(t *testing.T) {
	signer, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate signer: %v", err)
	}
	space := testutil.RandomDID(t)
	provider := testutil.RandomDID(t)

	digest, err := mh.Sum([]byte("blob"), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("mh: %v", err)
	}

	target, err := url.Parse("http://piri.example/blob")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	f, err := NewForge(ForgeConfig{
		Locator: stubLocator{loc: locator.Location{
			Commitment: locator.Commitment{
				Node:     provider,
				Space:    space,
				Content:  digest,
				Location: []commands.CborURL{commands.CborURL(*target)},
			},
			Range: blobindex.Range{Start: 0, End: 3},
		}},
		Signer: signer,
	})
	if err != nil {
		t.Fatalf("NewForge: %v", err)
	}
	blk := cid.NewCidV1(cid.Raw, digest)

	t.Run("no request-scoped store", func(t *testing.T) {
		_, err := f.GetBlock(context.Background(), space, blk)
		if err == nil || !strings.Contains(err.Error(), "no request-scoped retrieval authority") {
			t.Fatalf("expected missing-scoped-store error, got: %v", err)
		}
	})

	t.Run("empty scoped store", func(t *testing.T) {
		// A store is present but holds no chain for the space — an auth gap,
		// distinct from the no-store case above.
		ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(),
			ucanlib.ProofStore(ucanlib.NewContainerProofStore(container.New())))
		_, err := f.GetBlock(ctx, space, blk)
		if err == nil || !strings.Contains(err.Error(), "no retrieval authority") {
			t.Fatalf("expected authority-gap error, got: %v", err)
		}
	})
}
