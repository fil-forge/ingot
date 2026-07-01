package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore/locator"
)

// fakeLocStore is a minimal in-test LocationReader (avoids importing inmem, which
// imports registry).
type fakeLocStore struct{ loc *BlobLocation }

func (f fakeLocStore) GetLocation(_ context.Context, _ string, _ []byte) (*BlobLocation, error) {
	if f.loc == nil {
		return nil, ErrNotFound
	}
	return f.loc, nil
}

const testProviderDID = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

func TestLocalLocator_Locate(t *testing.T) {
	ctx := context.Background()
	digest, err := mh.Sum([]byte("blob bytes"), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	space, err := did.Parse(testProviderDID)
	if err != nil {
		t.Fatalf("parse space: %v", err)
	}
	const url = "http://piri:80/blob/z6abc"

	ll := NewLocalLocator(fakeLocStore{loc: &BlobLocation{
		Space: space.String(), Digest: digest, Provider: testProviderDID, URL: url, Size: 1234,
	}})

	locs, err := ll.Locate(ctx, []did.DID{space}, digest)
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if len(locs) != 1 {
		t.Fatalf("got %d locations, want 1", len(locs))
	}
	cm := locs[0].Commitment
	if cm.Node.String() != testProviderDID {
		t.Errorf("Node = %s, want %s", cm.Node, testProviderDID)
	}
	if string(cm.Content) != string(digest) {
		t.Errorf("Content digest mismatch")
	}
	if len(cm.Location) != 1 || cm.Location[0].URL().String() != url {
		t.Errorf("Location = %v, want [%s]", cm.Location, url)
	}
	// Whole blob, End inclusive: [0, size-1] so Forge computes End-Start+1 = size.
	if locs[0].Range.Start != 0 || locs[0].Range.End != 1233 {
		t.Errorf("Range = {%d,%d}, want {0,1233}", locs[0].Range.Start, locs[0].Range.End)
	}
}

func TestLocalLocator_NotFound(t *testing.T) {
	space, _ := did.Parse(testProviderDID)
	digest, _ := mh.Sum([]byte("absent"), mh.SHA2_256, -1)
	ll := NewLocalLocator(fakeLocStore{loc: nil})

	_, err := ll.Locate(context.Background(), []did.DID{space}, digest)
	var nf locator.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("err = %v, want locator.NotFoundError", err)
	}
}
