package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/fil-forge/libforge/blobindex"
	"github.com/fil-forge/libforge/commands"
	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore/locator"
)

// LocalLocator resolves a blob's retrieval location from the local blob_locations
// table instead of the indexing-service — the appliance read tier (the R0/R1
// reduction, docs/architecture.md §8). It satisfies blockstore/locator.Locator
// so it drops straight into the existing Forge block reader's UCAN
// /content/retrieve path: only the *source* of the location commitment differs
// (a Postgres row recorded at upload/accept, vs an indexer query). Swapping in an
// indexer-backed Locator later needs no read-path change.
//
// ingot stores each body blob whole on piri, so a blob's retrieval range is the
// entire blob: [0, size-1] (blobindex.Range End is inclusive — see Forge.GetBlock).
// LocationReader is the read slice of LocationStore the LocalLocator needs.
// *Postgres (and any full LocationStore) satisfies it.
type LocationReader interface {
	GetLocation(ctx context.Context, space did.DID, digest []byte) (*BlobLocation, error)
}

type LocalLocator struct {
	store LocationReader
}

// NewLocalLocator builds a LocalLocator over the blob-location table.
func NewLocalLocator(store LocationReader) *LocalLocator {
	return &LocalLocator{store: store}
}

var _ locator.Locator = (*LocalLocator)(nil)

// Locate returns the recorded location(s) of digest across the given spaces,
// or a locator.NotFoundError if none is recorded.
func (l *LocalLocator) Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]locator.Location, error) {
	var out []locator.Location
	for _, space := range spaces {
		bl, err := l.store.GetLocation(ctx, space, digest)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("local locator: get location: %w", err)
		}
		loc, err := blobLocationToLocation(space, digest, bl)
		if err != nil {
			return nil, err
		}
		out = append(out, loc)
	}
	if len(out) == 0 {
		return nil, locator.NotFoundError{Hash: digest}
	}
	return out, nil
}

// LocateMany resolves several digests; digests with no recorded location are
// absent from the returned map (matching the indexer-backed locator's contract).
func (l *LocalLocator) LocateMany(ctx context.Context, spaces []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]locator.Location], error) {
	m := blobindex.NewMultihashMap[[]locator.Location](len(digests))
	for _, digest := range digests {
		locs, err := l.Locate(ctx, spaces, digest)
		if err != nil {
			var nf locator.NotFoundError
			if errors.As(err, &nf) {
				continue
			}
			return nil, err
		}
		m.Set(digest, locs)
	}
	return m, nil
}

// blobLocationToLocation maps a stored blob_locations row to the same Location
// shape the index locator extracts from indexer results, so Forge.GetBlock
// consumes it unchanged.
func blobLocationToLocation(space did.DID, digest mh.Multihash, bl *BlobLocation) (locator.Location, error) {
	provider, err := did.Parse(bl.Provider)
	if err != nil {
		return locator.Location{}, fmt.Errorf("local locator: parse provider %q: %w", bl.Provider, err)
	}
	u, err := url.Parse(bl.URL)
	if err != nil {
		return locator.Location{}, fmt.Errorf("local locator: parse url %q: %w", bl.URL, err)
	}
	// Whole-blob storage: retrieve the entire blob. End is inclusive, so a size-N
	// blob is [0, N-1] and Forge computes wantLen = End-Start+1 = N.
	end := bl.Size - 1
	if end < 0 {
		end = 0
	}
	return locator.Location{
		Commitment: locator.Commitment{
			Node:     provider,
			Space:    space,
			Content:  digest,
			Location: []commands.CborURL{commands.CborURL(*u)},
		},
		Range: blobindex.Range{Start: 0, End: end},
	}, nil
}
