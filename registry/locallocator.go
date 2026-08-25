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

type LocationReader interface {
	GetLocation(ctx context.Context, space did.DID, digest mh.Multihash) (*BlobLocation, error)
}

// InclusionReader resolves which stored shard contains a block, and where.
type InclusionReader interface {
	GetInclusion(ctx context.Context, space did.DID, digest mh.Multihash) (*BlobInclusion, error)
}

// LocalLocator resolves a blob's retrieval location from the local
// blob_locations + shard_inclusions tables instead of the indexing-service —
// the appliance read tier (the R0/R1 reduction, docs/architecture.md §8). It
// mimics the indexing-service contract with the same two relations the
// IndexLocator consumes from query results: location commitments ("blob X is
// stored whole at provider/URL") and inclusions ("block B is at byte range
// [s,e] inside blob X"). It satisfies blockstore/locator.Locator so it drops
// straight into the existing Forge block reader's UCAN /content/retrieve path:
// only the *source* of the commitment differs (Postgres rows recorded at
// upload/ship, vs an indexer query). Swapping in an indexer-backed Locator
// later needs no read-path change.
//
// A body blob is stored whole on piri, so it resolves directly from
// blob_locations with the full range [0, size-1] (blobindex.Range End is
// inclusive — see Forge.GetBlock). A catalog block (manifest / MST node) is
// stored *inside* a shipped segment CAR: it resolves via its shard_inclusions
// row to the shard's blob_locations row, with the inner byte range — which is
// what keeps retention-retired catalog blocks readable.
//
// LocationReader / InclusionReader are the read slices of LocationStore /
// InclusionStore the LocalLocator needs. *Postgres (and *inmem.MemStore)
// satisfies both.
type LocalLocator struct {
	locations  LocationReader
	inclusions InclusionReader
}

// NewLocalLocator builds a LocalLocator over the blob-location and
// shard-inclusion tables.
func NewLocalLocator(locations LocationReader, inclusions InclusionReader) *LocalLocator {
	return &LocalLocator{locations: locations, inclusions: inclusions}
}

var _ locator.Locator = (*LocalLocator)(nil)

// Locate returns the recorded location(s) of digest across the given spaces,
// or a locator.NotFoundError if none is recorded. Whole blobs (bodies, shipped
// shard CARs) resolve directly; blocks inside a shipped shard resolve through
// their inclusion row to the shard's location with the inner byte range.
func (l *LocalLocator) Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]locator.Location, error) {
	var out []locator.Location
	for _, space := range spaces {
		loc, err := l.locateIn(ctx, space, digest)
		if errors.Is(err, ErrNotFound) {
			continue
		}
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

// locateIn resolves digest within one space: whole-blob row first, then the
// inclusion → shard-location path. Returns ErrNotFound when neither exists.
func (l *LocalLocator) locateIn(ctx context.Context, space did.DID, digest mh.Multihash) (locator.Location, error) {
	bl, err := l.locations.GetLocation(ctx, space, digest)
	if err == nil {
		return blobLocationToLocation(space, digest, bl)
	}
	if !errors.Is(err, ErrNotFound) {
		return locator.Location{}, fmt.Errorf("local locator: get location: %w", err)
	}

	inc, err := l.inclusions.GetInclusion(ctx, space, digest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return locator.Location{}, ErrNotFound
		}
		return locator.Location{}, fmt.Errorf("local locator: get inclusion: %w", err)
	}
	shardLoc, err := l.locations.GetLocation(ctx, space, inc.ShardDigest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// An inclusion row with no shard location is a write-path
			// invariant violation (the flush records the location first),
			// not absent data — surface it.
			return locator.Location{}, fmt.Errorf("local locator: block %s: shard %x has an inclusion but no location", digest.B58String(), inc.ShardDigest)
		}
		return locator.Location{}, fmt.Errorf("local locator: get shard location: %w", err)
	}
	// The commitment names the SHARD (that is the blob /content/retrieve
	// reads from); the range selects the inner block's bytes out of it —
	// the same shape IndexLocator emits for an inclusion hit.
	loc, err := blobLocationToLocation(space, shardLoc.Digest, shardLoc)
	if err != nil {
		return locator.Location{}, err
	}
	loc.Range = blobindex.Range{Start: inc.RangeStart, End: inc.RangeEnd}
	return loc, nil
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
