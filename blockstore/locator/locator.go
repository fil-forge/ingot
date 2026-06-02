package locator

import (
	"context"
	"fmt"

	"github.com/fil-forge/libforge/blobindex"
	"github.com/fil-forge/libforge/commands"
	"github.com/fil-forge/libforge/commands/assert"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
)

type Locator interface {
	// Locate finds and returns the locations of a single block identified by its
	// digest. Returns a [NotFoundError] if no locations are found.
	Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]Location, error)

	// LocateMany finds the locations of multiple blocks identified by their
	// digests, returning a map from digest to locations. Digests with no found
	// locations will be absent from the returned map.
	LocateMany(ctx context.Context, spaces []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]Location], error)
}

// Commitment is the fields extracted from a signed location commitment UCAN.
type Commitment struct {
	Node     did.DID
	Space    did.DID
	Content  mh.Multihash
	Location []commands.CborURL
	Range    *assert.Range
}

type Location struct {
	Commitment Commitment
	Range      blobindex.Range
}

type NotFoundError struct {
	Hash mh.Multihash
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("no locations found for block: %s", digestutil.Format(e.Hash))
}
