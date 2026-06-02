package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	indexclient "github.com/fil-forge/indexing-service/pkg/client"
	"github.com/fil-forge/indexing-service/pkg/types"
	"github.com/fil-forge/libforge/blobindex"
	"github.com/fil-forge/libforge/commands/assert"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("client/dagservice")

// AuthorizeRetrievalFunc creates a content retrieval authorization for the
// provided space(s).
//
// It should create a short lived delegation that allows the indexing service
// to retrieve indexes from the space(s).
//
// It must also return any delegations needed in an proof chain for invocation.
type AuthorizeRetrievalFunc func(ctx context.Context, spaces []did.DID) ([]ucan.Delegation, error)

func NewIndexLocator(indexer IndexerClient, authorizeRetrieval AuthorizeRetrievalFunc) *indexLocator {
	return &indexLocator{
		indexer:            indexer,
		authorizeRetrieval: authorizeRetrieval,
		commitments:        make(map[string][]Commitment),
		inclusions:         make(map[string][]inclusion),
	}
}

type indexLocator struct {
	indexer            IndexerClient
	authorizeRetrieval AuthorizeRetrievalFunc
	mu                 sync.RWMutex
	commitments        map[string][]Commitment
	inclusions         map[string][]inclusion
}

type inclusion struct {
	shard     mh.Multihash
	byteRange blobindex.Range
}

func cacheKey(spaceDID did.DID, hash mh.Multihash) string {
	return spaceDID.String() + ":" + hash.String()
}

var _ Locator = (*indexLocator)(nil)

type IndexerClient interface {
	QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error)
}

// Locate retrieves locations for the data identified by the given digest, in
// the given space.
func (s *indexLocator) Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]Location, error) {
	log.Debugw("Locating block", "digest", digestutil.Format(digest), "spaces", spaces)
	locations, err := s.LocateMany(ctx, spaces, []mh.Multihash{digest})
	if err != nil {
		return nil, err
	}

	if locations.Has(digest) {
		return locations.Get(digest), nil
	}

	return nil, NotFoundError{Hash: digest}
}

func (s *indexLocator) LocateMany(ctx context.Context, spaces []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]Location], error) {
	log.Debugw("Locating blocks", "count", len(digests), "spaces", spaces)
	locations := blobindex.NewMultihashMap[[]Location](len(digests))
	needInclusions := make([]mh.Multihash, 0, len(digests))
	needLocations := make([]mh.Multihash, 0, len(digests))

	for _, digest := range digests {
		cachedLocations, cachedInclusions := s.getCached(spaces, digest)

		if len(cachedLocations) == 0 {
			if len(cachedInclusions) == 0 {
				needInclusions = append(needInclusions, digest)
			} else {
				// If we have an inclusion but no locations, query the indexer for the
				// location of the shard.
				for _, inclusion := range cachedInclusions {
					needLocations = append(needLocations, inclusion.shard)
				}
			}
		}
	}

	if err := s.query(ctx, spaces, needInclusions, false); err != nil {
		return nil, err
	}

	if err := s.query(ctx, spaces, needLocations, true); err != nil {
		return nil, err
	}

	for _, digest := range digests {
		foundLocations, _ := s.getCached(spaces, digest)
		if len(foundLocations) > 0 {
			locations.Set(digest, foundLocations)
		}
	}

	return locations, nil
}

// getCached retrieves cached locations for the given spaces and hash. It
// returns the locations and a boolean indicating whether we know of at least
// one shard which includes the block. If the second return value is false, the
// caller should perform a full query to the indexer to attempt to discover new
// locations. If true, if the locations slice is empty, the caller can perform a
// location-only query to fetch the remaining location information (or to
// confirm that there are no locations).
func (s *indexLocator) getCached(spaces []did.DID, digest mh.Multihash) ([]Location, []inclusion) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var locations []Location
	var inclusions []inclusion
	for _, spaceDID := range spaces {
		incs, hasInclusions := s.inclusions[cacheKey(spaceDID, digest)]
		if !hasInclusions {
			continue
		}
		inclusions = append(inclusions, incs...)

		for _, inclusion := range incs {
			shardCommitments, hasCommitments := s.commitments[cacheKey(spaceDID, inclusion.shard)]
			if hasCommitments {
				for _, commitment := range shardCommitments {
					locations = append(locations, Location{
						Commitment: commitment,
						Range:      inclusion.byteRange,
					})
				}
			}
		}
	}

	return locations, inclusions
}

func (s *indexLocator) query(ctx context.Context, spaces []did.DID, digests []mh.Multihash, omitIndexes bool) error {
	if len(digests) == 0 {
		return nil
	}

	var queryType types.QueryType
	if omitIndexes {
		queryType = types.QueryTypeLocation
	} else {
		queryType = types.QueryTypeStandard
	}

	auth, err := s.authorizeRetrieval(ctx, spaces)
	if err != nil {
		return fmt.Errorf("authorizing retrieval for spaces %v: %w", spaces, err)
	}

	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes:      digests,
		Delegations: auth,
		Match: types.Match{
			Subject: spaces,
		},
		Type: queryType,
	})
	if err != nil {
		var failedResponseErr indexclient.ErrFailedResponse
		if ok := errors.As(err, &failedResponseErr); ok {
			return fmt.Errorf("indexer responded with status %d: %s", failedResponseErr.StatusCode, failedResponseErr.Body)
		}
		hashStrings := make([]string, len(digests))
		for i, h := range digests {
			hashStrings[i] = digestutil.Format(h)
		}
		return fmt.Errorf("querying claims for [%s]: %w", strings.Join(hashStrings, ", "), err)
	}

	blocks := map[cid.Cid][]byte{}
	for _, b := range result.Blocks() {
		blocks[b.Link] = b.Data
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// record the space each blob belongs to
	blobSpace := blobindex.NewMultihashMap[did.DID](-1)

	for _, root := range result.Claims() {
		b, ok := blocks[root]
		if !ok {
			log.Warnw("claim block not found in response", "root", root)
			continue
		}
		claim, err := invocation.Decode(b)
		if err != nil {
			log.Warnw("failed to decode claim", "root", root, "error", err)
			continue
		}
		if claim.Command() != assert.Location.Command {
			continue
		}
		var args assert.LocationArguments
		if err := args.UnmarshalCBOR(bytes.NewReader(claim.ArgumentsBytes())); err != nil {
			log.Warnw("failed to unmarshal location commitment arguments", "root", root, "error", err)
			continue
		}

		claimKey := cacheKey(args.Space, args.Content)
		s.commitments[claimKey] = append(s.commitments[claimKey], Commitment{
			Node:     claim.Issuer(),
			Space:    args.Space,
			Content:  args.Content,
			Location: args.Location,
			Range:    args.Range,
		})
		blobSpace.Set(args.Content, args.Space)
	}

	for _, indexLink := range result.Indexes() {
		indexBytes, ok := blocks[indexLink]
		if !ok {
			log.Warnw("index block not found in response", "index", indexLink)
			continue
		}
		index, err := blobindex.Extract(bytes.NewReader(indexBytes))
		if err != nil {
			return fmt.Errorf("extracting index: %w", err)
		}

		for shardDigest, shardMap := range index.Shards().Iterator() {
			spaceDID := blobSpace.Get(shardDigest)
			if spaceDID == did.Undef {
				if len(spaces) == 1 {
					spaceDID = spaces[0]
				} else {
					// if we don't know the space for this shard from our query result,
					// and we are locating across multiple spaces then assume it is the
					// same space as the index (it should be).
					spaceDID = blobSpace.Get(indexLink.Hash())
					// we expect the indexer to return a location claim for the index though
					if spaceDID == did.Undef {
						digestStrs := make([]string, 0, len(digests))
						for _, d := range digests {
							digestStrs = append(digestStrs, digestutil.Format(d))
						}
						return fmt.Errorf("missing location claim for index %q in query results for: %s", indexLink, strings.Join(digestStrs, ", "))
					}
				}
			}
			for sliceDigest, position := range shardMap.Iterator() {
				key := cacheKey(spaceDID, sliceDigest)
				s.inclusions[key] = append(s.inclusions[key], inclusion{
					shard:     shardDigest,
					byteRange: position,
				})
			}
		}
	}

	return nil
}
