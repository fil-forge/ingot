package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore/locator"
)

// fakeLocStore is a minimal in-test LocationReader + InclusionReader, so the
// locator's lookups are tested without a database. Keyed by string(digest).
type fakeLocStore struct {
	locs map[string]*BlobLocation
	incs map[string]*BlobInclusion
}

func (f fakeLocStore) GetLocation(_ context.Context, _ did.DID, digest mh.Multihash) (*BlobLocation, error) {
	if loc, ok := f.locs[string(digest)]; ok {
		return loc, nil
	}
	return nil, ErrNotFound
}

func (f fakeLocStore) GetInclusion(_ context.Context, _ did.DID, digest mh.Multihash) (*BlobInclusion, error) {
	if inc, ok := f.incs[string(digest)]; ok {
		return inc, nil
	}
	return nil, ErrNotFound
}

const testProviderDID = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

func mustDigest(t *testing.T, data string) mh.Multihash {
	t.Helper()
	digest, err := mh.Sum([]byte(data), mh.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	return digest
}

func TestLocalLocator_Locate(t *testing.T) {
	ctx := context.Background()
	digest := mustDigest(t, "blob bytes")
	space, err := did.Parse(testProviderDID)
	require.NoError(t, err, "parse space")
	const url = "http://piri:80/blob/z6abc"

	ll := NewLocalLocator(fakeLocStore{locs: map[string]*BlobLocation{
		string(digest): {Space: space, Digest: digest, Provider: testProviderDID, URL: url, Size: 1234},
	}}, fakeLocStore{})

	locs, err := ll.Locate(ctx, []did.DID{space}, digest)
	require.NoError(t, err, "Locate")
	require.Len(t, locs, 1)
	cm := locs[0].Commitment
	assert.Equal(t, testProviderDID, cm.Node.String(), "Node")
	assert.Equal(t, string(digest), string(cm.Content), "Content digest mismatch")
	assert.True(t, len(cm.Location) == 1 && cm.Location[0].URL().String() == url, "Location = %v, want [%s]", cm.Location, url)
	// Whole blob, End inclusive: [0, size-1] so Forge computes End-Start+1 = size.
	assert.Equal(t, int64(0), locs[0].Range.Start, "Range.Start")
	assert.Equal(t, int64(1233), locs[0].Range.End, "Range.End")
}

// TestLocalLocator_InclusionFallthrough is the retired-catalog-block case: the
// block has no whole-blob location row, but an inclusion row names its shard,
// and the shard has a location. The result must point the retrieval at the
// SHARD (Content = shard digest) with the inner block's byte range.
func TestLocalLocator_InclusionFallthrough(t *testing.T) {
	ctx := context.Background()
	blockDigest := mustDigest(t, "manifest block")
	shardDigest := mustDigest(t, "segment car bytes")
	space, err := did.Parse(testProviderDID)
	require.NoError(t, err, "parse space")
	const url = "http://piri:80/blob/zshard"

	store := fakeLocStore{
		locs: map[string]*BlobLocation{
			string(shardDigest): {Space: space, Digest: shardDigest, Provider: testProviderDID, URL: url, Size: 131033},
		},
		incs: map[string]*BlobInclusion{
			string(blockDigest): {Space: space, Digest: blockDigest, ShardDigest: shardDigest, RangeStart: 52114, RangeEnd: 52420},
		},
	}
	ll := NewLocalLocator(store, store)

	locs, err := ll.Locate(ctx, []did.DID{space}, blockDigest)
	require.NoError(t, err, "Locate")
	require.Len(t, locs, 1)
	cm := locs[0].Commitment
	assert.Equal(t, string(shardDigest), string(cm.Content), "Content = %x, want shard digest %x (retrieve reads the shard, ranged)", cm.Content, shardDigest)
	assert.True(t, len(cm.Location) == 1 && cm.Location[0].URL().String() == url, "Location = %v, want [%s]", cm.Location, url)
	assert.Equal(t, int64(52114), locs[0].Range.Start, "Range.Start")
	assert.Equal(t, int64(52420), locs[0].Range.End, "Range.End")
}

// An inclusion whose shard has no location row is a broken write-path
// invariant, not absent data: it must surface as a hard error, never as
// NotFound (which would read as "block was never stored").
func TestLocalLocator_InclusionWithoutShardLocation(t *testing.T) {
	blockDigest := mustDigest(t, "orphaned block")
	shardDigest := mustDigest(t, "unrecorded shard")
	space, _ := did.Parse(testProviderDID)

	store := fakeLocStore{
		incs: map[string]*BlobInclusion{
			string(blockDigest): {Space: space, Digest: blockDigest, ShardDigest: shardDigest, RangeStart: 0, RangeEnd: 10},
		},
	}
	ll := NewLocalLocator(store, store)

	_, err := ll.Locate(context.Background(), []did.DID{space}, blockDigest)
	require.Error(t, err, "Locate succeeded, want invariant-violation error")
	var nf locator.NotFoundError
	require.False(t, errors.As(err, &nf), "err = %v, want a non-NotFound error", err)
}

func TestLocalLocator_NotFound(t *testing.T) {
	space, _ := did.Parse(testProviderDID)
	digest := mustDigest(t, "absent")
	ll := NewLocalLocator(fakeLocStore{}, fakeLocStore{})

	_, err := ll.Locate(context.Background(), []did.DID{space}, digest)
	var nf locator.NotFoundError
	require.ErrorAs(t, err, &nf, "err = %v, want locator.NotFoundError", err)
}
