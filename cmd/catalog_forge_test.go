package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// TestSignedList: the listing verifies under the key's secret and Hilt's
// time bounds, with either clock ahead by less than the skew Hilt allows,
// and Hilt reads it as needing s3:ListBucket on the bucket.
func TestSignedList(t *testing.T) {
	key := accessKey{id: "z6MkTestAccessKey", secret: "test-secret"}
	req, err := signedList("bk", key, "us-west-1", time.Now())
	require.NoError(t, err, "signedList")
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, URL: req.URL, Headers: req.Headers})
	require.NoError(t, err, "parse")
	require.Equal(t, key.id, sr.AccessKeyID, "signed as %s in %v, want %s in us-west-1", sr.AccessKeyID, sr.Regions, key.id)
	require.Len(t, sr.Regions, 1, "signed as %s in %v, want %s in us-west-1", sr.AccessKeyID, sr.Regions, key.id)
	require.Equal(t, "us-west-1", sr.Regions[0], "signed as %s in %v, want %s in us-west-1", sr.AccessKeyID, sr.Regions, key.id)
	require.NoError(t, sigv4.Verify(sr, key.secret), "verify")
	require.Error(t, sigv4.Verify(sr, "another-secret"), "verified under another secret")
	for _, offset := range []time.Duration{0, sigv4.MaxClockSkew - time.Minute, -(sigv4.MaxClockSkew - time.Minute)} {
		require.NoError(t, sigv4.ValidateTimeBounds(sr, time.Now().Add(offset)), "time bounds with Hilt's clock %v off", offset)
	}
	_, reqs, err := hiltauth.RequirementsFor(req)
	require.NoError(t, err, "requirements")
	require.Len(t, reqs, 1, "requirements = %+v, want s3:ListBucket on bk", reqs)
	require.Equal(t, "bk", reqs[0].Bucket, "requirements = %+v, want s3:ListBucket on bk", reqs)
	require.Equal(t, "s3:ListBucket", reqs[0].Permission, "requirements = %+v, want s3:ListBucket on bk", reqs)
}

type fakeSignedAuthorizer struct {
	store *iam.DelegationCache
	err   error
	calls int
}

func (f *fakeSignedAuthorizer) AuthorizeSigned(_ context.Context, req s3.Request, accessKey string) (*iam.DelegationCache, error) {
	f.calls++
	return f.store, f.err
}

// fakeBlobs serves CARs by digest, and records each read with the proof
// store its context carried.
type fakeBlobs struct {
	cars   map[string][]byte
	opened []mh.Multihash
	stores []any
}

func (f *fakeBlobs) OpenBlob(ctx context.Context, _ did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	f.opened = append(f.opened, digest)
	store, _ := reqscope.ProofStore(ctx)
	f.stores = append(f.stores, store)
	return io.NopCloser(bytes.NewReader(f.cars[string(digest)])), nil
}

// TestForgeSegments: a block's shipped segment is read whole, once, with the
// authority Hilt granted on the walk's first fetch; a block with no shipped
// segment costs no call; a bucket the key may not list is fetched from no
// further; any other authorize failure fails the fetch.
func TestForgeSegments(t *testing.T) {
	ctx := context.Background()
	space, err := did.Parse("did:web:forge-segments.example")
	require.NoError(t, err)
	mem := registry.NewPostgres(pgtest.New(t))
	blobs := &fakeBlobs{cars: map[string][]byte{}}
	shard := func(blocks ...block.Block) mh.Multihash {
		var buf bytes.Buffer
		require.NoError(t, cars.Write(&buf, []cid.Cid{blocks[0].Cid()}, blocks))
		digest, err := mh.Sum(buf.Bytes(), mh.SHA2_256, -1)
		require.NoError(t, err)
		blobs.cars[string(digest)] = buf.Bytes()
		var incs []registry.BlobInclusion
		for _, b := range blocks {
			incs = append(incs, registry.BlobInclusion{Space: space, Digest: b.Cid().Hash(), ShardDigest: digest})
		}
		require.NoError(t, mem.PutInclusions(ctx, incs))
		return digest
	}
	a, b, c := cborBlock(t, "a"), cborBlock(t, "b"), cborBlock(t, "c")
	s1, s2 := shard(a, b), shard(c)
	unshipped := cborBlock(t, "unshipped")
	d := cborBlock(t, "d")
	s3 := shard(d)
	// The last frame cut off whole: the bytes still parse, minus d.
	whole := blobs.cars[string(s3)]
	blobs.cars[string(s3)] = whole[:len(whole)-int(frame(d))]

	authz := &fakeSignedAuthorizer{store: iam.NewDelegationCache(nil)}
	f := &forgeSegments{authz: authz, blobs: blobs, inclusions: mem,
		key: accessKey{id: "z6MkTestAccessKey", secret: "test-secret"}, region: "us-west-1", logger: zaptest.NewLogger(t)}
	fetch := f.forBucket("bk", space)

	got := func(k cid.Cid) []block.Block {
		t.Helper()
		blocks, err := fetch(ctx, k)
		require.NoError(t, err, "fetch %s", k)
		return blocks
	}
	blocks := got(unshipped.Cid())
	require.Empty(t, blocks, "a block no segment shipped")
	require.Zero(t, authz.calls, "authorizations for a block no segment shipped")
	blocks = got(a.Cid())
	require.Len(t, blocks, 2, "a's segment")
	require.Equal(t, a.Cid(), blocks[0].Cid(), "a's segment")
	require.Equal(t, b.Cid(), blocks[1].Cid(), "a's segment")
	require.Empty(t, got(b.Cid()), "b, whose segment was read")
	blocks = got(c.Cid())
	require.Len(t, blocks, 1, "c's segment")
	require.Equal(t, c.Cid(), blocks[0].Cid(), "c's segment")
	require.Equal(t, 1, authz.calls, "want one authorization for the bucket")
	require.Len(t, blobs.opened, 2, "read segments %v", blobs.opened)
	require.True(t, bytes.Equal(blobs.opened[0], s1), "read segments %v, want %v then %v", blobs.opened, s1, s2)
	require.True(t, bytes.Equal(blobs.opened[1], s2), "read segments %v, want %v then %v", blobs.opened, s1, s2)
	for i, st := range blobs.stores {
		require.True(t, st == any(authz.store), "read %d carried proof store %v, want the one Hilt granted", i, st)
	}

	refused := &fakeSignedAuthorizer{err: iam.ErrBucketNotPermitted}
	f.authz = refused
	fetch = f.forBucket("other", space)
	for range 2 {
		blocks := got(a.Cid())
		require.Empty(t, blocks, "a bucket the key may not list: %d blocks", len(blocks))
	}
	require.Equal(t, 1, refused.calls, "after a refusal: %d authorizations and %d reads, want one and no new read", refused.calls, len(blobs.opened))
	require.Len(t, blobs.opened, 2, "after a refusal: %d authorizations and %d reads, want one and no new read", refused.calls, len(blobs.opened))

	f.authz = &fakeSignedAuthorizer{store: iam.NewDelegationCache(nil)}
	_, err = f.forBucket("bk", space)(ctx, d.Cid())
	require.ErrorContains(t, err, "does not match its digest", "a segment short of its last frame")

	down := errors.New("hilt is down")
	f.authz = &fakeSignedAuthorizer{err: down}
	_, err = f.forBucket("bk", space)(ctx, a.Cid())
	require.ErrorIs(t, err, down, "a failed authorization")
}
