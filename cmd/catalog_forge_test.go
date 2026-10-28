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
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// TestSignedList: the listing verifies under the key's secret and Hilt's
// time bounds, and Hilt reads it as needing s3:ListBucket on the bucket.
func TestSignedList(t *testing.T) {
	key := accessKey{id: "z6MkTestAccessKey", secret: "test-secret"}
	req, err := signedList("bk", key, "us-west-1", time.Now())
	if err != nil {
		t.Fatalf("signedList: %v", err)
	}
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, URL: req.URL, Headers: req.Headers})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sr.AccessKeyID != key.id || len(sr.Regions) != 1 || sr.Regions[0] != "us-west-1" {
		t.Fatalf("signed as %s in %v, want %s in us-west-1", sr.AccessKeyID, sr.Regions, key.id)
	}
	if err := sigv4.Verify(sr, key.secret); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := sigv4.Verify(sr, "another-secret"); err == nil {
		t.Fatal("verified under another secret")
	}
	if err := sigv4.ValidateTimeBounds(sr, time.Now()); err != nil {
		t.Fatalf("time bounds: %v", err)
	}
	_, reqs, err := hiltauth.RequirementsFor(req)
	if err != nil {
		t.Fatalf("requirements: %v", err)
	}
	if len(reqs) != 1 || reqs[0].Bucket != "bk" || reqs[0].Permission != "s3:ListBucket" {
		t.Fatalf("requirements = %+v, want s3:ListBucket on bk", reqs)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	mem := inmem.NewMemStore()
	blobs := &fakeBlobs{cars: map[string][]byte{}}
	shard := func(blocks ...block.Block) mh.Multihash {
		var buf bytes.Buffer
		if err := cars.Write(&buf, []cid.Cid{blocks[0].Cid()}, blocks); err != nil {
			t.Fatal(err)
		}
		digest, err := mh.Sum(buf.Bytes(), mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		blobs.cars[string(digest)] = buf.Bytes()
		var incs []registry.BlobInclusion
		for _, b := range blocks {
			incs = append(incs, registry.BlobInclusion{Space: space, Digest: b.Cid().Hash(), ShardDigest: digest})
		}
		if err := mem.PutInclusions(ctx, incs); err != nil {
			t.Fatal(err)
		}
		return digest
	}
	a, b, c := cborBlock(t, "a"), cborBlock(t, "b"), cborBlock(t, "c")
	s1, s2 := shard(a, b), shard(c)
	unshipped := cborBlock(t, "unshipped")

	authz := &fakeSignedAuthorizer{store: iam.NewDelegationCache(nil)}
	f := &forgeSegments{authz: authz, blobs: blobs, inclusions: mem,
		key: accessKey{id: "z6MkTestAccessKey", secret: "test-secret"}, region: "us-west-1", logger: zaptest.NewLogger(t)}
	fetch := f.forBucket("bk", space)

	got := func(k cid.Cid) []block.Block {
		t.Helper()
		blocks, err := fetch(ctx, k)
		if err != nil {
			t.Fatalf("fetch %s: %v", k, err)
		}
		return blocks
	}
	if blocks := got(unshipped.Cid()); len(blocks) != 0 || authz.calls != 0 {
		t.Fatalf("a block no segment shipped: %d blocks after %d authorizations, want none", len(blocks), authz.calls)
	}
	if blocks := got(a.Cid()); len(blocks) != 2 || blocks[0].Cid() != a.Cid() || blocks[1].Cid() != b.Cid() {
		t.Fatalf("a's segment: %v, want a and b", blocks)
	}
	if blocks := got(b.Cid()); len(blocks) != 0 {
		t.Fatalf("b, whose segment was read: %d blocks, want none", len(blocks))
	}
	if blocks := got(c.Cid()); len(blocks) != 1 || blocks[0].Cid() != c.Cid() {
		t.Fatalf("c's segment: %v, want c", blocks)
	}
	if authz.calls != 1 {
		t.Fatalf("%d authorizations, want one for the bucket", authz.calls)
	}
	if len(blobs.opened) != 2 || !bytes.Equal(blobs.opened[0], s1) || !bytes.Equal(blobs.opened[1], s2) {
		t.Fatalf("read segments %v, want %v then %v", blobs.opened, s1, s2)
	}
	for i, st := range blobs.stores {
		if st != any(authz.store) {
			t.Fatalf("read %d carried proof store %v, want the one Hilt granted", i, st)
		}
	}

	refused := &fakeSignedAuthorizer{err: iam.ErrBucketNotPermitted}
	f.authz = refused
	fetch = f.forBucket("other", space)
	for range 2 {
		if blocks := got(a.Cid()); len(blocks) != 0 {
			t.Fatalf("a bucket the key may not list: %d blocks, want none", len(blocks))
		}
	}
	if refused.calls != 1 || len(blobs.opened) != 2 {
		t.Fatalf("after a refusal: %d authorizations and %d reads, want one and no new read", refused.calls, len(blobs.opened))
	}

	down := errors.New("hilt is down")
	f.authz = &fakeSignedAuthorizer{err: down}
	if _, err := f.forBucket("bk", space)(ctx, a.Cid()); !errors.Is(err, down) {
		t.Fatalf("a failed authorization: %v, want %v", err, down)
	}
}
