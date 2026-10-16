package bucket

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// memBlocks is a CBOR block store over a map, counting the blocks read.
type memBlocks struct {
	blocks map[cid.Cid][]byte
	gets   int
}

func newMemBlocks() *memBlocks { return &memBlocks{blocks: map[cid.Cid][]byte{}} }

func (m *memBlocks) Put(_ context.Context, v any) (cid.Cid, error) {
	var buf bytes.Buffer
	if err := v.(cbg.CBORMarshaler).MarshalCBOR(&buf); err != nil {
		return cid.Undef, err
	}
	h, err := mh.Sum(buf.Bytes(), mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	c := cid.NewCidV1(cid.DagCBOR, h)
	m.blocks[c] = buf.Bytes()
	return c, nil
}

func (m *memBlocks) Get(_ context.Context, _ did.DID, c cid.Cid, out any) error {
	raw, ok := m.blocks[c]
	if !ok {
		return fmt.Errorf("block %s not found", c)
	}
	m.gets++
	return out.(cbg.CBORUnmarshaler).UnmarshalCBOR(bytes.NewReader(raw))
}

// blobDigest is a distinct sha256 multihash per i.
func blobDigest(t testing.TB, i int) mh.Multihash {
	t.Helper()
	d, err := mh.Sum([]byte(fmt.Sprintf("blob %d", i)), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return d
}

// refsOf builds a contiguous list of blobs with the given lengths.
func refsOf(t testing.TB, lengths ...int64) BlobRefs {
	t.Helper()
	var r BlobRefs
	for i, n := range lengths {
		r.Append(blobDigest(t, i), n)
	}
	return r
}

// uniformRefs is n blobs of size bytes each.
func uniformRefs(t testing.TB, n int, size int64) BlobRefs {
	t.Helper()
	lengths := make([]int64, n)
	for i := range lengths {
		lengths[i] = size
	}
	return refsOf(t, lengths...)
}

// withShape shrinks the writer constants for one test.
func withShape(t *testing.T, head, extent int) {
	t.Helper()
	oldHead, oldExtent := headEntries, extentEntries
	headEntries, extentEntries = head, extent
	t.Cleanup(func() { headEntries, extentEntries = oldHead, oldExtent })
}

// roundTrip builds refs into a /1 manifest, encodes it as a value block,
// decodes it, and returns the decoded body and the block store holding its
// extents.
func roundTrip(t *testing.T, refs BlobRefs) (Body, *memBlocks, int) {
	t.Helper()
	ctx := context.Background()
	store := newMemBlocks()
	list, err := BuildBlobList(ctx, store, refs)
	if err != nil {
		t.Fatalf("BuildBlobList: %v", err)
	}
	mf := &ObjectManifest{Key: "k", Body: Body{Size: refs.Size(), Blobs: list}}
	var buf bytes.Buffer
	if err := ManifestValue(mf).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var em EnvelopedManifest
	if err := em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return em.Manifest.Body, store, buf.Len()
}

// assertSameBlobs checks every blob of idx against want, by Locate at each
// blob's first and last byte, and by Digests.
func assertSameBlobs(t *testing.T, idx BlobIndex, want BlobRefs) {
	t.Helper()
	ctx := context.Background()
	if idx.Size() != want.Size() {
		t.Fatalf("Size = %d, want %d", idx.Size(), want.Size())
	}
	for _, w := range want {
		for _, off := range []int64{w.Start, w.End} {
			got, err := idx.Locate(ctx, off)
			if err != nil {
				t.Fatalf("Locate(%d): %v", off, err)
			}
			if got.Start != w.Start || got.End != w.End || !bytes.Equal(got.Digest, w.Digest) {
				t.Fatalf("Locate(%d) = [%d,%d], want [%d,%d]", off, got.Start, got.End, w.Start, w.End)
			}
		}
	}
	digests, err := idx.Digests(ctx)
	if err != nil {
		t.Fatalf("Digests: %v", err)
	}
	if len(digests) != len(want) {
		t.Fatalf("Digests returned %d, want %d", len(digests), len(want))
	}
	for i, d := range digests {
		if !bytes.Equal(d, want[i].Digest) {
			t.Fatalf("digest %d differs", i)
		}
	}
}

// TestBlobList_RoundTrip writes bodies of every shape through a /1 manifest
// and reads them back: inline only, one entry past the head, several
// extents, and a 10,000 x 5 GiB-shaped body.
func TestBlobList_RoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 1024, 1025, 2048, 2049} {
		t.Run(fmt.Sprintf("%d blobs", n), func(t *testing.T) {
			refs := uniformRefs(t, n, 1000)
			body, store, _ := roundTrip(t, refs)
			wantExtents := 0
			if n > 1024 {
				wantExtents = (n - 1024 + 1023) / 1024
			}
			if len(body.Blobs.Head) != min(n, 1024) || len(body.Blobs.Extents) != wantExtents {
				t.Fatalf("head %d, extents %d; want %d, %d", len(body.Blobs.Head), len(body.Blobs.Extents), min(n, 1024), wantExtents)
			}
			assertSameBlobs(t, body.Index(store, did.Undef), refs)
		})
	}
}

// TestBlobList_LargestUpload is the shape a 10,000-part upload of 5 GiB
// parts takes at the default blob size: 21 blobs a part, 210,000 in all,
// with checksums and sizes for every part. Today's inline list cannot be
// encoded at this size.
func TestBlobList_LargestUpload(t *testing.T) {
	const parts, perPart = 10000, 21
	var refs BlobRefs
	partSizes := make([]int64, parts)
	partChecksums := make([]string, parts)
	for p := range parts {
		for b := range perPart {
			n := DefaultMaxBlobSize
			if b == perPart-1 {
				n = 5<<30 - (perPart-1)*DefaultMaxBlobSize
			}
			refs.Append(blobDigest(t, p*perPart+b), n)
		}
		partSizes[p] = 5 << 30
		partChecksums[p] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	}
	ctx := context.Background()
	store := newMemBlocks()
	list, err := BuildBlobList(ctx, store, refs)
	if err != nil {
		t.Fatalf("BuildBlobList: %v", err)
	}
	mf := &ObjectManifest{Key: "k", Body: Body{
		Size: refs.Size(), Blobs: list, PartSizes: partSizes, PartChecksums: partChecksums,
		TreeBlocks: make([]byte, 32768*32),
	}}
	var buf bytes.Buffer
	if err := ManifestValue(mf).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var em EnvelopedManifest
	if err := em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := len(em.Manifest.Body.PartSizes); got != parts {
		t.Fatalf("PartSizes = %d, want %d", got, parts)
	}
	if got := len(em.Manifest.Body.PartChecksums); got != parts {
		t.Fatalf("PartChecksums = %d, want %d", got, parts)
	}
	largest := 0
	for _, raw := range store.blocks {
		largest = max(largest, len(raw))
	}
	t.Logf("manifest %d bytes (TreeBlocks 1 MiB of it), %d extents, largest extent %d bytes",
		buf.Len(), len(list.Extents), largest)
	if largest > 64<<10 {
		t.Fatalf("largest extent %d bytes, want under 64 KiB", largest)
	}
	assertSameBlobs(t, em.Manifest.Body.Index(store, did.Undef), refs)
}

// TestBlobIndex_LocateLoadsAtMostOneExtent: random bodies, random offsets,
// a fresh index each time; Locate must agree with the flat list and fetch at
// most one block.
func TestBlobIndex_LocateLoadsAtMostOneExtent(t *testing.T) {
	withShape(t, 3, 4)
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	for trial := range 50 {
		lengths := make([]int64, 1+rng.Intn(40))
		for i := range lengths {
			lengths[i] = 1 + rng.Int63n(50)
		}
		refs := refsOf(t, lengths...)
		body, store, _ := roundTrip(t, refs)
		for range 20 {
			off := rng.Int63n(refs.Size())
			want, _ := refs.Locate(ctx, off)
			store.gets = 0
			got, err := body.Index(store, did.Undef).Locate(ctx, off)
			if err != nil {
				t.Fatalf("trial %d: Locate(%d): %v", trial, off, err)
			}
			if got.Start != want.Start || got.End != want.End || !bytes.Equal(got.Digest, want.Digest) {
				t.Fatalf("trial %d: Locate(%d) = [%d,%d], want [%d,%d]", trial, off, got.Start, got.End, want.Start, want.End)
			}
			if store.gets > 1 {
				t.Fatalf("trial %d: Locate(%d) loaded %d blocks", trial, off, store.gets)
			}
		}
		assertSameBlobs(t, body.Index(store, did.Undef), refs)
	}
}

// TestBlobIndex_SequentialReadLoadsEachExtentOnce: walking the body in order
// fetches each extent block once.
func TestBlobIndex_SequentialReadLoadsEachExtentOnce(t *testing.T) {
	withShape(t, 2, 3)
	refs := uniformRefs(t, 11, 10) // head 2, extents of 3, 3, 3
	body, store, _ := roundTrip(t, refs)
	idx := body.Index(store, did.Undef)
	ctx := context.Background()
	for off := int64(0); off < refs.Size(); off += 5 {
		if _, err := idx.Locate(ctx, off); err != nil {
			t.Fatalf("Locate(%d): %v", off, err)
		}
	}
	if store.gets != 3 {
		t.Fatalf("loaded %d extent blocks, want 3", store.gets)
	}
}

// TestBlobIndex_Extent returns the blobs stored with the one at an offset.
func TestBlobIndex_Extent(t *testing.T) {
	withShape(t, 2, 3)
	refs := uniformRefs(t, 7, 10) // head [0,2), extents [2,5), [5,7)
	body, store, _ := roundTrip(t, refs)
	idx := body.Index(store, did.Undef)
	for off, want := range map[int64][2]int{0: {0, 2}, 19: {0, 2}, 20: {2, 5}, 49: {2, 5}, 50: {5, 7}, 69: {5, 7}} {
		got, err := idx.Extent(context.Background(), off)
		if err != nil {
			t.Fatalf("Extent(%d): %v", off, err)
		}
		if len(got) != want[1]-want[0] || got[0].Start != refs[want[0]].Start {
			t.Fatalf("Extent(%d) = %d blobs from %d, want blobs %d..%d", off, len(got), got[0].Start, want[0], want[1])
		}
	}
	if _, err := idx.Extent(context.Background(), 70); err == nil {
		t.Fatal("Extent past the end: want an error")
	}
}

// TestBuildBlobList_Deterministic: the same refs give the same list and CIDs.
func TestBuildBlobList_Deterministic(t *testing.T) {
	withShape(t, 2, 2)
	refs := uniformRefs(t, 9, 7)
	ctx := context.Background()
	a, err := BuildBlobList(ctx, newMemBlocks(), refs)
	if err != nil {
		t.Fatalf("BuildBlobList: %v", err)
	}
	b, err := BuildBlobList(ctx, newMemBlocks(), refs)
	if err != nil {
		t.Fatalf("BuildBlobList: %v", err)
	}
	if len(a.Extents) != 4 || len(a.Extents) != len(b.Extents) {
		t.Fatalf("extents %d and %d, want 4", len(a.Extents), len(b.Extents))
	}
	for i := range a.Extents {
		if a.Extents[i] != b.Extents[i] {
			t.Fatalf("extent %d differs: %v vs %v", i, a.Extents[i], b.Extents[i])
		}
	}
}

// TestBuildBlobList_Rejects a list over capacity, a gap, an empty span and a
// missing digest.
func TestBuildBlobList_Rejects(t *testing.T) {
	ctx := context.Background()
	t.Run("over capacity", func(t *testing.T) {
		withShape(t, 1, 1)
		_, err := BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, blobCapacity()+1, 1))
		if err == nil || !strings.Contains(err.Error(), "at most") {
			t.Fatalf("err = %v, want the capacity refusal", err)
		}
		if _, err := BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, blobCapacity(), 1)); err != nil {
			t.Fatalf("at capacity: %v", err)
		}
	})
	for name, refs := range map[string]BlobRefs{
		"gap":           {{Digest: blobDigest(t, 0), Start: 0, End: 9}, {Digest: blobDigest(t, 1), Start: 11, End: 20}},
		"not from zero": {{Digest: blobDigest(t, 0), Start: 1, End: 9}},
		"empty span":    {{Digest: blobDigest(t, 0), Start: 0, End: -1}},
		"no digest":     {{Start: 0, End: 9}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildBlobList(ctx, newMemBlocks(), refs); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

// TestMaxBodyBlobs is the capacity at the default writer constants.
func TestMaxBodyBlobs(t *testing.T) {
	if MaxBodyBlobs != blobCapacity() {
		t.Fatalf("MaxBodyBlobs = %d, capacity at the default shape %d", MaxBodyBlobs, blobCapacity())
	}
}

// TestBlobIndex_RejectsMalformedLists: every shape the writer never
// produces is a read error, never a wrong blob.
func TestBlobIndex_RejectsMalformedLists(t *testing.T) {
	ctx := context.Background()
	store := newMemBlocks()
	d := func(i int) mh.Multihash { return blobDigest(t, i) }
	extent := func(entries ...BlobEntry) cid.Cid {
		c, err := store.Put(ctx, &EnvelopedBlobExtent{Extent: &BlobExtent{Entries: entries}})
		if err != nil {
			t.Fatalf("put extent: %v", err)
		}
		return c
	}
	good := extent(BlobEntry{d(2), 20}, BlobEntry{d(3), 30})
	cases := map[string]struct {
		size int64
		list BlobList
		off  int64
	}{
		"head not at zero":       {40, BlobList{Head: []BlobEntry{{d(0), 5}}}, 10},
		"head not increasing":    {40, BlobList{Head: []BlobEntry{{d(0), 0}, {d(1), 0}}}, 0},
		"last blob past the end": {10, BlobList{Head: []BlobEntry{{d(0), 0}, {d(1), 10}}}, 0},
		"zero-byte with blobs":   {0, BlobList{Head: []BlobEntry{{d(0), 0}}}, 0},
		"bytes with no blobs":    {10, BlobList{}, 0},
		"no digest":              {10, BlobList{Head: []BlobEntry{{nil, 0}}}, 0},
		"extent ref mismatch":    {40, BlobList{Head: []BlobEntry{{d(0), 0}}, Extents: []ExtentRef{{25, good}}}, 30},
		"extent before head end": {40, BlobList{Head: []BlobEntry{{d(0), 0}, {d(1), 10}}, Extents: []ExtentRef{{10, good}}}, 30},
		"extents not increasing": {40, BlobList{Head: []BlobEntry{{d(0), 0}}, Extents: []ExtentRef{{20, good}, {20, good}}}, 30},
		"extent ref past size":   {20, BlobList{Head: []BlobEntry{{d(0), 0}}, Extents: []ExtentRef{{20, good}}}, 0},
		"empty extent":           {40, BlobList{Head: []BlobEntry{{d(0), 0}}, Extents: []ExtentRef{{20, extent()}}}, 30},
		"missing extent block":   {40, BlobList{Head: []BlobEntry{{d(0), 0}}, Extents: []ExtentRef{{20, testCid(t, "nowhere")}}}, 30},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			body := Body{Size: c.size, Blobs: c.list}
			if _, err := body.Index(store, did.Undef).Locate(ctx, c.off); err == nil {
				t.Fatalf("Locate(%d): want an error", c.off)
			}
		})
	}
}

// TestManifestV0_DecodesAsCurrent: a /0 block, whose list is inline with
// spans, decodes into an ObjectManifest whose Head carries the blobs, every
// other field intact.
func TestManifestV0_DecodesAsCurrent(t *testing.T) {
	refs := refsOf(t, 100, 100, 50)
	v0 := &ObjectManifestV0{
		Key: "k", ContentType: "text/plain", Created: 7, ETag: "abc-3", Seq: 4, VersionID: "v",
		ChecksumAlgorithm: "CRC32", Checksum: "ck", ChecksumType: "COMPOSITE",
		CacheControl: "no-cache", Metadata: map[string]string{"a": "b"}, Plaintext: true,
		Body: BodyV0{
			Size: refs.Size(), MD5: []byte{1}, Blobs: refs, BLAKE3: []byte{2}, TreeChunkLog: 4,
			TreeBlocks: []byte{3}, PartSizes: []int64{200, 50}, PartChecksums: []string{"x", "y"},
		},
	}
	var buf bytes.Buffer
	if err := (&ValueUnion{ManifestV0: v0}).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal /0: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("/objectmanifest/0")) {
		t.Fatal("block does not carry the /0 key")
	}
	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if val.ManifestV0 != nil || val.Manifest == nil {
		t.Fatalf("decoded arms: v0 %v, manifest %v; want only the manifest", val.ManifestV0 != nil, val.Manifest != nil)
	}
	mf := val.Manifest
	if mf.Key != "k" || mf.ETag != "abc-3" || mf.Seq != 4 || mf.ChecksumType != "COMPOSITE" ||
		mf.CacheControl != "no-cache" || mf.Metadata["a"] != "b" || !mf.Plaintext ||
		mf.Body.TreeChunkLog != 4 || len(mf.Body.PartChecksums) != 2 || mf.Body.PartSizes[1] != 50 {
		t.Fatalf("decoded manifest = %+v", mf)
	}
	if len(mf.Body.Blobs.Extents) != 0 {
		t.Fatalf("a /0 list has %d extents, want none", len(mf.Body.Blobs.Extents))
	}
	assertSameBlobs(t, mf.Body.Index(newMemBlocks(), did.Undef), refs)

	var em EnvelopedManifest
	if err := em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil || em.Manifest.Key != "k" {
		t.Fatalf("EnvelopedManifest over /0: %v", err)
	}
}

// TestManifestV0_NotWritten: the /0 arm is decode-only.
func TestManifestV0_NotWritten(t *testing.T) {
	val := &ObjectValue{ValueUnion{ManifestV0: &ObjectManifestV0{Key: "k"}}}
	if err := val.MarshalCBOR(&bytes.Buffer{}); err == nil {
		t.Fatal("marshal of a /0 value: want an error")
	}
}

// TestBlobExtent_UnionKeys: an extent block carries its own key and decodes
// only as an extent.
func TestBlobExtent_UnionKeys(t *testing.T) {
	var ext bytes.Buffer
	if err := (&EnvelopedBlobExtent{Extent: &BlobExtent{Entries: []BlobEntry{{blobDigest(t, 0), 0}}}}).MarshalCBOR(&ext); err != nil {
		t.Fatalf("marshal extent: %v", err)
	}
	if !bytes.Contains(ext.Bytes(), []byte("/blobextent/0")) {
		t.Fatal("extent block does not carry its union key")
	}
	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(ext.Bytes())); err == nil {
		t.Fatal("extent decoded as a value block")
	}

	var mf bytes.Buffer
	if err := ManifestValue(&ObjectManifest{Key: "k"}).MarshalCBOR(&mf); err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	var env EnvelopedBlobExtent
	if err := env.UnmarshalCBOR(bytes.NewReader(mf.Bytes())); err == nil {
		t.Fatal("manifest decoded as an extent")
	}
	if err := (&EnvelopedBlobExtent{}).MarshalCBOR(&bytes.Buffer{}); err == nil {
		t.Fatal("marshal of an empty extent envelope: want an error")
	}
}
