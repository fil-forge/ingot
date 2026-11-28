package bucket

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "multihash")
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

// roundTrip builds refs into a /1 manifest with lists of the given sizes, encodes
// it as a value block, decodes it, and returns the decoded body and the block
// store holding its extents.
func roundTrip(t *testing.T, refs BlobRefs, sizes BlobListSizes) (Body, *memBlocks, int) {
	t.Helper()
	ctx := context.Background()
	store := newMemBlocks()
	list, err := BuildBlobList(ctx, store, refs, sizes)
	require.NoError(t, err, "BuildBlobList")
	mf := &ObjectManifest{Key: "k", Body: Body{Size: refs.Size(), Blobs: list}}
	var buf bytes.Buffer
	require.NoError(t, ManifestValue(mf).MarshalCBOR(&buf), "marshal")
	var em EnvelopedManifest
	require.NoError(t, em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	return em.Manifest.Body, store, buf.Len()
}

// assertSameBlobs checks every blob of idx against want, by Locate at each
// blob's first and last byte, and by Digests.
func assertSameBlobs(t *testing.T, idx BlobIndex, want BlobRefs) {
	t.Helper()
	ctx := context.Background()
	require.Equal(t, want.Size(), idx.Size(), "Size")
	for _, w := range want {
		for _, off := range []int64{w.Start, w.End} {
			got, err := idx.Locate(ctx, off)
			require.NoError(t, err, "Locate(%d)", off)
			require.Equal(t, w.Start, got.Start, "Locate(%d) = [%d,%d]", off, got.Start, got.End)
			require.Equal(t, w.End, got.End, "Locate(%d) = [%d,%d]", off, got.Start, got.End)
			require.True(t, bytes.Equal(got.Digest, w.Digest), "Locate(%d) = [%d,%d], want [%d,%d]", off, got.Start, got.End, w.Start, w.End)
		}
	}
	digests, err := idx.Digests(ctx)
	require.NoError(t, err, "Digests")
	require.Len(t, digests, len(want), "Digests")
	for i, d := range digests {
		require.True(t, bytes.Equal(d, want[i].Digest), "digest %d differs", i)
	}
}

// TestBlobList_RoundTrip writes bodies of every shape through a /1 manifest
// and reads them back: inline only, one entry past the head, several
// extents, and a 10,000 x 5 GiB-shaped body.
func TestBlobList_RoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 1024, 1025, 2048, 2049} {
		t.Run(fmt.Sprintf("%d blobs", n), func(t *testing.T) {
			refs := uniformRefs(t, n, 1000)
			body, store, _ := roundTrip(t, refs, BlobListSizes{})
			wantExtents := 0
			if n > 1024 {
				wantExtents = (n - 1024 + 1023) / 1024
			}
			require.Len(t, body.Blobs.Head, min(n, 1024), "head")
			require.Len(t, body.Blobs.Extents, wantExtents, "extents")
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
	list, err := BuildBlobList(ctx, store, refs, BlobListSizes{})
	require.NoError(t, err, "BuildBlobList")
	mf := &ObjectManifest{Key: "k", Body: Body{
		Size: refs.Size(), Blobs: list, PartSizes: partSizes, PartChecksums: partChecksums,
		TreeBlocks: make([]byte, 32768*32),
	}}
	var buf bytes.Buffer
	require.NoError(t, ManifestValue(mf).MarshalCBOR(&buf), "marshal")
	var em EnvelopedManifest
	require.NoError(t, em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	require.Len(t, em.Manifest.Body.PartSizes, parts, "PartSizes")
	require.Len(t, em.Manifest.Body.PartChecksums, parts, "PartChecksums")
	largest := 0
	for _, raw := range store.blocks {
		largest = max(largest, len(raw))
	}
	t.Logf("manifest %d bytes (TreeBlocks 1 MiB of it), %d extents, largest extent %d bytes",
		buf.Len(), len(list.Extents), largest)
	require.LessOrEqual(t, largest, 64<<10, "largest extent %d bytes", largest)
	assertSameBlobs(t, em.Manifest.Body.Index(store, did.Undef), refs)
}

// TestBlobIndex_LocateLoadsAtMostOneExtent: random bodies, random offsets,
// a fresh index each time; Locate must agree with the flat list and fetch at
// most one block.
func TestBlobIndex_LocateLoadsAtMostOneExtent(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	for trial := range 50 {
		lengths := make([]int64, 1+rng.Intn(40))
		for i := range lengths {
			lengths[i] = 1 + rng.Int63n(50)
		}
		refs := refsOf(t, lengths...)
		body, store, _ := roundTrip(t, refs, BlobListSizes{Head: 3, Extent: 4})
		for range 20 {
			off := rng.Int63n(refs.Size())
			want, _ := refs.Locate(ctx, off)
			store.gets = 0
			got, err := body.Index(store, did.Undef).Locate(ctx, off)
			require.NoError(t, err, "trial %d: Locate(%d)", trial, off)
			require.Equal(t, want.Start, got.Start, "trial %d: Locate(%d) = [%d,%d]", trial, off, got.Start, got.End)
			require.Equal(t, want.End, got.End, "trial %d: Locate(%d) = [%d,%d]", trial, off, got.Start, got.End)
			require.True(t, bytes.Equal(got.Digest, want.Digest), "trial %d: Locate(%d) = [%d,%d], want [%d,%d]", trial, off, got.Start, got.End, want.Start, want.End)
			require.LessOrEqual(t, store.gets, 1, "trial %d: Locate(%d) loaded %d blocks", trial, off, store.gets)
		}
		assertSameBlobs(t, body.Index(store, did.Undef), refs)
	}
}

// TestBlobIndex_SequentialReadLoadsEachExtentOnce: walking the body in order
// fetches each extent block once.
func TestBlobIndex_SequentialReadLoadsEachExtentOnce(t *testing.T) {
	refs := uniformRefs(t, 11, 10) // head 2, extents of 3, 3, 3
	body, store, _ := roundTrip(t, refs, BlobListSizes{Head: 2, Extent: 3})
	idx := body.Index(store, did.Undef)
	ctx := context.Background()
	for off := int64(0); off < refs.Size(); off += 5 {
		_, err := idx.Locate(ctx, off)
		require.NoError(t, err, "Locate(%d)", off)
	}
	require.Equal(t, 3, store.gets, "loaded %d extent blocks", store.gets)
}

// TestBlobIndex_Extent returns the blobs stored with the one at an offset.
func TestBlobIndex_Extent(t *testing.T) {
	refs := uniformRefs(t, 7, 10) // head [0,2), extents [2,5), [5,7)
	body, store, _ := roundTrip(t, refs, BlobListSizes{Head: 2, Extent: 3})
	idx := body.Index(store, did.Undef)
	for off, want := range map[int64][2]int{0: {0, 2}, 19: {0, 2}, 20: {2, 5}, 49: {2, 5}, 50: {5, 7}, 69: {5, 7}} {
		got, err := idx.Extent(context.Background(), off)
		require.NoError(t, err, "Extent(%d)", off)
		require.Len(t, got, want[1]-want[0], "Extent(%d)", off)
		require.Equal(t, refs[want[0]].Start, got[0].Start, "Extent(%d)", off)
	}
	_, err := idx.Extent(context.Background(), 70)
	require.Error(t, err, "Extent past the end: want an error")
}

// TestBuildBlobList_Deterministic: the same refs give the same list and CIDs.
func TestBuildBlobList_Deterministic(t *testing.T) {
	refs := uniformRefs(t, 9, 7)
	sizes := BlobListSizes{Head: 2, Extent: 2}
	ctx := context.Background()
	a, err := BuildBlobList(ctx, newMemBlocks(), refs, sizes)
	require.NoError(t, err, "BuildBlobList")
	b, err := BuildBlobList(ctx, newMemBlocks(), refs, sizes)
	require.NoError(t, err, "BuildBlobList")
	require.Len(t, a.Extents, 4, "extents")
	require.Equal(t, a.Extents, b.Extents, "extents")
}

// TestBuildBlobList_Rejects a list over capacity, sizes a list cannot
// encode, a gap, an empty span and a missing digest.
func TestBuildBlobList_Rejects(t *testing.T) {
	ctx := context.Background()
	t.Run("over capacity", func(t *testing.T) {
		sizes := BlobListSizes{Head: 1, Extent: 1}
		_, err := BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, sizes.MaxBlobs()+1, 1), sizes)
		require.ErrorContains(t, err, "at most")
		_, err = BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, sizes.MaxBlobs(), 1), sizes)
		require.NoError(t, err, "at capacity")
	})
	for name, sizes := range map[string]BlobListSizes{
		"negative head":   {Head: -1},
		"negative extent": {Extent: -1},
		"head too long":   {Head: 8193},
		"extent too long": {Extent: 8193},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, 1, 1), sizes)
			require.Error(t, err, "want an error")
		})
	}
	t.Run("longest lists", func(t *testing.T) {
		sizes := BlobListSizes{Head: 8192, Extent: 8192}
		_, err := BuildBlobList(ctx, newMemBlocks(), uniformRefs(t, 8192+8192, 1), sizes)
		require.NoError(t, err, "lists of 8,192")
	})
	for name, refs := range map[string]BlobRefs{
		"gap":           {{Digest: blobDigest(t, 0), Start: 0, End: 9}, {Digest: blobDigest(t, 1), Start: 11, End: 20}},
		"not from zero": {{Digest: blobDigest(t, 0), Start: 1, End: 9}},
		"empty span":    {{Digest: blobDigest(t, 0), Start: 0, End: -1}},
		"no digest":     {{Start: 0, End: 9}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildBlobList(ctx, newMemBlocks(), refs, BlobListSizes{})
			require.Error(t, err, "want an error")
		})
	}
}

// TestMaxBodyBlobs is the capacity at the default sizes.
func TestMaxBodyBlobs(t *testing.T) {
	got := (BlobListSizes{}).MaxBlobs()
	require.Equal(t, MaxBodyBlobs, got, "MaxBodyBlobs = %d, capacity at the default sizes %d", MaxBodyBlobs, got)
}

// TestBlobIndex_RejectsMalformedLists: every shape the writer never
// produces is a read error, never a wrong blob.
func TestBlobIndex_RejectsMalformedLists(t *testing.T) {
	ctx := context.Background()
	store := newMemBlocks()
	d := func(i int) mh.Multihash { return blobDigest(t, i) }
	extent := func(entries ...BlobEntry) cid.Cid {
		c, err := store.Put(ctx, &EnvelopedBlobExtent{Extent: &BlobExtent{Entries: entries}})
		require.NoError(t, err, "put extent")
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
			_, err := body.Index(store, did.Undef).Locate(ctx, c.off)
			require.Error(t, err, "Locate(%d): want an error", c.off)
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
	require.NoError(t, (&ValueUnion{ManifestV0: v0}).MarshalCBOR(&buf), "marshal /0")
	require.True(t, bytes.Contains(buf.Bytes(), []byte("/objectmanifest/0")), "block does not carry the /0 key")
	var val ObjectValue
	require.NoError(t, val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	require.Nil(t, val.ManifestV0, "decoded arms")
	require.NotNil(t, val.Manifest, "decoded arms")
	mf := val.Manifest
	require.Equal(t, "k", mf.Key, "decoded manifest = %+v", mf)
	require.Equal(t, "abc-3", mf.ETag, "decoded manifest = %+v", mf)
	require.Equal(t, uint64(4), mf.Seq, "decoded manifest = %+v", mf)
	require.Equal(t, "COMPOSITE", mf.ChecksumType, "decoded manifest = %+v", mf)
	require.Equal(t, "no-cache", mf.CacheControl, "decoded manifest = %+v", mf)
	require.Equal(t, "b", mf.Metadata["a"], "decoded manifest = %+v", mf)
	require.True(t, mf.Plaintext, "decoded manifest = %+v", mf)
	require.Equal(t, uint8(4), mf.Body.TreeChunkLog, "decoded manifest = %+v", mf)
	require.Len(t, mf.Body.PartChecksums, 2, "decoded manifest = %+v", mf)
	require.Equal(t, int64(50), mf.Body.PartSizes[1], "decoded manifest = %+v", mf)
	require.Empty(t, mf.Body.Blobs.Extents, "a /0 list has %d extents", len(mf.Body.Blobs.Extents))
	assertSameBlobs(t, mf.Body.Index(newMemBlocks(), did.Undef), refs)

	var em EnvelopedManifest
	err := em.UnmarshalCBOR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "EnvelopedManifest over /0")
	require.Equal(t, "k", em.Manifest.Key, "EnvelopedManifest over /0")
}

// TestManifestV0_NotWritten: the /0 arm is decode-only.
func TestManifestV0_NotWritten(t *testing.T) {
	val := &ObjectValue{ValueUnion{ManifestV0: &ObjectManifestV0{Key: "k"}}}
	require.Error(t, val.MarshalCBOR(&bytes.Buffer{}), "marshal of a /0 value: want an error")
}

// TestBlobExtent_UnionKeys: an extent block carries its own key and decodes
// only as an extent.
func TestBlobExtent_UnionKeys(t *testing.T) {
	var ext bytes.Buffer
	require.NoError(t, (&EnvelopedBlobExtent{Extent: &BlobExtent{Entries: []BlobEntry{{blobDigest(t, 0), 0}}}}).MarshalCBOR(&ext), "marshal extent")
	require.True(t, bytes.Contains(ext.Bytes(), []byte("/blobextent/0")), "extent block does not carry its union key")
	var val ObjectValue
	require.Error(t, val.UnmarshalCBOR(bytes.NewReader(ext.Bytes())), "extent decoded as a value block")

	var mf bytes.Buffer
	require.NoError(t, ManifestValue(&ObjectManifest{Key: "k"}).MarshalCBOR(&mf), "marshal manifest")
	var env EnvelopedBlobExtent
	require.Error(t, env.UnmarshalCBOR(bytes.NewReader(mf.Bytes())), "manifest decoded as an extent")
	require.Error(t, (&EnvelopedBlobExtent{}).MarshalCBOR(&bytes.Buffer{}), "marshal of an empty extent envelope: want an error")
}
