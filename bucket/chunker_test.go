package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/filecoin-project/go-fee/aesstream"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

// TestDefaultMaxBlobSizeFitsPiri pins DefaultMaxBlobSize's derivation: the
// FEE envelope of a max-size plaintext blob (ciphertext with per-chunk GCM
// tags, plus a generous COSE-header budget — a single-recipient header is a
// few hundred bytes) must fit the network blob ceiling (libforge
// blob.MaxBlobSize, the most raw bytes a default-configured piri accepts).
// Guards against the formula drifting (e.g. a larger header, or a second
// recipient).
func TestDefaultMaxBlobSizeFitsPiri(t *testing.T) {
	const headerBudget = 1024
	enc := aesstream.EncryptedSize(DefaultMaxBlobSize, aesstream.DefaultChunkSize) + headerBudget
	require.LessOrEqual(t, enc, int64(blobcmds.MaxBlobSize), "DefaultMaxBlobSize %d: envelope %d exceeds the network blob ceiling %d", DefaultMaxBlobSize, enc, int64(blobcmds.MaxBlobSize))
	// And the allowance is not vacuous: a split at a round 256 MiB — over
	// the ceiling by less than the allowance — must NOT fit once framed.
	require.Greater(t, aesstream.EncryptedSize(256<<20, aesstream.DefaultChunkSize), int64(blobcmds.MaxBlobSize),
		"a 256 MiB blob's envelope fits the ceiling: revisit DefaultMaxBlobSize")
}

// memBlobs is a blob store in memory: a blockstore.BlobWriter that keys each
// blob by its sha256 digest, and the BlobReader that reads it back.
type memBlobs struct{ blobs map[string][]byte }

func testBlobs(*testing.T) *memBlobs { return &memBlobs{blobs: map[string][]byte{}} }

func (m *memBlobs) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil || len(data) == 0 {
		return nil, int64(len(data)), err
	}
	sum := sha256.Sum256(data)
	digest, err := mh.Encode(sum[:], mh.SHA2_256)
	if err != nil {
		return nil, 0, err
	}
	m.blobs[string(digest)] = data
	return digest, int64(len(data)), nil
}

func (m *memBlobs) OpenBlob(_ context.Context, _ did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	data, ok := m.blobs[string(digest)]
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func makeData(n int) []byte {
	d := make([]byte, n)
	for i := range d {
		d[i] = byte(i*31 + 7)
	}
	return d
}

// TestSplitBody_StreamingRoundTrip splits a multi-blob body through a blob store and
// reads it back, asserting the blob boundaries, the whole-body digests, and a
// byte-exact round trip — all via the streaming WriteBlob/OpenBlob path.
func TestSplitBody_StreamingRoundTrip(t *testing.T) {
	ctx := context.Background()
	sp := testBlobs(t)
	const max = int64(4096)
	data := makeData(10000) // → blobs of 4096, 4096, 1808

	body, blobs, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	require.NoError(t, err, "SplitBody")

	require.Equal(t, int64(len(data)), body.Size, "Size")
	wantMD5 := md5.Sum(data)
	assert.Equal(t, wantMD5[:], body.MD5, "whole-body MD5 mismatch")

	wantBounds := []struct{ start, end int64 }{{0, 4095}, {4096, 8191}, {8192, 9999}}
	require.Len(t, blobs, len(wantBounds))
	for i, w := range wantBounds {
		b := blobs[i]
		assert.Equal(t, [2]int64{w.start, w.end}, [2]int64{b.Start, b.End}, "blob %d bounds", i)
		// The recorded digest must be the sha256 multihash of exactly that slice.
		want, _ := mh.Sum(data[w.start:w.end+1], mh.SHA2_256, -1)
		assert.True(t, bytes.Equal(b.Digest, want), "blob %d digest mismatch", i)
	}

	got, err := io.ReadAll(OpenBody(ctx, NewPlainOpener(sp), did.Undef, blobs))
	require.NoError(t, err, "OpenBody read")
	require.Equal(t, data, got, "round-trip mismatch")
}

// TestOpenBodyRange covers ranged reads that start mid-blob and span blob
// boundaries — exercising the seek-into-blob path of the streaming reader.
func TestOpenBodyRange(t *testing.T) {
	ctx := context.Background()
	sp := testBlobs(t)
	const max = int64(4096)
	data := makeData(10000)
	_, blobs, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	require.NoError(t, err, "SplitBody")

	cases := []struct{ start, end int64 }{
		{0, 9999},    // whole object
		{0, 0},       // first byte
		{4095, 4096}, // straddles the first/second blob boundary
		{5000, 6000}, // wholly inside the second blob (mid-blob start)
		{8192, 9999}, // the whole last (short) blob
		{9999, 9999}, // last byte
		{100, 8500},  // spans all three blobs, mid-blob start
	}
	for _, c := range cases {
		got, err := io.ReadAll(OpenBodyRange(ctx, NewPlainOpener(sp), did.Undef, blobs, c.start, c.end))
		require.NoError(t, err, "range [%d,%d]", c.start, c.end)
		want := data[c.start : c.end+1]
		assert.Equal(t, want, got, "range [%d,%d]", c.start, c.end)
	}
}

// TestSplitBody_Empty: a zero-byte body yields no blobs and the empty digests.
func TestSplitBody_Empty(t *testing.T) {
	ctx := context.Background()
	sp := testBlobs(t)
	body, blobs, err := SplitBody(ctx, sp, bytes.NewReader(nil), 4096)
	require.NoError(t, err, "SplitBody")
	require.Zero(t, body.Size, "empty body: Size=%d Blobs", body.Size)
	require.Empty(t, blobs, "empty body: Size=%d Blobs", body.Size)
	emptyMD5 := md5.Sum(nil)
	assert.Equal(t, emptyMD5[:], body.MD5, "empty MD5")
}

// The MD5 pass starts with the first byte: a body that never delivers one
// never starts a hasher and still reports the constant empty digest, while
// any bytes at all produce the crypto/md5 digest.
func TestLazyETagHash(t *testing.T) {
	var l lazyETagHash
	l.Write(nil)
	require.Nil(t, l.a, "an empty write started the MD5 hasher")
	emptyMD5 := md5.Sum(nil)
	got := l.Sum()
	require.Equal(t, emptyMD5[:], got, "empty Sum")
	require.Nil(t, l.a, "Sum started the MD5 hasher")

	data := makeData(100<<10 + 3)
	var started lazyETagHash
	started.Write(data[:1])
	require.NotNil(t, started.a, "the first byte did not start the MD5 hasher")
	started.Write(data[1:])
	want := md5.Sum(data)
	require.Equal(t, want[:], started.Sum(), "Sum")
}

// hashingDiscardWriter stands in for a blob store in benchmarks: it pays the
// stream's sha256 pass over the bytes and drops them, so the benchmark
// measures SplitBody's own hashing rather than the disk.
type hashingDiscardWriter struct{}

func (hashingDiscardWriter) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil || n == 0 {
		return nil, n, err
	}
	digest, err := mh.Encode(h.Sum(nil), mh.SHA2_256)
	return digest, n, err
}

// BenchmarkSplitBody measures one stream through SplitBody: the whole-body
// BLAKE3 tree and md5 plus the stream's sha256 of each blob, with no disk.
func BenchmarkSplitBody(b *testing.B) {
	const size = 64 << 20
	data := makeData(size)
	b.SetBytes(size)
	b.ReportAllocs()

	for b.Loop() {
		_, _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0)
		require.NoError(b, err)
	}
}

// BenchmarkSplitBodyParallel measures aggregate throughput with GOMAXPROCS
// streams in flight, which is where the shared md5-simd server pays on
// amd64 (its lanes pack concurrent MD5 streams onto one core); on other
// architectures it tracks crypto/md5.
func BenchmarkSplitBodyParallel(b *testing.B) {
	const size = 16 << 20
	data := makeData(size)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0)
			require.NoError(b, err)
		}
	})
}

// WithoutMD5 leaves Body.MD5 nil and changes nothing else about the split.
func TestSplitBody_WithoutMD5(t *testing.T) {
	ctx := context.Background()
	sp := testBlobs(t)
	data := makeData(3*4096 + 100)
	with, withBlobs, err := SplitBody(ctx, sp, bytes.NewReader(data), 4096)
	require.NoError(t, err, "SplitBody")
	without, withoutBlobs, err := SplitBody(ctx, testBlobs(t), bytes.NewReader(data), 4096, WithoutMD5())
	require.NoError(t, err, "SplitBody without md5")
	assert.Nil(t, without.MD5, "Body.MD5")
	wantMD5 := md5.Sum(data)
	assert.Equal(t, wantMD5[:], with.MD5, "default Body.MD5 mismatch")
	require.Equal(t, without.BLAKE3, with.BLAKE3, "split differs without md5: %+v vs %+v", with, without)
	require.Equal(t, without.Size, with.Size, "split differs without md5: %+v vs %+v", with, without)
	require.Len(t, withBlobs, len(withoutBlobs), "split differs without md5: %+v vs %+v", with, without)
	for i := range withBlobs {
		require.True(t, bytes.Equal(withBlobs[i].Digest, withoutBlobs[i].Digest), "blob %d differs without md5", i)
		require.Equal(t, withoutBlobs[i].Start, withBlobs[i].Start, "blob %d differs without md5", i)
		require.Equal(t, withoutBlobs[i].End, withBlobs[i].End, "blob %d differs without md5", i)
	}
}

// MD5Writer yields the same digest as crypto/md5 over the same bytes, in any
// write pattern, and Sum is idempotent.
func TestMD5Writer(t *testing.T) {
	data := makeData(200<<10 + 7)
	want := md5.Sum(data)
	w := NewMD5Writer()
	defer w.Sum()
	for i := 0; i < len(data); i += 33 * 1024 {
		end := min(i+33*1024, len(data))
		_, err := w.Write(data[i:end])
		require.NoError(t, err)
	}
	got := w.Sum()
	require.Equal(t, want[:], got, "MD5Writer")
	require.Equal(t, want[:], w.Sum(), "second Sum")
}

// BenchmarkSplitBodyNoMD5 is BenchmarkSplitBody with the md5 pass off: the
// stream a caller gets when the client already proved the MD5.
func BenchmarkSplitBodyNoMD5(b *testing.B) {
	const size = 64 << 20
	data := makeData(size)
	b.SetBytes(size)
	b.ReportAllocs()

	for b.Loop() {
		_, _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0, WithoutMD5())
		require.NoError(b, err)
	}
}

// rangeRecordingReader serves a blob and records the byte range each
// OpenBlobRange was asked for, as a read tier that fetches ranges from the
// network does.
type rangeRecordingReader struct {
	data   []byte
	ranges [][2]int64
}

func (r *rangeRecordingReader) OpenBlob(context.Context, did.DID, mh.Multihash) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.data)), nil
}

func (r *rangeRecordingReader) OpenBlobRange(_ context.Context, _ did.DID, _ mh.Multihash, start, end int64) (io.ReadCloser, error) {
	r.ranges = append(r.ranges, [2]int64{start, end})
	return io.NopCloser(bytes.NewReader(r.data[start : end+1])), nil
}

// TestPlainOpenerAsksForTheRange: a reader that can serve a byte range is
// asked for exactly the range a ranged read needs, so a small range late in
// a large blob never fetches the prefix before it.
func TestPlainOpenerAsksForTheRange(t *testing.T) {
	data := bytes.Repeat([]byte("plaintext blob "), 1000)
	rr := &rangeRecordingReader{data: data}
	d, err := mh.Sum(data, mh.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	blobs := BlobRefs{{Digest: d, Start: 0, End: int64(len(data)) - 1}}
	start, end := int64(len(data))-20, int64(len(data))-1

	got, err := io.ReadAll(OpenBodyRange(context.Background(), NewPlainOpener(rr), did.Undef, blobs, start, end))
	require.NoError(t, err, "read range")
	require.Equal(t, data[start:end+1], got, "range read")
	require.Equal(t, [][2]int64{{start, end}}, rr.ranges, "ranges asked of the reader")
}
