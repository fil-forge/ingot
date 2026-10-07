package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"io"
	"path/filepath"
	"testing"

	"github.com/fil-forge/ucantone/did"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/filecoin-project/go-fee/aesstream"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

// TestDefaultMaxBlobSizeFitsPiri pins DefaultMaxBlobSize's derivation: the
// FEE envelope of a max-size plaintext blob (ciphertext with per-chunk GCM
// tags, plus a generous COSE-header budget — the observed single-recipient
// header is ~211 bytes) must fit the network blob ceiling (libforge
// blob.MaxBlobSize, the most raw bytes a default-configured piri accepts).
// Guards against the formula drifting (e.g. a chunk-count header if
// WithContentLength is ever used, or a second recipient).
func TestDefaultMaxBlobSizeFitsPiri(t *testing.T) {
	const headerBudget = 1024
	enc := aesstream.EncryptedSize(DefaultMaxBlobSize, aesstream.DefaultChunkSize) + headerBudget
	if enc > blobcmds.MaxBlobSize {
		t.Fatalf("DefaultMaxBlobSize %d: envelope %d exceeds the network blob ceiling %d", DefaultMaxBlobSize, enc, int64(blobcmds.MaxBlobSize))
	}
	// And the allowance is not vacuous: a split at a round 256 MiB — over
	// the ceiling by less than the allowance — must NOT fit once framed.
	if enc := aesstream.EncryptedSize(256<<20, aesstream.DefaultChunkSize); enc <= blobcmds.MaxBlobSize {
		t.Fatalf("a 256 MiB blob's envelope (%d) fits the ceiling %d — revisit DefaultMaxBlobSize", enc, int64(blobcmds.MaxBlobSize))
	}
}

func testSpool(t *testing.T) *blockstore.Spool {
	t.Helper()
	s, err := blockstore.NewSpool(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	return s
}

func makeData(n int) []byte {
	d := make([]byte, n)
	for i := range d {
		d[i] = byte(i*31 + 7)
	}
	return d
}

// TestSplitBody_StreamingRoundTrip splits a multi-blob body through the spool and
// reads it back, asserting the blob boundaries, the whole-body digests, and a
// byte-exact round trip — all via the streaming WriteBlob/OpenBlob path.
func TestSplitBody_StreamingRoundTrip(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	const max = int64(4096)
	data := makeData(10000) // → blobs of 4096, 4096, 1808

	body, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}

	if body.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", body.Size, len(data))
	}
	wantMD5 := md5.Sum(data)
	if !bytes.Equal(body.MD5, wantMD5[:]) {
		t.Errorf("whole-body MD5 mismatch")
	}

	wantBounds := []struct{ start, end int64 }{{0, 4095}, {4096, 8191}, {8192, 9999}}
	if len(body.Blobs) != len(wantBounds) {
		t.Fatalf("got %d blobs, want %d", len(body.Blobs), len(wantBounds))
	}
	for i, w := range wantBounds {
		b := body.Blobs[i]
		if b.Start != w.start || b.End != w.end {
			t.Errorf("blob %d = [%d,%d], want [%d,%d]", i, b.Start, b.End, w.start, w.end)
		}
		// The recorded digest must be the sha256 multihash of exactly that slice.
		want, _ := mh.Sum(data[w.start:w.end+1], mh.SHA2_256, -1)
		if !bytes.Equal(b.Digest, want) {
			t.Errorf("blob %d digest mismatch", i)
		}
	}

	got, err := io.ReadAll(OpenBody(ctx, NewPlainOpener(sp), did.Undef, body))
	if err != nil {
		t.Fatalf("OpenBody read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

// TestOpenBodyRange covers ranged reads that start mid-blob and span blob
// boundaries — exercising the seek-into-blob path of the streaming reader.
func TestOpenBodyRange(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	const max = int64(4096)
	data := makeData(10000)
	body, err := SplitBody(ctx, sp, bytes.NewReader(data), max)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}

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
		got, err := io.ReadAll(OpenBodyRange(ctx, NewPlainOpener(sp), did.Undef, body, c.start, c.end))
		if err != nil {
			t.Fatalf("range [%d,%d]: %v", c.start, c.end, err)
		}
		want := data[c.start : c.end+1]
		if !bytes.Equal(got, want) {
			t.Errorf("range [%d,%d]: got %d bytes, want %d (mismatch)", c.start, c.end, len(got), len(want))
		}
	}
}

// TestSplitBody_Empty: a zero-byte body yields no blobs and the empty digests.
func TestSplitBody_Empty(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	body, err := SplitBody(ctx, sp, bytes.NewReader(nil), 4096)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}
	if body.Size != 0 || len(body.Blobs) != 0 {
		t.Fatalf("empty body: Size=%d Blobs=%d, want 0/0", body.Size, len(body.Blobs))
	}
	emptyMD5 := md5.Sum(nil)
	if !bytes.Equal(body.MD5, emptyMD5[:]) {
		t.Errorf("empty MD5 = %x, want %x", body.MD5, emptyMD5)
	}
}

// The MD5 pass starts with the first byte: a body that never delivers one
// never starts a hasher and still reports the constant empty digest, while
// any bytes at all produce the crypto/md5 digest.
func TestLazyETagHash(t *testing.T) {
	var l lazyETagHash
	l.Write(nil)
	if l.a != nil {
		t.Fatal("an empty write started the MD5 hasher")
	}
	emptyMD5 := md5.Sum(nil)
	if got := l.Sum(); !bytes.Equal(got, emptyMD5[:]) {
		t.Fatalf("empty Sum = %x, want %x", got, emptyMD5)
	}
	if l.a != nil {
		t.Fatal("Sum started the MD5 hasher")
	}

	data := makeData(100<<10 + 3)
	var started lazyETagHash
	started.Write(data[:1])
	if started.a == nil {
		t.Fatal("the first byte did not start the MD5 hasher")
	}
	started.Write(data[1:])
	want := md5.Sum(data)
	if got := started.Sum(); !bytes.Equal(got, want[:]) {
		t.Fatalf("Sum = %x, want %x", got, want)
	}
}

// hashingDiscardWriter stands in for the spool in benchmarks: it pays the
// spool's sha256 pass over the bytes and drops them, so the benchmark
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
// BLAKE3 tree and md5 plus the spool's sha256 of each blob, with no disk.
func BenchmarkSplitBody(b *testing.B) {
	const size = 64 << 20
	data := makeData(size)
	b.SetBytes(size)
	b.ReportAllocs()

	for b.Loop() {
		if _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0); err != nil {
			b.Fatal(err)
		}
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
			if _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// WithoutMD5 leaves Body.MD5 nil and changes nothing else about the split.
func TestSplitBody_WithoutMD5(t *testing.T) {
	ctx := context.Background()
	sp := testSpool(t)
	data := makeData(3*4096 + 100)
	with, err := SplitBody(ctx, sp, bytes.NewReader(data), 4096)
	if err != nil {
		t.Fatalf("SplitBody: %v", err)
	}
	without, err := SplitBody(ctx, testSpool(t), bytes.NewReader(data), 4096, WithoutMD5())
	if err != nil {
		t.Fatalf("SplitBody without md5: %v", err)
	}
	if without.MD5 != nil {
		t.Errorf("Body.MD5 = %x, want nil", without.MD5)
	}
	wantMD5 := md5.Sum(data)
	if !bytes.Equal(with.MD5, wantMD5[:]) {
		t.Errorf("default Body.MD5 mismatch")
	}
	if !bytes.Equal(with.BLAKE3, without.BLAKE3) || with.Size != without.Size || len(with.Blobs) != len(without.Blobs) {
		t.Fatalf("split differs without md5: %+v vs %+v", with, without)
	}
	for i := range with.Blobs {
		if !bytes.Equal(with.Blobs[i].Digest, without.Blobs[i].Digest) || with.Blobs[i].Start != without.Blobs[i].Start || with.Blobs[i].End != without.Blobs[i].End {
			t.Fatalf("blob %d differs without md5", i)
		}
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
		if _, err := w.Write(data[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.Sum(); !bytes.Equal(got, want[:]) {
		t.Fatalf("MD5Writer = %x, want %x", got, want)
	}
	if got := w.Sum(); !bytes.Equal(got, want[:]) {
		t.Fatalf("second Sum = %x, want %x", got, want)
	}
}

// BenchmarkSplitBodyNoMD5 is BenchmarkSplitBody with the md5 pass off: the
// stream a caller gets when the client already proved the MD5.
func BenchmarkSplitBodyNoMD5(b *testing.B) {
	const size = 64 << 20
	data := makeData(size)
	b.SetBytes(size)
	b.ReportAllocs()

	for b.Loop() {
		if _, err := SplitBody(context.Background(), hashingDiscardWriter{}, bytes.NewReader(data), 0, WithoutMD5()); err != nil {
			b.Fatal(err)
		}
	}
}
