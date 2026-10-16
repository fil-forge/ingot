package s3frontend

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
)

// encFixture is a spool + registry + region-key harness holding an object
// whose body blobs are FEE-encrypted — the state the encrypting write path
// will produce, minted directly so the read path is testable before it lands.
type encFixture struct {
	backend   *Backend
	space     did.DID
	body      msbucket.Body
	plaintext []byte
	spool     *blockstore.Spool
	mem       *inmem.MemStore
}

// encChunkSize is deliberately the FEE minimum so short test blobs still
// span several STREAM chunks and ranges exercise chunk-boundary math.
const encChunkSize = aesstream.MinChunkSize

// newEncFixture splits plaintext into blobs of blobSize bytes, encrypts each
// into a FEE envelope stored in a fresh spool under its ciphertext digest,
// records the encryption params + location rows, and returns a Backend wired
// with just the pieces the read path uses.
func newEncFixture(t *testing.T, plaintext []byte, blobSize int) *encFixture {
	t.Helper()
	ctx := context.Background()

	spool, err := blockstore.NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	mem := inmem.NewMemStore()
	provider, err := regionkey.NewInProcessProvider("v1", randBytes(t, regionkey.KEKLen))
	if err != nil {
		t.Fatalf("NewInProcessProvider: %v", err)
	}
	space := testutil.RandomDID(t)

	var blobs []msbucket.BlobRef
	for off := 0; off < len(plaintext); {
		end := off + blobSize
		if end > len(plaintext) {
			end = len(plaintext)
		}
		piece := plaintext[off:end]

		cek := randBytes(t, 32)
		rc, desc, err := fee.EncryptWithCEK(bytes.NewReader(piece), cek, nil,
			fee.WithChunkSize(encChunkSize), fee.WithPlaintextLength(int64(len(piece))))
		if err != nil {
			t.Fatalf("EncryptWithCEK: %v", err)
		}
		digest, n, err := spool.WriteBlob(ctx, rc)
		if err != nil {
			t.Fatalf("WriteBlob (envelope): %v", err)
		}
		_ = rc.Close()

		wrapped, err := provider.Wrap(ctx, regionkey.BindingContext{Space: space, Digest: digest}, cek)
		if err != nil {
			t.Fatalf("Wrap CEK: %v", err)
		}
		if err := mem.PutEncryptionParams(ctx, registry.BlobEncryptionParams{
			Space:            space,
			Digest:           digest,
			RegionWrappedCEK: wrapped.Ciphertext,
			RegionKeyVersion: string(wrapped.Version),
			HeaderLen:        desc.HeaderLen,
			AAD:              desc.AAD,
		}); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if err := mem.PutLocation(ctx, registry.BlobLocation{
			Space: space, Digest: digest, Provider: "did:web:piri.test", URL: "http://piri.test/blob", Size: n,
		}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		blobs = append(blobs, msbucket.BlobRef{Digest: digest, Start: int64(off), End: int64(end - 1)})
		off = end
	}

	read := blockstore.NewLayered(spool, nil, inmem.NopBaseReader{})
	return &encFixture{
		backend: &Backend{
			read:       read,
			locations:  mem,
			encParams:  mem,
			regionKeys: provider,
		},
		space:     space,
		body:      msbucket.Body{Size: int64(len(plaintext)), Blobs: blobs},
		plaintext: plaintext,
		spool:     spool,
		mem:       mem,
	}
}

// testRegionKeys returns an in-process region key provider over a random KEK
// — the required RegionKeys dependency for any test Backend that serves
// object reads.
func testRegionKeys(t *testing.T) regionkey.Provider {
	t.Helper()
	p, err := regionkey.NewInProcessProvider("v1", randBytes(t, regionkey.KEKLen))
	if err != nil {
		t.Fatalf("NewInProcessProvider: %v", err)
	}
	return p
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// patterned returns n deterministic, position-dependent bytes so a range
// served from the wrong offset can never compare equal.
func patterned(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i>>8) ^ byte(i*7)
	}
	return b
}

// TestDecryptingRead_Ranges mirrors the plaintext chunker range table over an
// encrypted three-blob body: every range must decrypt byte-exactly, whether it
// starts mid-blob, crosses blob boundaries, or spans everything.
func TestDecryptingRead_Ranges(t *testing.T) {
	ctx := context.Background()
	// Three blobs (10000, 10000, 4000 bytes) at the 4 KiB chunk size: blob
	// ranges cross STREAM chunk boundaries as well as blob boundaries.
	fx := newEncFixture(t, patterned(24000), 10000)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}

	whole, err := io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if err != nil {
		t.Fatalf("OpenBody read: %v", err)
	}
	if !bytes.Equal(whole, fx.plaintext) {
		t.Fatalf("whole-body decrypt mismatch (%d bytes)", len(whole))
	}

	cases := []struct {
		name       string
		start, end int64
	}{
		{"first byte", 0, 0},
		{"last byte", 23999, 23999},
		{"inside blob 1, chunk-crossing", 100, 9000},
		{"mid blob 2", 12000, 15000},
		{"blob boundary crossing", 9500, 10500},
		{"all blobs", 100, 23900},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, c.start, c.end))
			if err != nil {
				t.Fatalf("range read: %v", err)
			}
			if !bytes.Equal(got, fx.plaintext[c.start:c.end+1]) {
				t.Fatalf("range [%d,%d] mismatch (%d bytes)", c.start, c.end, len(got))
			}
		})
	}
}

// Fail closed: every body blob is written encrypted, so a referenced blob
// with no encryption-params row (a shredded or lost row) is a request error
// — never "stored as plaintext", which would stream the raw envelope bytes
// under a 200.
func TestDecryptingRead_MissingParamsRowFailsClosed(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)

	// Shred one blob's row out from under the body.
	if err := fx.mem.DeleteEncryptionParams(ctx, fx.space, fx.body.Blobs[1].Digest); err != nil {
		t.Fatalf("DeleteEncryptionParams: %v", err)
	}
	if _, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1); err == nil {
		t.Fatal("bodyOpener over a shredded row succeeded — raw ciphertext would have been served")
	} else if !strings.Contains(err.Error(), "no encryption-params row") {
		t.Fatalf("bodyOpener error = %v, want the missing-row rejection", err)
	}
}

// countingEncStores wraps the encryption-params and location stores,
// counting the lookups a read makes and recording the digests each batch
// asked for.
type countingEncStores struct {
	registry.EncryptionParamsStore
	singleParams int
	paramBatches [][]multihash.Multihash
	singleLocs   int
	locBatches   [][]multihash.Multihash
}

func (c *countingEncStores) GetEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobEncryptionParams, error) {
	c.singleParams++
	return c.EncryptionParamsStore.GetEncryptionParams(ctx, space, digest)
}

func (c *countingEncStores) GetEncryptionParamsBatch(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]registry.BlobEncryptionParams, error) {
	c.paramBatches = append(c.paramBatches, digests)
	return c.EncryptionParamsStore.GetEncryptionParamsBatch(ctx, space, digests)
}

// countingLocs is countingEncStores' location half.
type countingLocs struct {
	registry.LocationStore
	c *countingEncStores
}

func (l countingLocs) GetLocation(ctx context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobLocation, error) {
	l.c.singleLocs++
	return l.LocationStore.GetLocation(ctx, space, digest)
}

func (l countingLocs) GetLocations(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]registry.BlobLocation, error) {
	l.c.locBatches = append(l.c.locBatches, digests)
	return l.LocationStore.GetLocations(ctx, space, digests)
}

// countLookups swaps fx's stores for counting wrappers.
func countLookups(fx *encFixture) *countingEncStores {
	c := &countingEncStores{EncryptionParamsStore: fx.backend.encParams}
	fx.backend.encParams = c
	fx.backend.locations = countingLocs{LocationStore: fx.backend.locations, c: c}
	return c
}

// A whole-body read prefetches every blob in one params query and one
// location query, however many blobs the body has.
func TestDecryptingRead_PrefetchIsTwoQueries(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(40000), 4000) // ten blobs
	c := countLookups(fx)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	if c.singleParams != 0 || c.singleLocs != 0 {
		t.Fatalf("per-blob lookups: %d params, %d locations; want none", c.singleParams, c.singleLocs)
	}
	if len(c.paramBatches) != 1 || len(c.locBatches) != 1 {
		t.Fatalf("batches: %d params, %d locations; want 1 and 1", len(c.paramBatches), len(c.locBatches))
	}
	if n := len(c.paramBatches[0]); n != len(fx.body.Blobs) {
		t.Fatalf("params batch named %d digests, want %d", n, len(fx.body.Blobs))
	}
	got, err := io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if err != nil {
		t.Fatalf("OpenBody read: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext) {
		t.Fatal("whole-body decrypt mismatch")
	}
}

// A ranged read prefetches only the blobs its range covers, reads them, and
// refuses a blob outside the range it was built for.
func TestDecryptingRead_RangePrefetchesCoveredBlobs(t *testing.T) {
	ctx := context.Background()
	// Three blobs: [0,9999], [10000,19999], [20000,23999].
	fx := newEncFixture(t, patterned(24000), 10000)
	c := countLookups(fx)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 12000, 15000)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	want := fx.body.Blobs[1].Digest
	for _, batch := range [][][]multihash.Multihash{c.paramBatches, c.locBatches} {
		if len(batch) != 1 || len(batch[0]) != 1 || !bytes.Equal(batch[0][0], want) {
			t.Fatalf("batches = %x, want one naming only the middle blob %x", batch, want)
		}
	}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 12000, 15000))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext[12000:15001]) {
		t.Fatal("range decrypt mismatch")
	}
	if _, err := opener.OpenBlobRange(ctx, fx.space, fx.body.Blobs[0], 0, 99); err == nil ||
		!strings.Contains(err.Error(), "no prefetched encryption state") {
		t.Fatalf("OpenBlobRange outside the range: err = %v, want the fail-closed refusal", err)
	}
}

// A blob outside the range is never read, so its missing row does not fail
// the read; the same row missing inside the range does.
func TestDecryptingRead_MissingRowOutsideRangeIsNotRead(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)
	if err := fx.mem.DeleteEncryptionParams(ctx, fx.space, fx.body.Blobs[0].Digest); err != nil {
		t.Fatalf("DeleteEncryptionParams: %v", err)
	}

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 10000, 23999)
	if err != nil {
		t.Fatalf("bodyOpener over blobs 2-3: %v", err)
	}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 10000, 23999))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext[10000:]) {
		t.Fatal("range decrypt mismatch")
	}
	if _, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 9999, 10000); err == nil ||
		!strings.Contains(err.Error(), "no encryption-params row") {
		t.Fatalf("bodyOpener over the shredded blob: err = %v, want the missing-row rejection", err)
	}
}

// The network-tier shape: a read store WITHOUT the BlobRangeReader capability
// and whose readers cannot seek — the ciphertext span is served through the
// OpenBlob fallback (read-and-discard), as the Forge tier would absent ranged
// retrieval.
func TestDecryptingRead_UnrangedUnseekableStore(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	opener.(*decryptingOpener).read = unseekableBlobs{fx.spool}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 9500, 15000))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext[9500:15001]) {
		t.Fatal("fallback-path range mismatch")
	}
}

// Tampered ciphertext fails authentication mid-stream rather than yielding
// wrong bytes.
func TestDecryptingRead_TamperFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	// Flip one ciphertext byte on disk, past the envelope header.
	path := fx.spool.Path(fx.body.Blobs[0].Digest)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read spooled envelope: %v", err)
	}
	raw[len(raw)-1] ^= 0x01
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write tampered envelope: %v", err)
	}

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	_, err = io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if !errors.Is(err, aesstream.ErrCorrupted) {
		t.Fatalf("tampered read err = %v, want aesstream.ErrCorrupted", err)
	}
}

// A params row transplanted from another blob fails the CEK unwrap: the wrap
// is context-bound to (space, digest), so blob B's row cannot decrypt blob A.
func TestDecryptingRead_TransplantedRowFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(20000), 10000)

	a, b := fx.body.Blobs[0].Digest, fx.body.Blobs[1].Digest
	rowB, err := fx.backend.encParams.GetEncryptionParams(ctx, fx.space, b)
	if err != nil {
		t.Fatalf("GetEncryptionParams: %v", err)
	}
	transplant := *rowB
	transplant.Digest = a
	if err := fx.backend.encParams.PutEncryptionParams(ctx, transplant); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	_, err = io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if !errors.Is(err, regionkey.ErrAuthentication) {
		t.Fatalf("transplanted-row read err = %v, want regionkey.ErrAuthentication", err)
	}
}

// The encryption dependencies are required: a Backend missing them fails a
// read with a clear error rather than serving ciphertext as plaintext or
// nil-panicking.
func TestDecryptingRead_MissingDepsFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	fx.backend.regionKeys = nil
	if _, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1); err == nil ||
		!strings.Contains(err.Error(), "encryption dependencies not configured") {
		t.Fatalf("bodyOpener err = %v, want encryption-dependencies error", err)
	}
}

// An encrypted blob whose location row is missing fails at opener
// construction, before any response headers could go out.
func TestDecryptingRead_MissingLocationFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	if err := fx.backend.locations.DeleteLocation(ctx, fx.space, fx.body.Blobs[0].Digest); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}
	_, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err == nil || !strings.Contains(err.Error(), "no recorded location") {
		t.Fatalf("bodyOpener err = %v, want missing-location error", err)
	}
}

// An encrypted blob whose recorded stored size contradicts the plaintext
// length its envelope declares fails at opener construction, before any
// response headers could go out, even when the blob's bytes are intact.
func TestDecryptingRead_StoredSizeMismatchFails(t *testing.T) {
	ctx := context.Background()
	for name, delta := range map[string]int64{"truncated": -1, "padded": 1} {
		t.Run(name, func(t *testing.T) {
			fx := newEncFixture(t, patterned(9000), 10000)
			ref := fx.body.Blobs[0]
			loc, err := fx.backend.locations.GetLocation(ctx, fx.space, ref.Digest)
			if err != nil {
				t.Fatalf("GetLocation: %v", err)
			}
			loc.Size += delta
			if err := fx.mem.PutLocation(ctx, *loc); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
			_, err = fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
			if !errors.Is(err, fee.ErrSizeMismatch) {
				t.Fatalf("bodyOpener err = %v, want fee.ErrSizeMismatch", err)
			}
		})
	}
}

// unseekableBlobs strips both the BlobRangeReader capability and reader
// seekability from a spool, imitating a network tier without ranged reads.
type unseekableBlobs struct {
	spool *blockstore.Spool
}

func (u unseekableBlobs) OpenBlob(ctx context.Context, space did.DID, digest multihash.Multihash) (io.ReadCloser, error) {
	rc, err := u.spool.OpenBlob(ctx, space, digest)
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.Reader(rc), rc}, nil
}

// A plaintext range past the end of a blob means the manifest and the
// registry row disagree; the read fails instead of serving a clamped range.
func TestDecryptingRead_RangePastBlobEndFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)
	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	_, err = opener.OpenBlobRange(ctx, fx.space, fx.body.Blobs[0], 0, 9000)
	if err == nil || !strings.Contains(err.Error(), "exceeds the 9000 bytes its ciphertext holds") {
		t.Fatalf("OpenBlobRange err = %v, want a range-past-end error", err)
	}
}

// spanReaderAt only serves in-order reads of an opened span, and reports a
// span that ends early.
func TestSpanReaderAt(t *testing.T) {
	data := []byte("0123456789")

	var unopened spanReaderAt
	if _, err := unopened.ReadAt(make([]byte, 2), 0); err == nil {
		t.Fatal("ReadAt before the span is fetched: want an error")
	}

	s := &spanReaderAt{span: bytes.NewReader(data), pos: 100}
	buf := make([]byte, 4)
	if n, err := s.ReadAt(buf, 100); err != nil || n != 4 || string(buf) != "0123" {
		t.Fatalf("first ReadAt = %d, %v, %q", n, err, buf[:n])
	}
	if _, err := s.ReadAt(buf, 100); err == nil {
		t.Fatal("ReadAt repeating an offset: want an error")
	}
	if _, err := s.ReadAt(buf, 105); err == nil {
		t.Fatal("ReadAt skipping ahead: want an error")
	}
	if n, err := s.ReadAt(buf, 104); err != nil || n != 4 || string(buf) != "4567" {
		t.Fatalf("second ReadAt = %d, %v, %q", n, err, buf[:n])
	}
	if n, err := s.ReadAt(buf, 108); !errors.Is(err, io.ErrUnexpectedEOF) || n != 2 {
		t.Fatalf("ReadAt past the span's end = %d, %v, want 2, io.ErrUnexpectedEOF", n, err)
	}
}

// trackingBlobs wraps a read store, counting the ranged fetches it opens and
// how many are closed.
type trackingBlobs struct {
	blockstore.ReadStore
	opened, closed int
}

func (t *trackingBlobs) OpenBlobRange(ctx context.Context, space did.DID, digest multihash.Multihash, start, end int64) (io.ReadCloser, error) {
	rc, err := blockstore.OpenBlobRangeOf(ctx, t.ReadStore, space, digest, start, end)
	if err != nil {
		return nil, err
	}
	t.opened++
	return struct {
		io.Reader
		io.Closer
	}{rc, closerFunc(func() error { t.closed++; return rc.Close() })}, nil
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// Closing a decrypted range releases its ciphertext fetch.
func TestDecryptingRead_CloseReleasesFetch(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)
	tracked := &trackingBlobs{ReadStore: fx.backend.read}
	fx.backend.read = tracked

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size-1)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	rc, err := opener.OpenBlobRange(ctx, fx.space, fx.body.Blobs[0], 100, 199)
	if err != nil {
		t.Fatalf("OpenBlobRange: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext[100:200]) {
		t.Fatal("decrypted range does not match the plaintext")
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if tracked.opened != 1 || tracked.closed != 1 {
		t.Fatalf("fetches opened %d, closed %d; want 1 and 1", tracked.opened, tracked.closed)
	}
}
