package s3frontend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
)

// encFixture is a cache + registry + region-key harness holding an object
// whose body blobs are FEE-encrypted — the state the encrypting write path
// will produce, minted directly so the read path is testable before it lands.
type encFixture struct {
	backend   *Backend
	space     did.DID
	body      msbucket.BlobRefs
	plaintext []byte
	cache     *blockstore.BlobCache
	mem       *testStore
}

// encChunkSize is deliberately the FEE minimum so short test blobs still
// span several STREAM chunks and ranges exercise chunk-boundary math.
const encChunkSize = aesstream.MinChunkSize

// newEncFixture splits plaintext into blobs of blobSize bytes, encrypts each
// into a FEE envelope stored in a fresh cache under its ciphertext digest,
// records the encryption params + location rows, and returns a Backend wired
// with just the pieces the read path uses.
func newEncFixture(t *testing.T, plaintext []byte, blobSize int) *encFixture {
	t.Helper()
	ctx := context.Background()

	cache, err := blockstore.NewBlobCache(t.TempDir())
	require.NoError(t, err, "NewBlobCache")
	mem := newTestStore(t)
	provider, err := regionkey.NewInProcessProvider("v1", randBytes(t, regionkey.KEKLen))
	require.NoError(t, err, "NewInProcessProvider")
	space := testutil.RandomDID(t)

	var blobs msbucket.BlobRefs
	for off := 0; off < len(plaintext); {
		end := off + blobSize
		if end > len(plaintext) {
			end = len(plaintext)
		}
		piece := plaintext[off:end]

		cek := randBytes(t, 32)
		rc, desc, err := fee.EncryptWithCEK(bytes.NewReader(piece), cek, nil,
			fee.WithChunkSize(encChunkSize), fee.WithPlaintextLength(int64(len(piece))))
		require.NoError(t, err, "EncryptWithCEK")
		digest, n := cacheBlob(t, cache, rc)
		_ = rc.Close()

		wrapped, err := provider.Wrap(ctx, regionkey.BindingContext{Space: space, Digest: digest}, cek)
		require.NoError(t, err, "Wrap CEK")
		require.NoError(t, mem.PutEncryptionParams(ctx, registry.BlobEncryptionParams{
			Space:            space,
			Digest:           digest,
			RegionWrappedCEK: wrapped.Ciphertext,
			RegionKeyVersion: string(wrapped.Version),
			HeaderLen:        desc.HeaderLen,
			AAD:              desc.AAD,
		}), "PutEncryptionParams")
		require.NoError(t, mem.PutLocation(ctx, registry.BlobLocation{
			Space: space, Digest: digest, Provider: "did:web:piri.test", URL: "http://piri.test/blob", Size: n,
		}), "PutLocation")
		blobs = append(blobs, msbucket.BlobRef{Digest: digest, Start: int64(off), End: int64(end - 1)})
		off = end
	}

	read := blockstore.NewLayered(cache, nil, testfakes.NopBaseReader{})
	return &encFixture{
		backend: &Backend{
			read:       read,
			locations:  mem,
			encParams:  mem,
			regionKeys: provider,
		},
		space:     space,
		body:      blobs,
		plaintext: plaintext,
		cache:     cache,
		mem:       mem,
	}
}

// testRegionKeys returns an in-process region key provider over a random KEK
// — the required RegionKeys dependency for any test Backend that serves
// object reads.
func testRegionKeys(t *testing.T) regionkey.Provider {
	t.Helper()
	p, err := regionkey.NewInProcessProvider("v1", randBytes(t, regionkey.KEKLen))
	require.NoError(t, err, "NewInProcessProvider")
	return p
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err, "rand")
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")

	whole, err := io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	require.NoError(t, err, "OpenBody read")
	require.Equal(t, fx.plaintext, whole, "whole-body decrypt mismatch (%d bytes)", len(whole))

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
			require.NoError(t, err, "range read")
			require.Equal(t, fx.plaintext[c.start:c.end+1], got, "range [%d,%d] mismatch (%d bytes)", c.start, c.end, len(got))
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
	require.NoError(t, fx.mem.DeleteEncryptionParams(ctx, fx.space, fx.body[1].Digest), "DeleteEncryptionParams")
	_, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.Error(t, err, "bodyOpener over a shredded row succeeded — raw ciphertext would have been served")
	require.ErrorContains(t, err, "no encryption-params row", "bodyOpener error, want the missing-row rejection")
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	require.Zero(t, c.singleParams, "per-blob lookups: %d params, %d locations", c.singleParams, c.singleLocs)
	require.Zero(t, c.singleLocs, "per-blob lookups: %d params, %d locations", c.singleParams, c.singleLocs)
	require.Len(t, c.paramBatches, 1, "batches: %d params, %d locations", len(c.paramBatches), len(c.locBatches))
	require.Len(t, c.locBatches, 1, "batches: %d params, %d locations", len(c.paramBatches), len(c.locBatches))
	n := len(c.paramBatches[0])
	require.Equal(t, len(fx.body), n, "params batch named %d digests", n)
	got, err := io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	require.NoError(t, err, "OpenBody read")
	require.Equal(t, fx.plaintext, got, "whole-body decrypt mismatch")
}

// A ranged read prefetches only the blobs its range covers, reads them, and
// refuses a blob outside the range it was built for.
func TestDecryptingRead_RangePrefetchesCoveredBlobs(t *testing.T) {
	ctx := context.Background()
	// Three blobs: [0,9999], [10000,19999], [20000,23999].
	fx := newEncFixture(t, patterned(24000), 10000)
	c := countLookups(fx)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 12000, 15000)
	require.NoError(t, err, "bodyOpener")
	want := fx.body[1].Digest
	for _, batch := range [][][]multihash.Multihash{c.paramBatches, c.locBatches} {
		require.Len(t, batch, 1, "batches = %x, want one naming only the middle blob %x", batch, want)
		require.Len(t, batch[0], 1, "batches = %x, want one naming only the middle blob %x", batch, want)
		require.True(t, bytes.Equal(batch[0][0], want), "batches = %x, want one naming only the middle blob %x", batch, want)
	}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 12000, 15000))
	require.NoError(t, err, "range read")
	require.Equal(t, fx.plaintext[12000:15001], got, "range decrypt mismatch")
	_, err = opener.OpenBlobRange(ctx, fx.space, fx.body[0], 0, 99)
	require.ErrorContains(t, err, "no prefetched encryption state", "OpenBlobRange outside the range, want the fail-closed refusal")
}

// extentIndex serves a flat list as extents of per blobs each, the shape of
// a stored list without the catalog.
type extentIndex struct {
	msbucket.BlobRefs
	per int
}

func (x extentIndex) Extent(ctx context.Context, off int64) ([]msbucket.BlobRef, error) {
	ref, err := x.Locate(ctx, off)
	if err != nil {
		return nil, err
	}
	i := 0
	for x.BlobRefs[i].Start != ref.Start {
		i++
	}
	from := i - i%x.per
	return x.BlobRefs[from:min(from+x.per, len(x.BlobRefs))], nil
}

// A read across several extents fetches each extent's state once, as the
// read reaches it, and holds at most one extent's at a time: a read of any
// size keeps a bounded amount of key material.
func TestDecryptingRead_HoldsOneExtentAtATime(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(40000), 4000) // ten blobs
	c := countLookups(fx)
	idx := extentIndex{BlobRefs: fx.body, per: 3} // extents of 3, 3, 3, 1

	opener, err := fx.backend.bodyOpener(ctx, fx.space, idx, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	held := &maxHeld{opener: opener.(*decryptingOpener)}
	got, err := io.ReadAll(msbucket.OpenBody(ctx, held, fx.space, idx))
	require.NoError(t, err, "read")
	require.Equal(t, fx.plaintext, got, "decrypt mismatch")
	require.Len(t, c.paramBatches, 4, "batches: %d params, %d locations; want one per extent, 4", len(c.paramBatches), len(c.locBatches))
	require.Len(t, c.locBatches, 4, "batches: %d params, %d locations; want one per extent, 4", len(c.paramBatches), len(c.locBatches))
	require.Equal(t, 3, held.max, "held state for up to %d blobs at once", held.max)
}

// maxHeld records the most blobs the opener held state for after any open.
type maxHeld struct {
	opener *decryptingOpener
	max    int
}

func (m *maxHeld) OpenBlobRange(ctx context.Context, space did.DID, ref msbucket.BlobRef, start, end int64) (io.ReadCloser, error) {
	rc, err := m.opener.OpenBlobRange(ctx, space, ref, start, end)
	m.max = max(m.max, len(m.opener.enc))
	return rc, err
}

// A blob outside the range is never read, so its missing row does not fail
// the read; the same row missing inside the range does.
func TestDecryptingRead_MissingRowOutsideRangeIsNotRead(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)
	require.NoError(t, fx.mem.DeleteEncryptionParams(ctx, fx.space, fx.body[0].Digest), "DeleteEncryptionParams")

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 10000, 23999)
	require.NoError(t, err, "bodyOpener over blobs 2-3")
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 10000, 23999))
	require.NoError(t, err, "range read")
	require.Equal(t, fx.plaintext[10000:], got, "range decrypt mismatch")
	_, err = fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 9999, 10000)
	require.ErrorContains(t, err, "no encryption-params row", "bodyOpener over the shredded blob, want the missing-row rejection")
}

// The network-tier shape: a read store WITHOUT the BlobRangeReader capability
// and whose readers cannot seek — the ciphertext span is served through the
// OpenBlob fallback (read-and-discard), as the Forge tier would absent ranged
// retrieval.
func TestDecryptingRead_UnrangedUnseekableStore(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	opener.(*decryptingOpener).read = unseekableBlobs{fx.cache}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 9500, 15000))
	require.NoError(t, err, "range read")
	require.Equal(t, fx.plaintext[9500:15001], got, "fallback-path range mismatch")
}

// Tampered ciphertext fails authentication mid-stream rather than yielding
// wrong bytes.
func TestDecryptingRead_TamperFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	// Flip one ciphertext byte on disk, past the envelope header.
	path := fx.cache.Path(fx.body[0].Digest)
	raw, err := os.ReadFile(path)
	require.NoError(t, err, "read cached envelope")
	raw[len(raw)-1] ^= 0x01
	require.NoError(t, os.WriteFile(path, raw, 0o600), "write tampered envelope")

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	_, err = io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	require.ErrorIs(t, err, aesstream.ErrCorrupted, "tampered read err")
}

// A params row transplanted from another blob fails the CEK unwrap: the wrap
// is context-bound to (space, digest), so blob B's row cannot decrypt blob A.
func TestDecryptingRead_TransplantedRowFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(20000), 10000)

	a, b := fx.body[0].Digest, fx.body[1].Digest
	rowB, err := fx.backend.encParams.GetEncryptionParams(ctx, fx.space, b)
	require.NoError(t, err, "GetEncryptionParams")
	transplant := *rowB
	transplant.Digest = a
	require.NoError(t, fx.backend.encParams.PutEncryptionParams(ctx, transplant), "PutEncryptionParams")

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	_, err = io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	require.ErrorIs(t, err, regionkey.ErrAuthentication, "transplanted-row read err")
}

// The encryption dependencies are required: a Backend missing them fails a
// read with a clear error rather than serving ciphertext as plaintext or
// nil-panicking.
func TestDecryptingRead_MissingDepsFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	fx.backend.regionKeys = nil
	_, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.Error(t, err, "bodyOpener err = %v, want encryption-dependencies error", err)
	require.Contains(t, err.Error(), "encryption dependencies not configured", "bodyOpener err")
}

// An encrypted blob whose location row is missing fails at opener
// construction, before any response headers could go out.
func TestDecryptingRead_MissingLocationFails(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	require.NoError(t, fx.backend.locations.DeleteLocation(ctx, fx.space, fx.body[0].Digest), "DeleteLocation")
	_, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.Error(t, err, "bodyOpener err = %v, want missing-location error", err)
	require.Contains(t, err.Error(), "no recorded location", "bodyOpener err")
}

// An encrypted blob whose recorded stored size contradicts the plaintext
// length its envelope declares fails at opener construction, before any
// response headers could go out, even when the blob's bytes are intact.
func TestDecryptingRead_StoredSizeMismatchFails(t *testing.T) {
	ctx := context.Background()
	for name, delta := range map[string]int64{"truncated": -1, "padded": 1} {
		t.Run(name, func(t *testing.T) {
			fx := newEncFixture(t, patterned(9000), 10000)
			ref := fx.body[0]
			loc, err := fx.backend.locations.GetLocation(ctx, fx.space, ref.Digest)
			require.NoError(t, err, "GetLocation")
			loc.Size += delta
			require.NoError(t, fx.mem.PutLocation(ctx, *loc), "PutLocation")
			_, err = fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
			require.ErrorIs(t, err, fee.ErrSizeMismatch, "bodyOpener err")
		})
	}
}

// unseekableBlobs strips both the BlobRangeReader capability and reader
// seekability from a cache, imitating a network tier without ranged reads.
type unseekableBlobs struct {
	cache *blockstore.BlobCache
}

func (u unseekableBlobs) OpenBlob(ctx context.Context, space did.DID, digest multihash.Multihash) (io.ReadCloser, error) {
	rc, err := u.cache.OpenBlob(ctx, space, digest)
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
	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	_, err = opener.OpenBlobRange(ctx, fx.space, fx.body[0], 0, 9000)
	require.Error(t, err, "OpenBlobRange err = %v, want a range-past-end error", err)
	require.Contains(t, err.Error(), "exceeds the 9000 bytes its ciphertext holds", "OpenBlobRange err = %v, want a range-past-end error", err)
}

// spanReaderAt only serves in-order reads of an opened span, and reports a
// span that ends early.
func TestSpanReaderAt(t *testing.T) {
	data := []byte("0123456789")

	var unopened spanReaderAt
	_, err := unopened.ReadAt(make([]byte, 2), 0)
	require.Error(t, err, "ReadAt before the span is fetched: want an error")

	s := &spanReaderAt{span: bytes.NewReader(data), pos: 100}
	buf := make([]byte, 4)
	n, err := s.ReadAt(buf, 100)
	require.NoError(t, err, "first ReadAt")
	require.Equal(t, 4, n, "first ReadAt")
	require.Equal(t, "0123", string(buf), "first ReadAt")
	_, err = s.ReadAt(buf, 100)
	require.Error(t, err, "ReadAt repeating an offset: want an error")
	_, err = s.ReadAt(buf, 105)
	require.Error(t, err, "ReadAt skipping ahead: want an error")
	n, err = s.ReadAt(buf, 104)
	require.NoError(t, err, "second ReadAt")
	require.Equal(t, 4, n, "second ReadAt")
	require.Equal(t, "4567", string(buf), "second ReadAt")
	n, err = s.ReadAt(buf, 108)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "ReadAt past the span's end")
	require.Equal(t, 2, n, "ReadAt past the span's end")
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body, false, 0, fx.body.Size()-1)
	require.NoError(t, err, "bodyOpener")
	rc, err := opener.OpenBlobRange(ctx, fx.space, fx.body[0], 100, 199)
	require.NoError(t, err, "OpenBlobRange")
	got, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll")
	require.Equal(t, fx.plaintext[100:200], got, "decrypted range does not match the plaintext")
	require.NoError(t, rc.Close(), "Close")
	require.Equal(t, 1, tracked.opened, "fetches opened %d, closed %d", tracked.opened, tracked.closed)
	require.Equal(t, 1, tracked.closed, "fetches opened %d, closed %d", tracked.opened, tracked.closed)
}

// cacheBlob stores r's bytes in cache under their sha256 digest, and returns
// the digest and the byte count.
func cacheBlob(t testing.TB, cache *blockstore.BlobCache, r io.Reader) (multihash.Multihash, int64) {
	t.Helper()
	local, err := cache.CreateTemp()
	require.NoError(t, err, "CreateTemp")
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(local, h), r)
	require.NoError(t, err, "write blob")
	digest, err := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	require.NoError(t, err)
	_, err = cache.Adopt(local, digest)
	require.NoError(t, err, "Adopt")
	return digest, n
}
