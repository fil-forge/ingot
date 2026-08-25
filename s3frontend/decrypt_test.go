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
}

// encChunkSize is deliberately the FEE minimum so short test blobs still
// span several STREAM chunks and ranges exercise chunk-boundary math.
const encChunkSize = aesstream.MinChunkSize

// newEncFixture splits plaintext into blobs of blobSize bytes, encrypts each
// into a FEE envelope stored in a fresh spool under its ciphertext digest,
// records the encryption params + location rows, and returns a Backend wired
// with just the pieces the read path uses. plainBlobs marks blob indexes to
// leave unencrypted, proving plain and encrypted blobs mix in one body.
func newEncFixture(t *testing.T, plaintext []byte, blobSize int, plainBlobs ...int) *encFixture {
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

	plain := map[int]bool{}
	for _, i := range plainBlobs {
		plain[i] = true
	}

	var blobs []msbucket.BlobRef
	for i, off := 0, 0; off < len(plaintext); i++ {
		end := off + blobSize
		if end > len(plaintext) {
			end = len(plaintext)
		}
		piece := plaintext[off:end]

		var digest multihash.Multihash
		if plain[i] {
			digest, _, err = spool.WriteBlob(ctx, bytes.NewReader(piece))
			if err != nil {
				t.Fatalf("WriteBlob (plain): %v", err)
			}
		} else {
			cek := randBytes(t, 32)
			rc, desc, err := fee.EncryptWithCEK(bytes.NewReader(piece), cek, nil, fee.WithChunkSize(encChunkSize))
			if err != nil {
				t.Fatalf("EncryptWithCEK: %v", err)
			}
			var n int64
			digest, n, err = spool.WriteBlob(ctx, rc)
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
				BaseNonce:        desc.BaseNonce,
				ChunkSize:        int64(desc.ChunkSize),
				AAD:              desc.AAD,
			}); err != nil {
				t.Fatalf("PutEncryptionParams: %v", err)
			}
			if err := mem.PutLocation(ctx, registry.BlobLocation{
				Space: space, Digest: digest, Provider: "did:web:piri.test", URL: "http://piri.test/blob", Size: n,
			}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
		}
		blobs = append(blobs, msbucket.BlobRef{Digest: digest, Offset: int64(off), Length: int64(len(piece))})
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
	}
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
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

// Plain and encrypted blobs mix in one body: the opener dispatches per blob
// on row existence.
func TestDecryptingRead_MixedPlainAndEncrypted(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000, 1) // blob index 1 stays plaintext

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	got, err := io.ReadAll(msbucket.OpenBodyRange(ctx, opener, fx.space, fx.body, 9500, 20500))
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(got, fx.plaintext[9500:20501]) {
		t.Fatal("mixed-body range mismatch")
	}
}

// The network-tier shape: a read store WITHOUT the BlobRangeReader capability
// and whose readers cannot seek — the ciphertext span is served through the
// OpenBlob fallback (read-and-discard), as the Forge tier would absent ranged
// retrieval.
func TestDecryptingRead_UnrangedUnseekableStore(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(24000), 10000)

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
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

	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	_, err = io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if !errors.Is(err, regionkey.ErrAuthentication) {
		t.Fatalf("transplanted-row read err = %v, want regionkey.ErrAuthentication", err)
	}
}

// With no region key provider configured the opener skips encryption lookups
// entirely and serves stored bytes — the plaintext-only deployment contract.
func TestDecryptingRead_NilProviderServesStoredBytes(t *testing.T) {
	ctx := context.Background()
	fx := newEncFixture(t, patterned(9000), 10000)

	fx.backend.regionKeys = nil
	opener, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
	if err != nil {
		t.Fatalf("bodyOpener: %v", err)
	}
	got, err := io.ReadAll(msbucket.OpenBody(ctx, opener, fx.space, fx.body))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if bytes.Equal(got, fx.plaintext[:len(got)]) {
		t.Fatal("expected raw envelope bytes, got decrypted plaintext")
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
	_, err := fx.backend.bodyOpener(ctx, fx.space, fx.body)
	if err == nil || !strings.Contains(err.Error(), "no recorded location") {
		t.Fatalf("bodyOpener err = %v, want missing-location error", err)
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
