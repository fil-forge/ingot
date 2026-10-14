package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-fee"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ucantone/did"
)

// This file is the decrypting half of the body read path (the FilOne
// encryption design's read side). An encrypted body blob is stored as a FEE
// envelope: a COSE header followed by the AES-256-GCM STREAM ciphertext of
// the blob's plaintext. The blob_encryption_params row caches everything the
// decryptor needs — the header length, the AAD (whose protected header holds
// the STREAM base nonce, chunk size and plaintext length), and the
// region-wrapped CEK — so a ranged GET unwraps the CEK through the region key
// provider and fetches ONLY the ciphertext chunks its plaintext range
// overlaps, never the envelope header.
//
// Coordinates: BlobRef.Start/End are inclusive plaintext spans (the manifest
// describes the object's plaintext layout), while BlobRef.Digest names the
// stored — for an encrypted blob, ciphertext — bytes. The bodyOpener is
// where the two meet: go-fee's fee.DecryptRangeWithCEK maps a plaintext range
// of one blob onto the contiguous ciphertext span that serves it, and
// decrypts that span as it streams. Every range in this file is HTTP-style
// inclusive [start, end].

// bodyOpener returns the BlobRangeOpener for one request over body: the
// plain opener for a body its manifest marks plaintext (stored as received
// under a "none" bucket, sse.go), else a decrypting opener carrying the
// prefetched encryption state of every blob (the plain opener alone only for
// a body with no blobs). The manifest's mark is the only thing that makes a
// body plaintext: every encrypted body blob is written with its params row
// committed before the manifest, so a referenced blob without a row is an
// error — never "stored as plaintext": failing open there would stream raw
// envelope bytes under a 200. Prefetching here (rather than at first Read)
// surfaces missing-row/missing-location problems as request errors, before
// any response headers are written.
//
// The encryption-params store and the region key provider are required
// dependencies (validated at server construction): which implementation
// backs the provider — OpenBao in production, in-process for tests and
// development — is configuration, but bucket encryption itself is not
// optional. The guard below only turns a mis-built harness into a clear
// error instead of a nil-pointer panic.
func (b *Backend) bodyOpener(ctx context.Context, space did.DID, body msbucket.Body, plaintext bool) (msbucket.BlobRangeOpener, error) {
	if b.regionKeys == nil || b.encParams == nil {
		return nil, errors.New("s3frontend: encryption dependencies not configured (EncParams, RegionKeys)")
	}
	plain := msbucket.NewPlainOpener(b.read)
	if plaintext {
		return plain, nil
	}

	var enc map[string]encBlob
	for _, ref := range body.Blobs {
		key := string(ref.Digest)
		if _, ok := enc[key]; ok {
			continue
		}
		params, err := b.encParams.GetEncryptionParams(ctx, space, ref.Digest)
		if errors.Is(err, registry.ErrNotFound) {
			// Fail closed: the row is the blob's decryption key material. A
			// referenced blob without one is unreadable (a shredded or lost
			// row), not plaintext.
			return nil, fmt.Errorf("s3frontend: blob %x has no encryption-params row; refusing to serve ciphertext", ref.Digest)
		}
		if err != nil {
			return nil, fmt.Errorf("s3frontend: encryption params for blob %x: %w", ref.Digest, err)
		}
		// The stored (ciphertext) size: recorded at accept in blob_locations,
		// out of reach of whoever can write to the object store. The chunk
		// geometry is derived from it, so a truncated or padded stored blob
		// surfaces as a decrypt error rather than as silently wrong bytes.
		loc, err := b.locations.GetLocation(ctx, space, ref.Digest)
		if err != nil {
			return nil, fmt.Errorf("s3frontend: encrypted blob %x has no recorded location: %w", ref.Digest, err)
		}
		// The row must describe a FEE body, and an envelope that declares its
		// plaintext length must agree with the stored size, so a bad row or a
		// truncated or padded blob fails here, before any response headers
		// are written.
		desc := fee.BodyDescriptor{HeaderLen: params.HeaderLen, AAD: params.AAD}
		if _, err := desc.PlaintextSize(loc.Size); err != nil {
			return nil, fmt.Errorf("s3frontend: blob %x: %w", ref.Digest, err)
		}
		if enc == nil {
			enc = make(map[string]encBlob)
		}
		enc[key] = encBlob{desc: desc, params: *params, storedSize: loc.Size}
	}
	if len(enc) == 0 {
		return plain, nil
	}
	return &decryptingOpener{
		read: b.read,
		keys: b.regionKeys,
		enc:  enc,
	}, nil
}

// encBlob is one encrypted blob's prefetched decryption state.
type encBlob struct {
	desc       fee.BodyDescriptor
	params     registry.BlobEncryptionParams
	storedSize int64 // whole stored blob: envelope header + ciphertext
}

// decryptingOpener serves each blob's plaintext range: it unwraps the CEK,
// fetches the covering ciphertext span, and decrypts it in a streaming pass.
// bodyOpener prefetched state for every blob of the body, so a digest absent
// from enc is a bug — fail closed rather than fall through to raw bytes.
type decryptingOpener struct {
	read blockstore.BlobReader
	keys regionkey.Provider
	enc  map[string]encBlob
}

func (o *decryptingOpener) OpenBlobRange(ctx context.Context, space did.DID, ref msbucket.BlobRef, start, end int64) (io.ReadCloser, error) {
	e, ok := o.enc[string(ref.Digest)]
	if !ok {
		return nil, fmt.Errorf("s3frontend: blob %x has no prefetched encryption state; refusing to serve ciphertext", ref.Digest)
	}

	// The CEK's wrap is context-bound to (space, digest): a row transplanted
	// under another blob fails authentication here rather than decrypting.
	cek, err := o.keys.Unwrap(ctx,
		regionkey.BindingContext{Space: space, Digest: ref.Digest},
		regionkey.WrappedKey{
			Version:    regionkey.KeyVersion(e.params.RegionKeyVersion),
			Ciphertext: e.params.RegionWrappedCEK,
		})
	if err != nil {
		return nil, fmt.Errorf("s3frontend: unwrap CEK for blob %x: %w", ref.Digest, err)
	}
	// DecryptRangeWithCEK copies the key into its cipher at construction, so
	// the raw CEK is wiped before this function returns, on every path.
	defer clear(cek)

	// No ciphertext is read until the first Read, so the reader is built
	// first, and its ciphertext span then fetched in one ranged request.
	src := &spanReaderAt{}
	rr, err := fee.DecryptRangeWithCEK(src, e.storedSize, cek, start, end, &e.desc)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: decrypt blob %x: %w", ref.Digest, err)
	}
	if rr.Len() < end-start+1 {
		// The manifest's plaintext span extends past what this ciphertext
		// decrypts to: the registry row and the manifest disagree. (go-fee
		// clamps such a range rather than rejecting it.)
		_ = rr.Close()
		return nil, fmt.Errorf("s3frontend: blob %x: plaintext range [%d,%d] exceeds the %d bytes its ciphertext holds",
			ref.Digest, start, end, rr.Size())
	}
	spanStart, spanEnd := rr.CiphertextSpan()
	span, err := blockstore.OpenBlobRangeOf(ctx, o.read, space, ref.Digest, spanStart, spanEnd)
	if err != nil {
		_ = rr.Close()
		return nil, fmt.Errorf("s3frontend: fetch ciphertext span of blob %x: %w", ref.Digest, err)
	}
	src.span, src.pos = span, spanStart
	// Read fails mid-stream, ending the response body, on a chunk that fails
	// authentication (tampered or stale ciphertext: aesstream.ErrCorrupted)
	// or a fetch that ends before its span does.
	return decryptedRange{RangeReader: rr, span: span}, nil
}

// spanReaderAt serves go-fee's reads of one blob's ciphertext span from a
// single ranged fetch of that span. fee.DecryptRangeWithCEK reads the span in
// order, each read starting where the last ended and the whole covering
// exactly RangeReader.CiphertextSpan, so a read at any other offset is a bug
// and fails rather than returning the wrong bytes.
type spanReaderAt struct {
	span io.Reader // the fetched span; nil until it is opened
	pos  int64     // blob-absolute offset of span's next byte
}

func (s *spanReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if s.span == nil {
		return 0, errors.New("s3frontend: ciphertext read before its span was fetched")
	}
	if off != s.pos {
		return 0, fmt.Errorf("s3frontend: ciphertext read at offset %d, want %d", off, s.pos)
	}
	n, err := io.ReadFull(s.span, p)
	s.pos += int64(n)
	return n, err
}

// decryptedRange is one blob's decrypted plaintext range. Closing it releases
// both the decryptor and the ciphertext fetch it reads from.
type decryptedRange struct {
	*fee.RangeReader
	span io.Closer
}

func (d decryptedRange) Close() error {
	return errors.Join(d.RangeReader.Close(), d.span.Close())
}

// blobPlaintextLen reports how many plaintext bytes a stored, encrypted
// blob decrypts to: the FEE geometry derived from its encryption-params row
// and the stored (envelope) byte count, checked against the plaintext length
// the envelope declares. Every encrypted body blob has a row;
// a missing one is an error — treating it as plaintext would record the
// envelope byte count as the manifest span, permanently corrupting the
// object's Size and every range GET. Multipart Complete uses this to rebuild
// the manifest's plaintext spans from upload_intents' stored sizes (a
// plaintext session's stored sizes are its spans already).
func (b *Backend) blobPlaintextLen(ctx context.Context, space did.DID, digest multihash.Multihash, storedSize int64) (int64, error) {
	params, err := b.encParams.GetEncryptionParams(ctx, space, digest)
	if errors.Is(err, registry.ErrNotFound) {
		return 0, fmt.Errorf("blob %x has no encryption-params row", digest)
	}
	if err != nil {
		return 0, fmt.Errorf("encryption params for blob %x: %w", digest, err)
	}
	plainLen, err := fee.BodyDescriptor{HeaderLen: params.HeaderLen, AAD: params.AAD}.PlaintextSize(storedSize)
	if err != nil {
		return 0, fmt.Errorf("blob %x: %d stored bytes do not fit its FEE geometry: %w", digest, storedSize, err)
	}
	return plainLen, nil
}
