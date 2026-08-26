package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-fee/aesstream"

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
// decryptor needs — the header length, STREAM base nonce, chunk size, AAD,
// and the region-wrapped CEK — so a ranged GET unwraps the CEK through the
// region key provider and fetches ONLY the ciphertext chunks its plaintext
// range overlaps, never the envelope header.
//
// Coordinates: BlobRef.Offset/Length are plaintext spans (the manifest
// describes the object's plaintext layout), while BlobRef.Digest names the
// stored — for an encrypted blob, ciphertext — bytes. The bodyOpener is
// where the two meet: it maps a plaintext range of one blob onto the
// contiguous ciphertext span that serves it (aesstream.CiphertextRange) and
// decrypts that span as it streams (aesstream.SpanReader).

// bodyOpener returns the BlobRangeOpener for one request over body: the
// plain opener when nothing is encrypted, or a decrypting opener carrying
// the prefetched encryption state of every encrypted blob. Prefetching here
// (rather than at first Read) surfaces missing-row/missing-location problems
// as request errors, before any response headers are written.
//
// The encryption-params store and the region key provider are required
// dependencies (validated at server construction): which implementation
// backs the provider — OpenBao in production, in-process for tests and
// development — is configuration, but bucket encryption itself is not
// optional. The guard below only turns a mis-built harness into a clear
// error instead of a nil-pointer panic.
func (b *Backend) bodyOpener(ctx context.Context, space did.DID, body msbucket.Body) (msbucket.BlobRangeOpener, error) {
	if b.regionKeys == nil || b.encParams == nil {
		return nil, errors.New("s3frontend: encryption dependencies not configured (EncParams, RegionKeys)")
	}
	plain := msbucket.NewPlainOpener(b.read)

	var enc map[string]encBlob
	for _, ref := range body.Blobs {
		key := string(ref.Digest)
		if _, ok := enc[key]; ok {
			continue
		}
		params, err := b.encParams.GetEncryptionParams(ctx, space, ref.Digest)
		if errors.Is(err, registry.ErrNotFound) {
			continue // stored as plaintext
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
		if enc == nil {
			enc = make(map[string]encBlob)
		}
		enc[key] = encBlob{params: *params, storedSize: loc.Size}
	}
	if len(enc) == 0 {
		return plain, nil
	}
	return &decryptingOpener{
		plain: plain,
		read:  b.read,
		keys:  b.regionKeys,
		enc:   enc,
	}, nil
}

// encBlob is one encrypted blob's prefetched decryption state.
type encBlob struct {
	params     registry.BlobEncryptionParams
	storedSize int64 // whole stored blob: envelope header + ciphertext
}

// decryptingOpener serves each blob's plaintext range, dispatching per blob:
// blobs with no encryption row go through the plain opener untouched; for
// encrypted blobs it unwraps the CEK, fetches the covering ciphertext span,
// and decrypts it in a streaming pass.
type decryptingOpener struct {
	plain msbucket.BlobRangeOpener
	read  blockstore.BlobReader
	keys  regionkey.Provider
	enc   map[string]encBlob
}

func (o *decryptingOpener) OpenBlobRange(ctx context.Context, space did.DID, ref msbucket.BlobRef, off, length int64) (io.ReadCloser, error) {
	e, ok := o.enc[string(ref.Digest)]
	if !ok {
		return o.plain.OpenBlobRange(ctx, space, ref, off, length)
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
	// NewSpanReader copies the key into its cipher at construction, so the
	// raw CEK is wiped before this function returns, on every path.
	defer clear(cek)

	// Map the plaintext range onto the single contiguous ciphertext span
	// that serves it. Offsets from CiphertextRange are relative to the
	// ciphertext stream, which starts HeaderLen bytes into the stored blob.
	ctSize := e.storedSize - e.params.HeaderLen
	start, n, plainLen, err := aesstream.CiphertextRange(ctSize, int(e.params.ChunkSize), off, length)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: ciphertext range for blob %x: %w", ref.Digest, err)
	}
	if plainLen < length {
		// The manifest's plaintext span extends past what this ciphertext
		// decrypts to: the registry row and the manifest disagree.
		return nil, fmt.Errorf("s3frontend: blob %x: plaintext range [%d,+%d) exceeds the %d bytes its ciphertext holds",
			ref.Digest, off, length, plainLen)
	}
	if n == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	span, err := blockstore.OpenBlobRangeOf(ctx, o.read, space, ref.Digest, e.params.HeaderLen+start, n)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: fetch ciphertext span of blob %x: %w", ref.Digest, err)
	}
	sr, err := aesstream.NewSpanReader(span, aesstream.Config{
		Key:       cek,
		BaseNonce: e.params.BaseNonce,
		AAD:       e.params.AAD,
		ChunkSize: int(e.params.ChunkSize),
	}, ctSize, off, length)
	if err != nil {
		_ = span.Close()
		return nil, fmt.Errorf("s3frontend: decrypt blob %x: %w", ref.Digest, err)
	}
	// A chunk that fails authentication mid-stream (tampered or stale
	// ciphertext) surfaces from Read as aesstream.ErrCorrupted, terminating
	// the response body — the encryption RFC's stated behavior.
	return readerCloser{Reader: sr, Closer: span}, nil
}

// readerCloser pairs the decrypted plaintext reader with the ciphertext
// stream it consumes, so closing the body releases the underlying fetch.
type readerCloser struct {
	io.Reader
	io.Closer
}
