package s3frontend

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
)

// This file is the encrypting half of the body write path (the FilOne
// encryption design's write side; the read side is decrypt.go). Every body
// blob is encrypted at ingest: a fresh CEK per blob, the plaintext streamed
// through FEE into a COSE envelope, the envelope spooled under its
// CIPHERTEXT digest, and the CEK wrapped by the region key provider — bound
// to (space, digest) — into the blob's blob_encryption_params row.
//
// The envelope carries no COSE recipient yet (a recipient-less
// COSE_Encrypt0): the tenant/insurance recipient of the encryption RFC needs
// Hilt's wrap-key registry and DID-document publication, which do not exist.
// FEE's multi-recipient model lets new writes gain that recipient later
// without changing this layer; existing blobs would need a Tier-2
// re-encryption.
//
// A deliberate consequence (per the RFC): content dedup is gone for
// encrypted bodies. A fresh CEK per encryption event makes every ciphertext
// digest unique, so re-uploading identical plaintext creates a new blob, row
// and claim.

// encryptingBlobWriter is the blockstore.BlobWriter the write path hands to
// SplitBody: it encrypts each plaintext piece into a FEE envelope, spools
// the envelope under its ciphertext digest, and wraps the CEK. Crucially it
// returns the PLAINTEXT byte count — SplitBody's loop sentinels (n == 0
// terminates, n < max marks the last blob) and every manifest span are
// plaintext-based; only the digest names ciphertext. The per-digest
// encryption state (descriptor, wrapped CEK, stored size) accumulates in
// results for splitSpool to persist.
//
// Not safe for concurrent use; the write path drives one instance per body,
// sequentially.
type encryptingBlobWriter struct {
	spool   blockstore.BlobWriter
	keys    regionkey.Provider
	space   did.DID
	results map[string]encWrite
}

// encWrite is one encrypted blob's write-side state, keyed by ciphertext
// digest in encryptingBlobWriter.results.
type encWrite struct {
	desc       fee.BodyDescriptor
	wrapped    regionkey.WrappedKey
	storedSize int64 // envelope header + ciphertext, the spooled byte count
}

func newEncryptingBlobWriter(spool blockstore.BlobWriter, keys regionkey.Provider, space did.DID) *encryptingBlobWriter {
	return &encryptingBlobWriter{spool: spool, keys: keys, space: space, results: map[string]encWrite{}}
}

// WriteBlob implements blockstore.BlobWriter over the encrypting pipeline.
// It preserves the plain writer's contract exactly: an empty r stores
// nothing and returns (nil, 0, nil), and n is the number of PLAINTEXT bytes
// consumed from r.
func (w *encryptingBlobWriter) WriteBlob(ctx context.Context, r io.Reader) (multihash.Multihash, int64, error) {
	// Probe for EOF before minting anything: an encrypted empty stream is
	// never empty on the wire (envelope + one tag-only chunk), so without
	// this the zero-byte object would grow a blob and SplitBody's n == 0
	// terminator would never fire.
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("s3frontend: read body: %w", err)
	}

	cek := make([]byte, 32)
	if _, err := rand.Read(cek); err != nil {
		return nil, 0, fmt.Errorf("s3frontend: generate CEK: %w", err)
	}
	// EncryptWithCEK internalizes the key before returning and the wrap
	// below copies it; wipe our copy on every path out.
	defer clear(cek)

	plaintext := &countingReader{r: io.MultiReader(bytes.NewReader(first[:]), r)}
	// No recipients: a recipient-less COSE_Encrypt0 (see the file comment).
	// The descriptor is complete on return, before any plaintext is read.
	rc, desc, err := fee.EncryptWithCEK(plaintext, cek, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("s3frontend: encrypt blob: %w", err)
	}
	defer rc.Close()

	digest, storedSize, err := w.spool.WriteBlob(ctx, rc)
	if err != nil {
		return nil, 0, fmt.Errorf("s3frontend: spool envelope: %w", err)
	}

	wrapped, err := w.keys.Wrap(ctx, regionkey.BindingContext{Space: w.space, Digest: digest}, cek)
	if err != nil {
		return nil, 0, fmt.Errorf("s3frontend: wrap CEK for blob %x: %w", digest, err)
	}

	w.results[string(digest)] = encWrite{desc: desc, wrapped: wrapped, storedSize: storedSize}
	return digest, plaintext.n, nil
}

// params renders the recorded encryption state of one blob as its
// blob_encryption_params row.
func (w *encryptingBlobWriter) params(space did.DID, digest multihash.Multihash) (registry.BlobEncryptionParams, error) {
	res, ok := w.results[string(digest)]
	if !ok {
		return registry.BlobEncryptionParams{}, fmt.Errorf("s3frontend: no encryption state recorded for blob %x", digest)
	}
	return registry.BlobEncryptionParams{
		Space:            space,
		Digest:           digest,
		RegionWrappedCEK: res.wrapped.Ciphertext,
		RegionKeyVersion: string(res.wrapped.Version),
		HeaderLen:        res.desc.HeaderLen,
		BaseNonce:        res.desc.BaseNonce,
		ChunkSize:        int64(res.desc.ChunkSize),
		AAD:              res.desc.AAD,
	}, nil
}

// storedSize reports the spooled (envelope) byte count of one blob.
func (w *encryptingBlobWriter) storedSize(digest multihash.Multihash) (int64, error) {
	res, ok := w.results[string(digest)]
	if !ok {
		return 0, fmt.Errorf("s3frontend: no encryption state recorded for blob %x", digest)
	}
	return res.storedSize, nil
}

// countingReader counts the bytes read through it — the plaintext length of
// the blob being encrypted.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
