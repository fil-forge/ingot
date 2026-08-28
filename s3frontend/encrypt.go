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
// CIPHERTEXT digest, and the CEK wrapped twice — by the region key provider,
// bound to (space, digest), into the blob's blob_encryption_params row (the
// wrap every read uses), and to the tenant's X25519 wrap key as the
// envelope's single COSE recipient (the insurance copy: recoverable from the
// envelope alone with the tenant's private key, no region involved).
//
// The tenant recipient is resolved once per request (tenantkey.Source),
// before any plaintext is spooled, and a request that cannot obtain it
// fails: a region-only wrap is the backstop-less design the RFC rejected.
// Its kid is the key's fingerprint, so the envelope names the exact key it
// was wrapped to whatever Hilt's DID document later says.
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
	spool      blockstore.BlobWriter
	keys       regionkey.Provider
	space      did.DID
	recipients []fee.Recipient
	results    map[string]encWrite
}

// encWrite is one encrypted blob's write-side state, keyed by ciphertext
// digest in encryptingBlobWriter.results.
type encWrite struct {
	desc       fee.BodyDescriptor
	wrapped    regionkey.WrappedKey
	storedSize int64 // envelope header + ciphertext, the spooled byte count
}

func newEncryptingBlobWriter(spool blockstore.BlobWriter, keys regionkey.Provider, space did.DID, recipients []fee.Recipient) *encryptingBlobWriter {
	return &encryptingBlobWriter{spool: spool, keys: keys, space: space, recipients: recipients, results: map[string]encWrite{}}
}

// tenantRecipient resolves the requesting tenant's wrap key and returns it
// as the envelope recipient for this request's blobs. The error is the
// write's error: nothing is spooled without a recipient.
func (b *Backend) tenantRecipient(ctx context.Context) (fee.Recipient, error) {
	if b.tenantKeys == nil {
		return nil, errors.New("s3frontend: tenant key source not configured (TenantKeys)")
	}
	kid, pub, err := b.tenantKeys.WrapKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: tenant recipient: %w", err)
	}
	return fee.NewECDHESRecipient([]byte(kid), pub), nil
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
	// The descriptor is complete on return, before any plaintext is read.
	rc, desc, err := fee.EncryptWithCEK(plaintext, cek, w.recipients)
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
