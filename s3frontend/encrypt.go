package s3frontend

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// This file is the encrypting half of the body write path (the FilOne
// encryption design's write side; the read side is decrypt.go). Every body
// blob is encrypted at ingest — except under a bucket whose default
// encryption is "none" (sse.go), whose blobs the same writer stores as
// received. Encrypted: a fresh CEK per blob, the plaintext streamed
// through FEE into a COSE envelope, the envelope stored under its
// CIPHERTEXT digest, and the CEK wrapped twice — by the region key provider,
// bound to (space, digest), into the blob's blob_encryption_params row (the
// wrap every read uses), and to the tenant's X25519 wrap key as the
// envelope's single COSE recipient (the insurance copy: recoverable from the
// envelope alone with the tenant's private key, no region involved).
//
// The tenant recipient is resolved once per request (tenantkey.Source),
// before any plaintext is stored, and a request that cannot obtain it
// fails: a region-only wrap is the backstop-less design the RFC rejected.
// Its kid is the key's fingerprint, so the envelope names the exact key it
// was wrapped to whatever Hilt's DID document later says.
//
// A deliberate consequence (per the RFC): content dedup is gone for
// encrypted bodies. A fresh CEK per encryption event makes every ciphertext
// digest unique, so re-uploading identical plaintext creates a new blob, row
// and claim.

// bodyWriter is the bucket.SizedBlobWriter the write path hands to
// SplitSizedBody. Encrypting (the default), it encrypts each plaintext piece
// into a FEE envelope, streams the envelope to its provider under its
// ciphertext digest (see WriteSizedBlob), and wraps the CEK. Every manifest
// span is plaintext-based; only the digest names ciphertext. The per-digest
// encryption state (descriptor, wrapped CEK, stored size) accumulates in
// results for splitBody to persist.
//
// A plaintext writer streams each piece as received, under the digest of its
// own bytes, and records only the stored size: no key, no wrap, no
// blob_encryption_params row. Two plaintext bodies with the same bytes share
// a digest, which the encrypting path never produces.
//
// Not safe for concurrent use; the write path drives one instance per body,
// sequentially.
type bodyWriter struct {
	// plaintext stores each blob as received; keys and recipients are then
	// unused.
	plaintext  bool
	keys       regionkey.Provider
	space      did.DID
	recipients []fee.Recipient
	results    map[string]encWrite

	// stream sends each blob to its provider as it is written.
	stream uploader.StreamingBodyUploader
	logger *zap.Logger
	// cache, when set, takes a copy of each blob as it streams, which the
	// write adopts once the provider holds the blob; nil keeps none.
	cache *blockstore.BlobCache
}

// encWrite is one blob's write-side state, keyed by stored digest in
// bodyWriter.results. A plaintext blob records no key material.
type encWrite struct {
	desc       fee.BodyDescriptor
	wrapped    regionkey.WrappedKey
	storedSize int64 // the stored byte count: envelope header + ciphertext, or the plaintext
	// streamed is the upload the blob went to: parked on its provider,
	// awaiting its conclude.
	streamed uploader.StreamedBlob
	// local is the blob's copy in the cache, not yet adopted; nil when the
	// write keeps none.
	local *blockstore.TempBlob
}

func newEncryptingBlobWriter(stream uploader.StreamingBodyUploader, keys regionkey.Provider, space did.DID, recipients []fee.Recipient) *bodyWriter {
	return &bodyWriter{stream: stream, keys: keys, space: space, recipients: recipients, results: map[string]encWrite{}, logger: zap.NewNop()}
}

// newPlainBlobWriter is the writer for a body stored as received.
func newPlainBlobWriter(stream uploader.StreamingBodyUploader, space did.DID) *bodyWriter {
	return &bodyWriter{stream: stream, plaintext: true, space: space, results: map[string]encWrite{}, logger: zap.NewNop()}
}

// tenantRecipient resolves the requesting tenant's wrap key and returns it
// as the envelope recipient for this request's blobs. The error is the
// write's error: nothing is stored without a recipient.
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

// record wraps the blob's CEK and keeps its encryption state for splitBody.
// A plaintext blob has no CEK; its stored size is all that is kept.
func (w *bodyWriter) record(ctx context.Context, digest multihash.Multihash, cek []byte, res encWrite) error {
	if _, ok := w.results[string(digest)]; ok {
		// The same bytes twice in one body, which a body stored as received
		// can hold: the first record stands, and the provider needs the
		// bytes once. The second allocation is given up now rather than
		// left for the provider to expire.
		w.abandon(ctx, res.streamed)
		res.local.Discard()
		return nil
	}
	if !w.plaintext {
		wrapped, err := w.keys.Wrap(ctx, regionkey.BindingContext{Space: w.space, Digest: digest}, cek)
		if err != nil {
			res.local.Discard()
			return fmt.Errorf("s3frontend: wrap CEK for blob %x: %w", digest, err)
		}
		res.wrapped = wrapped
	}
	w.results[string(digest)] = res
	return nil
}

// params renders the recorded encryption state of one blob as its
// blob_encryption_params row. A plaintext writer has none to render.
func (w *bodyWriter) params(space did.DID, digest multihash.Multihash) (registry.BlobEncryptionParams, error) {
	res, ok := w.results[string(digest)]
	if !ok || w.plaintext {
		return registry.BlobEncryptionParams{}, fmt.Errorf("s3frontend: no encryption state recorded for blob %x", digest)
	}
	return registry.BlobEncryptionParams{
		Space:            space,
		Digest:           digest,
		RegionWrappedCEK: res.wrapped.Ciphertext,
		RegionKeyVersion: string(res.wrapped.Version),
		HeaderLen:        res.desc.HeaderLen,
		AAD:              res.desc.AAD,
	}, nil
}

// storedSize reports the stored (envelope) byte count of one blob.
func (w *bodyWriter) storedSize(digest multihash.Multihash) (int64, error) {
	res, ok := w.results[string(digest)]
	if !ok {
		return 0, fmt.Errorf("s3frontend: no encryption state recorded for blob %x", digest)
	}
	return res.storedSize, nil
}

// streamed returns the uploads the blobs went to, keyed by string(digest).
func (w *bodyWriter) streamed() map[string]uploader.StreamedBlob {
	out := make(map[string]uploader.StreamedBlob, len(w.results))
	for digest, res := range w.results {
		out[digest] = res.streamed
	}
	return out
}

// locals returns the blobs' cache copies not yet adopted, keyed by
// string(digest).
func (w *bodyWriter) locals() map[string]*blockstore.TempBlob {
	out := map[string]*blockstore.TempBlob{}
	for digest, res := range w.results {
		if res.local != nil {
			out[digest] = res.local
		}
	}
	return out
}

// drop discards the cache copies, for a body that failed. Its allocations
// are left for the provider to expire, as a request that died leaves them.
func (w *bodyWriter) drop() {
	for _, res := range w.results {
		res.local.Discard()
	}
}
