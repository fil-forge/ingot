package inmem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
)

// Provider is an in-memory stand-in for the storage provider behind ingot's
// network tier. It accepts every upload, like NopUploader, and also keeps the
// bytes it was given, so a test can serve reads from it exactly as the network
// read tier would serve them from the real provider. It is both the upload
// sink and the base of the layered read path.
//
// Every blob is checked against the digest it is stored under: a body that does
// not hash to its digest is refused, as a real provider refuses it.
type Provider struct {
	NopUploader

	mu    sync.Mutex
	blobs map[string][]byte // keyed by string(digest)
}

// NewProvider returns an empty Provider.
func NewProvider() *Provider {
	return &Provider{blobs: map[string][]byte{}}
}

// Put stores data under digest. It fails when data does not hash to digest.
func (p *Provider) Put(digest multihash.Multihash, data []byte) error {
	dm, err := multihash.Decode(digest)
	if err != nil {
		return fmt.Errorf("inmem: decode digest: %w", err)
	}
	sum, err := multihash.Sum(data, dm.Code, dm.Length)
	if err != nil {
		return fmt.Errorf("inmem: hash blob: %w", err)
	}
	if !bytes.Equal(sum, digest) {
		return fmt.Errorf("inmem: blob does not hash to digest %x", []byte(digest))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blobs[string(digest)] = bytes.Clone(data)
	return nil
}

// PutFile stores the contents of the file at path under digest.
func (p *Provider) PutFile(digest multihash.Multihash, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("inmem: read blob file: %w", err)
	}
	return p.Put(digest, data)
}

// Tamper rewrites the stored bytes of a blob in place without re-checking them
// against the digest, so a test can serve a provider that returns corrupted
// bytes. It fails when the blob is not stored.
func (p *Provider) Tamper(digest multihash.Multihash, mutate func(data []byte)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, ok := p.blobs[string(digest)]
	if !ok {
		return blockstore.ErrNotFound
	}
	mutate(data)
	return nil
}

// Has reports whether the blob is stored.
func (p *Provider) Has(digest multihash.Multihash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.blobs[string(digest)]
	return ok
}

func (p *Provider) get(digest multihash.Multihash) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, ok := p.blobs[string(digest)]
	return data, ok
}

// UploadBlob stores the bytes at localPath, then accepts the blob as
// NopUploader does.
func (p *Provider) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string, opts ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	if err := p.PutFile(digest, localPath); err != nil {
		return uploader.UploadedBlob{}, err
	}
	return p.NopUploader.UploadBlob(ctx, space, digest, size, localPath, opts...)
}

// RemoveBlob drops the blob, as the provider does when its last claim goes.
func (p *Provider) RemoveBlob(_ context.Context, _ did.DID, digest multihash.Multihash) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.blobs, string(digest))
	return nil
}

// GetBlock serves a stored blob as a raw block. A CID whose multihash names no
// stored blob is ErrNotFound.
func (p *Provider) GetBlock(_ context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
	data, ok := p.get(c.Hash())
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	return block.NewBlockWithCid(bytes.Clone(data), c)
}

// OpenBlob streams a stored blob, or reports ErrNotFound.
func (p *Provider) OpenBlob(_ context.Context, _ did.DID, digest multihash.Multihash) (io.ReadCloser, error) {
	data, ok := p.get(digest)
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// OpenBlobRange streams stored bytes [start, end] (inclusive) of a blob. An end
// past the stored bytes yields a shorter stream, and a start past them an empty
// one, as a ranged retrieval does.
func (p *Provider) OpenBlobRange(_ context.Context, _ did.DID, digest multihash.Multihash, start, end int64) (io.ReadCloser, error) {
	data, ok := p.get(digest)
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("inmem: invalid range [%d, %d]", start, end)
	}
	size := int64(len(data))
	if start >= size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if end >= size {
		end = size - 1
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

// Compile-time guarantees: Provider is the upload sink and the network tier.
var (
	_ uploader.Uploader             = (*Provider)(nil)
	_ uploader.DeferredBodyUploader = (*Provider)(nil)
	_ uploader.BlobRemover          = (*Provider)(nil)
	_ blockstore.BlockReader        = (*Provider)(nil)
	_ blockstore.BlobReader         = (*Provider)(nil)
	_ blockstore.BlobRangeReader    = (*Provider)(nil)
)
