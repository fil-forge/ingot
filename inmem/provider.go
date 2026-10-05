package inmem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/ucantone/did"
)

// Provider is an in-memory stand-in for the storage provider behind ingot's
// upload and read path. It takes streamed uploads the way the real one does —
// StartBlob allocates, PutBlob receives the bytes, ConcludeBlobs accepts the
// blob under the digest the caller computed, AbortBlob drops it — and serves
// accepted bytes as a BlockReader/BlobReader/BlobRangeReader, so a test can
// read back through it exactly as the network read tier would.
//
// Every blob is checked against the digest it is accepted under: a body that
// does not hash to its digest is refused, as a real provider refuses it.
type Provider struct {
	NopUploader

	mu      sync.Mutex
	blobs   map[string][]byte       // accepted, keyed by string(digest)
	pending map[string]*pendingBlob // allocated, keyed by the add task's key string
	seq     int
}

// pendingBlob is an allocation: the size it was made for and, once PutBlob has
// run, the bytes received.
type pendingBlob struct {
	size     int64
	data     []byte
	digest   multihash.Multihash // of data, once sent
	sent     bool
	accepted bool // concluded: the bytes are an accepted blob
}

// NewProvider returns an empty Provider.
func NewProvider() *Provider {
	return &Provider{blobs: map[string][]byte{}, pending: map[string]*pendingBlob{}}
}

// StartBlob allocates a blob of size bytes.
func (p *Provider) StartBlob(_ context.Context, _ did.DID, size int64) (uploader.StreamedBlob, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	sb := uploader.StreamedBlob{
		Size:          size,
		AddTask:       taskCID("add", p.seq),
		AcceptTask:    taskCID("accept", p.seq),
		PutInvocation: []byte(fmt.Sprintf("put-%d", p.seq)),
	}
	p.pending[sb.AddTask.KeyString()] = &pendingBlob{size: size}
	return sb, nil
}

func taskCID(kind string, n int) cid.Cid {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", kind, n)))
	d, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		panic(err) // a 32-byte sha2-256 digest always encodes
	}
	return cid.NewCidV1(cid.Raw, d)
}

// PutBlob receives the bytes of an allocated blob, which must be exactly the
// size it was allocated for.
func (p *Provider) PutBlob(_ context.Context, sb uploader.StreamedBlob, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	pb, ok := p.pending[sb.AddTask.KeyString()]
	if !ok {
		return fmt.Errorf("inmem: no allocation for %s", sb.AddTask)
	}
	if int64(len(data)) != pb.size {
		return fmt.Errorf("inmem: put of %d bytes to an allocation of %d", len(data), pb.size)
	}
	sum := sha256.Sum256(data)
	digest, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		return err
	}
	pb.data, pb.digest, pb.sent = data, digest, true
	return nil
}

// DigestOf returns the sha2-256 digest of the bytes sent to an allocation, or
// nil when none were sent or the allocation is gone. It is how a test names the
// blob an AbortBlob refers to by its add task alone.
func (p *Provider) DigestOf(add cid.Cid) multihash.Multihash {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pb, ok := p.pending[add.KeyString()]; ok {
		return pb.digest
	}
	return nil
}

// ConcludeBlobs accepts each parked blob under its digest, which the received
// bytes must hash to. A blob that was not sent, or does not match, comes back
// with no location and the error names it.
func (p *Provider) ConcludeBlobs(_ context.Context, _ did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	locations := make([]*uploader.BlobLocation, len(parked))
	var errs []error
	for i, blob := range parked {
		p.mu.Lock()
		pb, ok := p.pending[blob.AddTask.KeyString()]
		p.mu.Unlock()
		if !ok || !pb.sent {
			errs = append(errs, fmt.Errorf("inmem: blob %x was not sent", []byte(blob.Digest)))
			continue
		}
		// Concluding an accepted blob again answers with its location again,
		// as the upload service does for a repeated conclude.
		if err := p.Put(blob.Digest, pb.data); err != nil {
			errs = append(errs, err)
			continue
		}
		p.mu.Lock()
		pb.accepted = true
		p.mu.Unlock()
		locations[i] = &uploader.BlobLocation{Size: blob.Size}
	}
	return locations, errors.Join(errs...)
}

// AbortBlob drops an allocation and anything sent to it. An allocation already
// concluded cannot be aborted: the error wraps [uploader.ErrBlobAccepted], as
// the upload service answers.
func (p *Provider) AbortBlob(_ context.Context, _ did.DID, add cid.Cid) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pb, ok := p.pending[add.KeyString()]; ok && pb.accepted {
		return uploader.ErrBlobAccepted
	}
	delete(p.pending, add.KeyString())
	return nil
}

// Pending reports how many allocations are neither concluded nor aborted.
func (p *Provider) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, pb := range p.pending {
		if !pb.accepted {
			n++
		}
	}
	return n
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
	_ uploader.Uploader              = (*Provider)(nil)
	_ uploader.StreamingBodyUploader = (*Provider)(nil)
	_ uploader.BlobRemover           = (*Provider)(nil)
	_ blockstore.BlockReader         = (*Provider)(nil)
	_ blockstore.BlobReader          = (*Provider)(nil)
	_ blockstore.BlobRangeReader     = (*Provider)(nil)
)
