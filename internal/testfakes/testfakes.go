// Package testfakes stands in for the network in tests: a provider that
// accepts every blob and serves none, an upload service that counts nothing,
// and a bucket authority in place of hilt. The registry is real: tests run it on
// Postgres (see internal/pgtest).
package testfakes

import (
	"context"
	"io"

	"github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/uploader"
)

// Authority stands in for hilt's bucket management: it mints a space for
// each bucket created, keeps no policies, and lists no buckets.
type Authority struct{}

func (Authority) CreateBucket(context.Context, s3.Request) (did.DID, error) {
	id, err := ed25519.Generate()
	if err != nil {
		return did.Undef, err
	}
	return id.KeyDID(), nil
}

func (Authority) DeleteBucket(context.Context, s3.Request) error { return nil }

func (Authority) ListBuckets(context.Context, s3.Request) (*bucket.ListOK, error) {
	return &bucket.ListOK{}, nil
}

// PolicyDocument reports no policy, and refuses to set one.
func (Authority) PolicyDocument(_ context.Context, req s3.Request, _ []byte) (*bucket.PolicyOK, error) {
	if req.Method == "PUT" {
		return nil, bucketauthority.ErrUnsupported
	}
	return nil, bucketauthority.ErrNoPolicy
}

// NopBaseReader is the base tier of the layered read path when there is
// no network: every miss past the local log returns ErrNotFound.
type NopBaseReader struct{}

func (NopBaseReader) GetBlock(_ context.Context, _ did.DID, _ cid.Cid) (block.Block, error) {
	return nil, blockstore.ErrNotFound
}

// OpenBlob is the streaming-read counterpart: with no network tier, an evicted
// body blob is unrecoverable, so tests that read back what they wrote keep it
// local.
func (NopBaseReader) OpenBlob(_ context.Context, _ did.DID, _ multihash.Multihash) (io.ReadCloser, error) {
	return nil, blockstore.ErrNotFound
}

// NopUploader is a no-op upload sink. SubmitShard ships nothing (a catalog
// plane is marked shipped without touching the network); UploadBlob accepts a
// body blob without touching the network, so the local copy serves all reads.
type NopUploader struct{}

func (NopUploader) SubmitShard(_ context.Context, _ blockstore.Plane, _ did.DID, _ uploader.CARShard) (uploader.BlobLocation, multihash.Multihash, error) {
	return uploader.BlobLocation{}, nil, nil
}

// CaptureShipAuthority is a no-op: SubmitShard needs no authority.
func (NopUploader) CaptureShipAuthority(_ context.Context, _ did.DID) {}

// UploadBlob accepts immediately, even with WithConclude(false) — there is
// no network to park on, so the deferred flow degenerates to the synchronous
// one and reads keep coming from local disk.
func (NopUploader) UploadBlob(_ context.Context, _ did.DID, digest multihash.Multihash, size int64, _ string, _ ...uploader.UploadOption) (uploader.UploadedBlob, error) {
	return uploader.UploadedBlob{Digest: digest, Size: size, Location: &uploader.BlobLocation{Size: size}}, nil
}

func (NopUploader) RemoveBlob(_ context.Context, _ did.DID, _ multihash.Multihash) error { return nil }

func (NopUploader) ConcludeBlobs(_ context.Context, _ did.DID, parked []uploader.UploadedBlob) ([]*uploader.BlobLocation, error) {
	locations := make([]*uploader.BlobLocation, len(parked))
	for i, p := range parked {
		locations[i] = &uploader.BlobLocation{Size: p.Size}
	}
	return locations, nil
}

func (NopUploader) AbortBlob(_ context.Context, _ did.DID, _ cid.Cid) error {
	return nil
}

// CaptureAuthority, RegisterUpload and RetractUpload keep no content-entry
// list: there is no upload service to count against, and nothing in the suite
// reads one back. The captured authority is an empty container, enough for a
// queued task to carry.
func (NopUploader) CaptureAuthority(_ context.Context, _ did.DID, _ ucan.Command) ([]byte, error) {
	return container.Encode(container.RawGzip, container.New())
}

// PrepareAuthority keeps whatever the row holds: there is no upload service to
// authorize against, so nothing expires.
func (NopUploader) PrepareAuthority(_ context.Context, _ did.DID, _ ucan.Command, stored []byte) ([]byte, bool, error) {
	return stored, false, nil
}

func (NopUploader) RegisterUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	return make([]error, len(changes)), nil
}

func (NopUploader) RetractUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	return make([]error, len(changes)), nil
}

var (
	_ bucketauthority.BucketAuthority = Authority{}
	_ blockstore.BlockReader          = NopBaseReader{}
	_ blockstore.BlobReader           = NopBaseReader{}
	_ uploader.Uploader               = NopUploader{}
	_ uploader.BodyUploader           = NopUploader{}
	_ uploader.DeferredBodyUploader   = NopUploader{}
	_ uploader.BlobRemover            = NopUploader{}
	_ uploader.UploadRegistrar        = NopUploader{}
)
