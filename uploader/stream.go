package uploader

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// ErrUnsupportedDigestCode reports an add the upload service refused because
// it, or every storage node it tried, cannot add a blob by digest code. The
// caller hashes the blob first and uploads it with UploadBlob instead.
var ErrUnsupportedDigestCode = errors.New("uploader: the upload service cannot add a blob by digest code")

// StreamedBlob is a blob added before its digest is known, so its bytes can
// go to the provider as they are written. Once they are sent, the blob is
// parked: [StreamedBlob.Parked] gives the UploadedBlob that ConcludeBlobs
// accepts and AbortBlob abandons, like one from UploadBlob with
// WithConclude(false).
type StreamedBlob struct {
	// Size is the byte count the blob was allocated for, which its body must
	// match.
	Size int64
	// AddTask is the /blob/add task link: the cause an abort names while the
	// digest is still unknown. AcceptTask is the /blob/accept task link.
	AddTask    cid.Cid
	AcceptTask cid.Cid
	// PutInvocation is the issued /http/put invocation; sensitive, like a
	// park's.
	PutInvocation []byte

	// add is the edge client's handle on the allocation, set by Forge.
	add forgeclient.StreamingAdd
}

// Parked returns the parked blob the sent bytes leave, digest being the
// digest of those bytes.
func (b StreamedBlob) Parked(digest multihash.Multihash) UploadedBlob {
	return UploadedBlob{
		Digest:        digest,
		Size:          b.Size,
		AddTask:       b.AddTask,
		AcceptTask:    b.AcceptTask,
		PutInvocation: b.PutInvocation,
	}
}

// StreamingBodyUploader uploads an object-body blob while it is being written:
// StartBlob allocates by size and hash function, PutBlob sends the bytes, and
// the caller, which hashed them on the way, finishes the upload with the
// digest through the DeferredBodyUploader it also implements.
type StreamingBodyUploader interface {
	DeferredBodyUploader
	// StartBlob adds a blob of size bytes whose SHA2-256 digest is computed as
	// it is sent. It fails wrapping [ErrUnsupportedDigestCode] when the upload
	// service cannot add by digest code.
	StartBlob(ctx context.Context, space did.DID, size int64) (StreamedBlob, error)
	// PutBlob sends body, exactly blob.Size bytes, to the blob's allocation.
	PutBlob(ctx context.Context, blob StreamedBlob, body io.Reader) error
}

// StartBlob runs on the request ctx, like UploadBlob, and needs its proof
// store for the same reasons.
func (u *Forge) StartBlob(ctx context.Context, space did.DID, size int64) (StreamedBlob, error) {
	store, ok := reqscope.ProofStore(ctx)
	if !ok {
		return StreamedBlob{}, fmt.Errorf("uploader: no request-scoped proof store for space %s (IAM layer did not attach one)", space)
	}
	u.captureShipProofs(space, store)

	add, err := u.client.BlobAddByDigestCode(ctx, space, uint64(size), forgeclient.WithProofStore(store))
	if err != nil {
		if forgeclient.IsUnsupportedDigestCode(err) {
			return StreamedBlob{}, fmt.Errorf("uploader: start blob: %w: %w", ErrUnsupportedDigestCode, err)
		}
		return StreamedBlob{}, fmt.Errorf("uploader: start blob: %w", err)
	}
	return StreamedBlob{
		Size:          size,
		AddTask:       add.AddTask,
		AcceptTask:    add.AcceptTask,
		PutInvocation: add.PutInvocation,
		add:           add,
	}, nil
}

func (u *Forge) PutBlob(ctx context.Context, blob StreamedBlob, body io.Reader) error {
	if err := u.client.PutStream(ctx, blob.add, body, forgeclient.WithPutClient(u.putClient)); err != nil {
		return fmt.Errorf("uploader: put blob: %w", err)
	}
	return nil
}

var _ StreamingBodyUploader = (*Forge)(nil)
