package forgeclient

import (
	"context"
	"fmt"
	"io"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// StreamingAdd is a /blob/add made before the blob's digest is known: it names
// only the hash function and the size, so the data can be sent as it is
// produced and hashed on the way. The allocation is made and the put is ready.
// Once the data is sent, the blob is parked exactly like one uploaded with
// WithConclude(false), except that its digest is the one computed while
// sending: [StreamingAdd.Parked] makes the AddedBlob that [Client.BlobConclude]
// concludes and [Client.BlobAbort] abandons.
type StreamingAdd struct {
	Size uint64
	// AddTask is the /blob/add task link, the cause an abort names.
	AddTask cid.Cid
	// AcceptTask is the /blob/accept task link the conclude polls.
	AcceptTask cid.Cid
	// PutInvocation is the issued /http/put invocation. Its metadata embeds
	// the key that signs the put receipt: sensitive, like a park's.
	PutInvocation []byte

	address blobcmds.BlobAddress
}

// Parked returns the parked blob a completed put leaves, digest being the
// digest of the bytes sent.
func (a StreamingAdd) Parked(digest multihash.Multihash) AddedBlob {
	return AddedBlob{
		Digest:        digest,
		Size:          a.Size,
		AddTask:       a.AddTask,
		AcceptTask:    a.AcceptTask,
		PutInvocation: a.PutInvocation,
	}
}

// BlobAddByDigestCode invokes /blob/add for size bytes to be hashed with
// SHA2-256 as they are sent. The invocation carries a random nonce, which the
// upload service requires: without a digest, the task link is all that sets
// one upload apart from another of the same size. An upload service or node
// that cannot add by digest code fails the add with UnsupportedDigestCode
// (see [IsUnsupportedDigestCode]); the caller then hashes the data first and
// adds it with [Client.BlobAdd].
func (c *Client) BlobAddByDigestCode(ctx context.Context, space did.DID, size uint64, options ...BlobAddOption) (StreamingAdd, error) {
	cfg := NewBlobAddConfig(options...)
	added, err := c.invokeAdd(ctx, space, blobcmds.SpecFromDigestCode(multihash.SHA2_256, size), cfg)
	if err != nil {
		return StreamingAdd{}, err
	}
	// Without a digest the node cannot recognise content it holds, so it
	// always hands back an address and there is never a put receipt yet.
	if added.allocOK.Address == nil {
		return StreamingAdd{}, fmt.Errorf("allocation by digest code returned no upload address")
	}
	if added.putRcpt != nil {
		return StreamingAdd{}, fmt.Errorf("allocation by digest code came with a put receipt")
	}
	return StreamingAdd{
		Size:          size,
		AddTask:       added.inv.Task().Link(),
		AcceptTask:    added.accInv.Task().Link(),
		PutInvocation: added.putInv.Bytes(),
		address:       *added.allocOK.Address,
	}, nil
}

// PutStream sends a streaming add's data to its allocation. body must yield
// exactly add.Size bytes: the node rejects a PUT of any other length.
func (c *Client) PutStream(ctx context.Context, add StreamingAdd, body io.Reader, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	if err := putBlob(ctx, cfg.PutClient, add.address.URL.URL(), add.address.Headers, body, int64(add.Size)); err != nil {
		return fmt.Errorf("putting blob: %w", err)
	}
	return nil
}

// IsUnsupportedDigestCode reports whether err is an add refused because the
// upload service or its storage nodes cannot add a blob by digest code.
func IsUnsupportedDigestCode(err error) bool {
	var named ucanerrors.Named
	return ucanerrors.As(err, &named) && named.Name() == blobcmds.UnsupportedDigestCodeErrorName
}
