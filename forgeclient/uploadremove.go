// Carried from github.com/fil-forge/guppy/pkg/client/uploadremove.go.
package forgeclient

import (
	"context"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
)

// UploadRemove invokes /upload/remove against the upload service (sprue),
// dropping root's content entry from the space. The space is the invocation
// subject. It does NOT remove the blobs the root covers — that is a separate
// per-digest /blob/remove decision the reference index owns, since a blob can
// be claimed by more than one version. Idempotent: removing an unknown root
// succeeds.
func (c *Client) UploadRemove(ctx context.Context, space did.DID, root cid.Cid, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}
	results, err := c.UploadRemoveBatch(ctx, []UploadChange{{Space: space, Root: root, Proofs: proofStore}})
	if err != nil {
		return err
	}
	return results[0]
}
