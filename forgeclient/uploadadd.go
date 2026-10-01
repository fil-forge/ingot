// Carried from github.com/fil-forge/guppy/pkg/client/uploadadd.go.
package forgeclient

import (
	"context"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
)

// UploadAdd invokes /upload/add against the upload service (sprue), recording
// root as a content entry in the space. The space is the invocation subject.
// The entry is what sprue counts to report the space's object count; it does
// not make the content durable, which /blob/add already did.
//
// Upsert semantics: adding the same root again merges the shards and replaces
// the index, and sprue counts only the first add, so a retry is safe.
//
// The shards argument is deliberately empty from ingot. The capability
// describes shards as the CAR archives containing the content DAG, and ingot's
// object bodies are raw sha256-addressed blobs rather than CARs — the blobs a
// root covers are already registered individually by /blob/add, and repeating
// them here would write a shard row per blob per object version for no reader.
func (c *Client) UploadAdd(ctx context.Context, space did.DID, root cid.Cid, options ...BlobAddOption) error {
	cfg := NewBlobAddConfig(options...)
	proofStore := ucanlib.ProofStore(c.tokenStore)
	if cfg.ProofStore != nil {
		proofStore = cfg.ProofStore
	}
	results, err := c.UploadAddBatch(ctx, []UploadChange{{Space: space, Root: root, Proofs: proofStore}})
	if err != nil {
		return err
	}
	return results[0]
}
