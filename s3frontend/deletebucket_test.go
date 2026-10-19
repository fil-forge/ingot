package s3frontend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// releasableLog stands in for the production catalog log as DeleteBucket sees
// it: it lists one shipped segment digest, and once that digest is released
// every catalog block it served is gone, as it is when the network copy is
// removed and the quiesced store no longer serves local reads.
type releasableLog struct {
	blockstore.Log
	segment  multihash.Multihash
	released atomic.Bool
}

func (l *releasableLog) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	if l.released.Load() {
		return nil, blockstore.ErrNotFound
	}
	return l.Log.Get(ctx, c)
}

func (l *releasableLog) QuiesceBucketLog(context.Context, string) error { return nil }

func (l *releasableLog) ShippedSegmentDigests(context.Context, string) ([]multihash.Multihash, error) {
	return []multihash.Multihash{l.segment}, nil
}

// catalogRemover releases the log's segment when asked to, and records whether
// the bucket's root still named catalog blocks at that moment.
type catalogRemover struct {
	log *releasableLog
	reg registry.Registry

	mu                 sync.Mutex
	releasedUnderARoot bool
}

func (r *catalogRemover) RemoveBlob(ctx context.Context, _ did.DID, d multihash.Multihash) error {
	if string(d) != string(r.log.segment) {
		return nil
	}
	if st, err := r.reg.Get(ctx, "bk"); err == nil && st.Root.Defined() {
		r.mu.Lock()
		r.releasedUnderARoot = true
		r.mu.Unlock()
	}
	r.log.released.Store(true)
	return nil
}

// refusingAuthority refuses DeleteBucket the way hilt does while the space
// still lists a blob, until told to stop.
type refusingAuthority struct {
	bucketauthority.BucketAuthority
	refuse atomic.Bool
}

func (a *refusingAuthority) DeleteBucket(ctx context.Context, req cmds3.Request) error {
	if a.refuse.Load() {
		return bucketauthority.ErrNotEmpty
	}
	return a.BucketAuthority.DeleteBucket(ctx, req)
}

// TestDeleteBucket_RefusalLeavesBucketReadable: a DeleteBucket that hilt
// refuses after the bucket's catalog segments were released leaves the bucket
// listable, and a retry deletes it. The emptied bucket's root is committed
// empty before any segment is released, so nothing it reads goes with them.
func TestDeleteBucket_RefusalLeavesBucketReadable(t *testing.T) {
	var (
		log  *releasableLog
		auth *refusingAuthority
		rm   *catalogRemover
	)
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
		log = &releasableLog{Log: d.Log, segment: digestOf(t, []byte("catalog segment"))}
		d.Log = log
		d.Reads = blockstore.NewLayered(blockstore.LocalBlobs{Cache: d.Cache, Spool: d.Spool}, log, inmem.NopBaseReader{})
		auth = &refusingAuthority{BucketAuthority: d.Authority}
		d.Authority = auth
		rm = &catalogRemover{log: log, reg: d.Registry}
		d.Remover = rm
	})
	ctx := context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{})
	bucket := "bk"

	// The bucket held an object and is empty again: its root names an empty
	// tree whose nodes live in the catalog log.
	putObj(t, b, "obj", []byte("written then deleted"))
	deleteObj(t, b, "obj")
	if st, err := mem.Get(ctx, bucket); err != nil || !st.Root.Defined() {
		t.Fatalf("bucket root after the delete = %v (err %v), want a defined root", st, err)
	}

	auth.refuse.Store(true)
	wantAPIErr(t, b.DeleteBucket(ctx, bucket), s3err.ErrBucketNotEmpty)
	if !log.released.Load() {
		t.Fatalf("the refused delete never released the catalog segment; the test proves nothing")
	}
	if rm.releasedUnderARoot {
		t.Fatalf("the catalog segment was released while the bucket's root still named catalog blocks")
	}

	res, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListObjectsV2 after the refused delete: %v", err)
	}
	if len(res.Contents) != 0 {
		t.Fatalf("ListObjectsV2 after the refused delete = %d keys, want none", len(res.Contents))
	}

	auth.refuse.Store(false)
	if err := b.DeleteBucket(ctx, bucket); err != nil {
		t.Fatalf("retried DeleteBucket: %v", err)
	}
	if _, err := mem.Get(ctx, bucket); err == nil {
		t.Fatalf("bucket row survived the retried delete")
	}
}
