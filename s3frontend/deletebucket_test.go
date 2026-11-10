package s3frontend

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// releasableLog stands in for the production catalog log as DeleteBucket sees
// it: it lists one shipped segment digest, and once that digest is released
// every catalog block it served is gone, as it is when the network copy is
// removed and the quiesced store no longer serves local reads. A retired
// segment's digest, if set, goes through ReleaseRetiredSegments.
type releasableLog struct {
	blockstore.Log
	segment  multihash.Multihash
	retired  multihash.Multihash
	released atomic.Bool
}

func (l *releasableLog) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	if l.released.Load() {
		return nil, blockstore.ErrNotFound
	}
	return l.Log.Get(ctx, c)
}

func (l *releasableLog) QuiesceBucketLog(context.Context, string) error { return nil }

func (l *releasableLog) ReleaseRetiredSegments(_ context.Context, _ string, release func(multihash.Multihash) error) error {
	if l.retired == nil {
		return nil
	}
	return release(l.retired)
}

func (l *releasableLog) ShippedSegmentDigests(context.Context, string) ([]multihash.Multihash, error) {
	return []multihash.Multihash{l.segment}, nil
}

// catalogRemover releases the log's segments when asked to, and records which
// it released and whether the bucket's root still named catalog blocks at that
// moment.
type catalogRemover struct {
	log *releasableLog
	reg registry.Registry

	mu                 sync.Mutex
	releasedUnderARoot bool
	releasedRetired    bool
}

func (r *catalogRemover) RemoveBlob(ctx context.Context, _ did.DID, d multihash.Multihash) error {
	shipped, retired := string(d) == string(r.log.segment), string(d) == string(r.log.retired)
	if !shipped && !retired {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, err := r.reg.Get(ctx, "bk"); err == nil && st.Root.Defined() {
		r.releasedUnderARoot = true
	}
	if retired {
		r.releasedRetired = true
	}
	if shipped {
		r.log.released.Store(true)
	}
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
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		log = &releasableLog{
			Log:     d.Log,
			segment: digestOf(t, []byte("catalog segment")),
			retired: digestOf(t, []byte("retired catalog segment")),
		}
		d.Log = log
		d.Reads = blockstore.NewLayered(d.Cache, log, testfakes.NopBaseReader{})
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
	rm.mu.Lock()
	releasedRetired, releasedUnderARoot := rm.releasedRetired, rm.releasedUnderARoot
	rm.mu.Unlock()
	if !releasedRetired {
		t.Fatalf("the refused delete never released the retired catalog segment")
	}
	if releasedUnderARoot {
		t.Fatalf("a catalog segment was released while the bucket's root still named catalog blocks")
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

// TestDeleteBucket_RefusalDropsTheJournal: the write that empties the root
// also drops the bucket's journaled catalog blocks. A refused delete leaves
// the bucket's last segment unshipped until its next write, and the journal
// must not wait with it: nothing the emptied bucket reads needs those blocks.
func TestDeleteBucket_RefusalDropsTheJournal(t *testing.T) {
	b, mem, _, _ := newJournalingBackend(t)
	auth := &refusingAuthority{BucketAuthority: mem}
	auth.refuse.Store(true)
	b.authority = auth
	ctx := context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{})

	// The ship waits on the test, so the PUT's and DELETE's blocks stay
	// journaled.
	putObj(t, b, "obj", []byte("written then deleted"))
	deleteObj(t, b, "obj")
	if st, err := mem.CatalogJournalStats(ctx); err != nil || st.Blocks == 0 {
		t.Fatalf("journal before the delete = %+v (err %v), want rows; the test proves nothing", st, err)
	}

	wantAPIErr(t, b.DeleteBucket(ctx, "bk"), s3err.ErrBucketNotEmpty)
	if st, err := mem.CatalogJournalStats(ctx); err != nil || st.Blocks != 0 {
		t.Fatalf("journal after the refused delete = %+v (err %v), want empty", st, err)
	}
}

// An abandoned delete, left queued by a DeleteBucket whose caller went away,
// never acts on another request's authority: run with a later PUT's context,
// which carries that PUT's signed request, as a drain of the bucket's tasks
// would, it finds no mark of its own DeleteBucket and fails without touching
// the bucket.
func TestAnAbandonedDeleteNeverActs(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	args, err := encodeBucketTask(&BucketTask{Name: "bk"})
	require.NoError(t, err)

	put := context.WithValue(ctx, reqscope.RequestKey(), cmds3.Request{})
	_, err = b.runDeleteBucket(put, nil, taskrunner.Task{Queue: registry.BucketQueue, Action: registry.BucketDeleteAction, Args: args})
	if err == nil || !taskrunner.IsPermanent(err) {
		t.Fatalf("delete run with another request's context: %v, want a permanent refusal", err)
	}
	if _, err := mem.Get(ctx, "bk"); err != nil {
		t.Fatalf("the bucket after the refused delete: %v", err)
	}
}

// answeringAuthority answers DeleteBucket with err.
type answeringAuthority struct {
	bucketauthority.BucketAuthority
	err error
}

func (a *answeringAuthority) DeleteBucket(context.Context, cmds3.Request) error { return a.err }

// TestDeleteBucket_OnlyARefusalReopens: Hilt not finding the space means an
// earlier delete removed it, so the delete finishes; a refusal reopens the
// bucket; a call that failed on its way may have deleted the space, so the
// bucket stays closing.
func TestDeleteBucket_OnlyARefusalReopens(t *testing.T) {
	for name, tc := range map[string]struct {
		err     error
		gone    bool
		closing bool
	}{
		"not found":   {err: bucketauthority.ErrNotFound, gone: true},
		"refused":     {err: fmt.Errorf("%w: access denied", bucketauthority.ErrRefused)},
		"not empty":   {err: bucketauthority.ErrNotEmpty},
		"unreachable": {err: errors.New("connection reset"), closing: true},
	} {
		t.Run(name, func(t *testing.T) {
			b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
				d.Authority = &answeringAuthority{BucketAuthority: d.Authority, err: tc.err}
			})
			ctx := context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{})
			err := b.DeleteBucket(ctx, "bk")
			if tc.gone != (err == nil) {
				t.Fatalf("DeleteBucket: %v", err)
			}
			st, gerr := mem.Get(ctx, "bk")
			if tc.gone {
				if !errors.Is(gerr, registry.ErrNotFound) {
					t.Fatalf("the bucket after the delete: %v, want gone", gerr)
				}
				return
			}
			if gerr != nil {
				t.Fatal(gerr)
			}
			if st.Closing != tc.closing {
				t.Fatalf("closing = %v, want %v", st.Closing, tc.closing)
			}
		})
	}
}

// nameHeldLog is a catalog log that records, for each bucket log it removes,
// whether the bucket's row still held the name.
type nameHeldLog struct {
	blockstore.Log
	reg registry.Registry

	mu   sync.Mutex
	held map[string]bool
}

func (l *nameHeldLog) RemoveBucketLog(ctx context.Context, bucket string) error {
	_, err := l.reg.Get(ctx, bucket)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held[bucket] = err == nil
	return nil
}

// TestDeleteBucketRemovesItsLogWhileHoldingTheName: the log is the name's,
// and a bucket created under the name once the row goes runs apart from the
// delete, so the delete removes the log before it gives the name up.
func TestDeleteBucketRemovesItsLogWhileHoldingTheName(t *testing.T) {
	log := &nameHeldLog{held: map[string]bool{}}
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		log.Log, log.reg = d.Log, d.Registry
		d.Log = log
	})
	ctx := context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{})
	if err := b.DeleteBucket(ctx, "bk"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := mem.Get(ctx, "bk"); err == nil {
		t.Fatal("the bucket row survived the delete")
	}
	log.mu.Lock()
	held, removed := log.held["bk"]
	log.mu.Unlock()
	if !removed {
		t.Fatal("the delete never removed the bucket's log")
	}
	if !held {
		t.Fatal("the delete removed the bucket's log after giving its name up")
	}
}
