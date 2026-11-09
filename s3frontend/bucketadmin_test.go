package s3frontend

import (
	"context"
	"errors"
	"testing"

	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// A bucket task queued for a bucket's old space, run once the name belongs to
// a bucket in another space, answers NoSuchBucket and leaves the bucket that
// has the name now alone.
func TestABucketTaskForAnotherSpaceIsNoSuchBucket(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	old := testutil.RandomDID(t)

	err := b.runBucket(ctx, old, registry.BucketSetVersioningAction, &BucketSetVersioningTask{Name: "bk", State: string(registry.VersioningEnabled)})
	wantNoSuchBucket(t, "set-versioning", err)
	if st, _ := mem.Get(ctx, "bk"); st.Versioning == registry.VersioningEnabled {
		t.Fatal("a task for another space set the bucket's versioning")
	}

	args, err := encodeBucketTask(&BucketTask{Name: "bk"})
	if err != nil {
		t.Fatal(err)
	}
	del := context.WithValue(context.WithValue(ctx, reqscope.RequestKey(), cmds3.Request{}), deleteKey{}, "bk")
	_, err = b.runDeleteBucket(del, nil, taskrunner.Task{Subject: old.String(), Args: args})
	wantNoSuchBucket(t, "delete", err)
	if _, err := mem.Get(ctx, "bk"); err != nil {
		t.Fatalf("the bucket after a delete for another space: %v", err)
	}

	commit, err := encodeBucketTask(&BucketCommitTask{Bucket: "bk"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.runCommit(ctx, nil, taskrunner.Task{Subject: old.String(), Args: commit})
	if !errors.Is(err, bucketop.ErrBucketNotFound) {
		t.Fatalf("commit for another space: %v, want the bucket not found", err)
	}
}

// A ship queued for an earlier bucket of the name, or for a bucket since
// deleted, ships nothing.
func TestAShipForAnotherBucketShipsNothing(t *testing.T) {
	log := &shipFails{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		log.Log = d.Log
		d.Log = log
	})
	ctx := context.Background()
	args, err := encodeBucketTask(&BucketTask{Name: "bk"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.runShip(ctx, nil, taskrunner.Task{Subject: testutil.RandomDID(t).String(), Args: args}); err != nil {
		t.Fatalf("ship for another space: %v", err)
	}
	gone, err := encodeBucketTask(&BucketTask{Name: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.runShip(ctx, nil, taskrunner.Task{Subject: mem.bk().String(), Args: gone}); err != nil {
		t.Fatalf("ship of a deleted bucket: %v", err)
	}
	if n := log.ships.Load(); n != 0 {
		t.Fatalf("shipped %d times, want none", n)
	}
}

// A create run again after it recorded the bucket succeeds; one for a name
// another space's bucket holds is BucketAlreadyExists.
func TestACreateRunAgainSucceeds(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	create := &BucketCreateTask{Name: "bk", Tenant: st.Tenant.String()}
	if err := b.runBucket(ctx, st.Space, registry.BucketCreateAction, create); err != nil {
		t.Fatalf("create run again: %v", err)
	}
	err = b.runBucket(ctx, testutil.RandomDID(t), registry.BucketCreateAction, create)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "BucketAlreadyExists" {
		t.Fatalf("create for another space: %v, want BucketAlreadyExists", err)
	}
}

func wantNoSuchBucket(t *testing.T, what string, err error) {
	t.Helper()
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchBucket" {
		t.Fatalf("%s for another space: %v, want NoSuchBucket", what, err)
	}
}
