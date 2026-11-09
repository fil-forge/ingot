package s3frontend

import (
	"context"
	"errors"
	"testing"

	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3err"

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
}

func wantNoSuchBucket(t *testing.T, what string, err error) {
	t.Helper()
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchBucket" {
		t.Fatalf("%s for another space: %v, want NoSuchBucket", what, err)
	}
}
