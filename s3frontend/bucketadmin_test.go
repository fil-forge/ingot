package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/queue/bucketqueue"
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

	err := runBucketTask(ctx, mem, old, bucketqueue.SetVersioningAction, &bucketqueue.BucketSetVersioningTask{Name: "bk", State: string(registry.VersioningEnabled)})
	wantNoSuchBucket(t, "set-versioning", err)
	if st, _ := mem.Get(ctx, "bk"); st.Versioning == registry.VersioningEnabled {
		t.Fatal("a task for another space set the bucket's versioning")
	}

	for _, action := range []string{bucketqueue.CloseAction, bucketqueue.AdmitUploadAction} {
		wantNoSuchBucket(t, action, runBucketTask(ctx, mem, old, action, &bucketqueue.BucketTask{Name: "bk"}))
	}
	if st, _ := mem.Get(ctx, "bk"); st.Closing {
		t.Fatal("a close for another space closed the bucket")
	}

	del := context.WithValue(ctx, reqscope.RequestKey(), cmds3.Request{})
	wantNoSuchBucket(t, "delete", mapBucketError(b.buckets.Delete(del, old, "bk"), "delete"))
	if _, err := mem.Get(ctx, "bk"); err != nil {
		t.Fatalf("the bucket after a delete for another space: %v", err)
	}

	wantNoSuchBucket(t, "commit", runBucketTask(ctx, mem, old, bucketqueue.CommitAction, &bucketqueue.BucketCommitTask{Bucket: "bk"}))
}

// A ship queued for an earlier bucket of the name, or for a bucket since
// deleted, ships nothing.
func TestAShipForAnotherBucketShipsNothing(t *testing.T) {
	log := &shipFails{}
	_, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		log.Log = d.Log
		d.Log = log
	})
	ctx := context.Background()
	if err := runBucketTask(ctx, mem, testutil.RandomDID(t), bucketqueue.ShipAction, &bucketqueue.BucketTask{Name: "bk"}); err != nil {
		t.Fatalf("ship for another space: %v", err)
	}
	if err := runBucketTask(ctx, mem, mem.bk(), bucketqueue.ShipAction, &bucketqueue.BucketTask{Name: "gone"}); err != nil {
		t.Fatalf("ship of a deleted bucket: %v", err)
	}
	if n := log.ships.Load(); n != 0 {
		t.Fatalf("shipped %d times, want none", n)
	}
}

// A create run again after it recorded the bucket succeeds; one for a name
// another space's bucket holds is BucketAlreadyExists.
func TestACreateRunAgainSucceeds(t *testing.T) {
	_, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	create := &bucketqueue.BucketCreateTask{Name: "bk", Tenant: st.Tenant.String()}
	if err := runBucketTask(ctx, mem, st.Space, bucketqueue.CreateAction, create); err != nil {
		t.Fatalf("create run again: %v", err)
	}
	err = runBucketTask(ctx, mem, testutil.RandomDID(t), bucketqueue.CreateAction, create)
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

// The registry deletes a bucket only in the space given, so a delete for a
// bucket's old space leaves the bucket recreated under its name.
func TestARegistryDeleteForAnotherSpaceLeavesTheBucket(t *testing.T) {
	_, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	if err := mem.Delete(ctx, "bk", testutil.RandomDID(t)); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("delete for another space: %v, want ErrNotFound", err)
	}
	if _, err := mem.Get(ctx, "bk"); err != nil {
		t.Fatalf("the bucket after a delete for another space: %v", err)
	}
}

// logRemovals is a catalog log that records the bucket logs it removes.
type logRemovals struct {
	blockstore.Log
	removed []string
}

func (l *logRemovals) RemoveBucketLog(_ context.Context, bucket string) error {
	l.removed = append(l.removed, bucket)
	return nil
}

// A create recording a new bucket first removes any log a deleted bucket of
// the name left behind; one run again after recording it leaves the log alone.
func TestACreateRemovesALeftoverLogOnce(t *testing.T) {
	log := &logRemovals{}
	_, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		log.Log = d.Log
		d.Log = log
	})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	space := testutil.RandomDID(t)
	create := &bucketqueue.BucketCreateTask{Name: "again", Tenant: st.Tenant.String()}
	for range 2 {
		if err := runBucketTask(ctx, mem, space, bucketqueue.CreateAction, create); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	if !slices.Equal(log.removed, []string{"again"}) {
		t.Fatalf("removed logs = %v, want the name's once", log.removed)
	}
}

// TestBucketSettingsChangeOnlyTheirOwnBucket: a tagging, CORS or encryption
// change queued for a bucket deleted and recreated under its name since, in
// another space, is NoSuchBucket, and leaves the new bucket's settings as
// they were.
func TestBucketSettingsChangeOnlyTheirOwnBucket(t *testing.T) {
	_, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Delete(ctx, "bk", st.Space); err != nil {
		t.Fatal(err)
	}
	if err := mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: st.Tenant}); err != nil {
		t.Fatal(err)
	}
	for action, value := range map[string]string{
		bucketqueue.SetTaggingAction:    `{"k":"v"}`,
		bucketqueue.SetCORSAction:       "<CORSConfiguration></CORSConfiguration>",
		bucketqueue.SetEncryptionAction: string(registry.BucketEncryptionNone),
	} {
		err := runBucketTask(ctx, mem, st.Space, action, &bucketqueue.BucketSetConfigTask{Name: "bk", Value: []byte(value)})
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	now, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	if now.BucketTagging != nil || now.CORSConfig != nil || now.Encryption != registry.BucketEncryptionUnset {
		t.Fatalf("the new bucket's settings changed: tagging %q, cors %q, encryption %q", now.BucketTagging, now.CORSConfig, now.Encryption)
	}
}

// runBucketTask runs a queue-bucket action on space's bucket through mem's
// runner, answering as a request would.
func runBucketTask(ctx context.Context, mem *testStore, space did.DID, action string, args interface{ MarshalCBOR(io.Writer) error }) error {
	return mapBucketError(runBucketTaskRaw(ctx, mem, space, action, args), action)
}

// runBucketTaskRaw is runBucketTask answering with the task's own error.
func runBucketTaskRaw(ctx context.Context, mem *testStore, space did.DID, action string, args interface{ MarshalCBOR(io.Writer) error }) error {
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return err
	}
	_, err := mem.tasks.Run(ctx, taskrunner.Spec{Queue: bucketqueue.Name, Subject: bucketqueue.Subject(space), Action: action, Args: buf.Bytes()})
	return err
}
