package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// pendingUploadTasks returns the pending queue-multipart-upload tasks of
// uploadID with action, by their due times.
func pendingUploadTasks(t *testing.T, mem *testStore, uploadID, action string) []time.Time {
	t.Helper()
	tasks, err := mem.queued(context.Background(), multipartqueue.Name)
	if err != nil {
		t.Fatalf("queued: %v", err)
	}
	var due []time.Time
	for _, task := range tasks {
		if task.Subject == uploadID && task.Action == action && task.State == "pending" && task.NotBefore != nil {
			due = append(due, *task.NotBefore)
		}
	}
	return due
}

func wantAPIError(t *testing.T, what string, err error, code string) {
	t.Helper()
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		t.Fatalf("%s: err = %v, want %s", what, err, code)
	}
}

// TestCreateSchedulesTheAutoAbortAndCompleteTheReap: an upload's auto-abort
// is queued with its creation, a session TTL out, and its reap with its
// Complete.
func TestCreateSchedulesTheAutoAbortAndCompleteTheReap(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	key := "scheduled"
	before := time.Now()
	uploadID, parts := completeTwoParts(t, b, key)
	due := pendingUploadTasks(t, mem, uploadID, multipartqueue.AbortAction)
	if len(due) != 1 || due[0].Before(before.Add(multipartqueue.DefaultSessionTTL-time.Minute)) {
		t.Fatalf("auto-aborts pending = %v, want one a session TTL out", due)
	}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if due := pendingUploadTasks(t, mem, uploadID, multipartqueue.ReapAction); len(due) != 1 || due[0].Before(before.Add(multipartqueue.DefaultSessionTTL-time.Minute)) {
		t.Fatalf("reaps pending = %v, want one a session TTL out", due)
	}
}

// TestAbortAndReapLeaveTheOtherState: the auto-abort leaves a completed
// upload alone and a client's abort of one is NoSuchUpload; the reap leaves
// an open upload alone.
func TestAbortAndReapLeaveTheOtherState(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	key := "states"
	uploadID, parts := completeTwoParts(t, b, key)
	if _, err := storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.ReapAction, nil)); err != nil {
		t.Fatalf("reap of an open upload: %v", err)
	}
	if s, err := mem.GetSession(ctx, uploadID); err != nil || s.State != registry.SessionOpen {
		t.Fatalf("session after the reap of an open upload = %+v, %v; want open", s, err)
	}
	if _, err := mpComplete(t, b, key, uploadID, parts, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var auto bytes.Buffer
	if err := (&multipartqueue.AbortTask{Auto: true}).MarshalCBOR(&auto); err != nil {
		t.Fatal(err)
	}
	if _, err := storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.AbortAction, auto.Bytes())); err != nil {
		t.Fatalf("auto-abort of a completed upload: %v", err)
	}
	if s, err := mem.GetSession(ctx, uploadID); err != nil || s.State != registry.SessionCompleted {
		t.Fatalf("session after the auto-abort = %+v, %v; want completed", s, err)
	}
	sess, err := mem.GetSession(ctx, uploadID)
	if err != nil {
		t.Fatal(err)
	}
	wantAPIError(t, "abort of a completed upload", b.abortUpload(ctx, *sess), "NoSuchUpload")
	if _, err := storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.ReapAction, nil)); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("session after its reap: %v, want ErrNotFound", err)
	}
}

// TestClosedBucketRefusesWritesAndUploads: once a DeleteBucket has closed a
// bucket, its commits and new multipart uploads are refused, so it stays
// empty and gains no upload while the delete aborts the ones in flight.
func TestClosedBucketRefusesWritesAndUploads(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	if err := runBucketTask(ctx, mem, st.Space, bucketqueue.CloseAction, &bucketqueue.BucketTask{Name: "bk"}); err != nil {
		t.Fatalf("close: %v", err)
	}
	bucket, key := "bk", "late"
	_, err = b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x"))})
	wantAPIError(t, "PutObject to a closed bucket", err, "OperationAborted")
	_, err = b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	wantAPIError(t, "CreateMultipartUpload in a closed bucket", err, "OperationAborted")
}

// TestCloseRefusesANonEmptyBucket: a bucket that holds an object is not
// closed, and keeps taking writes.
func TestCloseRefusesANonEmptyBucket(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	putObj(t, b, "kept", []byte("body"))
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	wantAPIError(t, "close of a non-empty bucket", runBucketTask(ctx, mem, st.Space, bucketqueue.CloseAction, &bucketqueue.BucketTask{Name: "bk"}), "BucketNotEmpty")
	putObj(t, b, "after", []byte("body"))
}

// TestDeleteBucketAbortsUploadsInFlight: a bucket with a multipart upload in
// flight is deleted, as on AWS: the upload is aborted and its part's blob
// released, then the bucket goes.
func TestDeleteBucketAbortsUploadsInFlight(t *testing.T) {
	pu := &parkingUploader{}
	b, mem := newDeferredBackend(t, pu)
	key := "in-flight"
	uploadID := mpCreate(t, b, key, "", "")
	uploadDistinctParts(t, b, key, uploadID, 1)
	d := hygienePartDigests(t, mem, uploadID, 1)[0]

	ctx := context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{})
	if err := b.DeleteBucket(ctx, "bk"); err != nil {
		t.Fatalf("DeleteBucket with an upload in flight: %v", err)
	}
	if _, err := mem.GetSession(ctx, uploadID); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the upload survived its bucket's delete: %v", err)
	}
	if !pu.abortedDigests()[string(d)] {
		t.Fatalf("the upload's blob %x was not aborted", d)
	}
	if _, err := mem.Get(ctx, "bk"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the bucket survived its delete: %v", err)
	}
	bucket := "bk"
	_, err := b.ListParts(ctx, &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err == nil {
		t.Fatal("ListParts of the aborted upload succeeded")
	}
}

// TestAnAbandonedAddPartTakesNothing: an add-part left queued by an
// UploadPart whose caller went away runs on a worker, with no request's part
// to record, and fails without acting. The next UploadPart, queued behind it,
// records its own part with its own references.
func TestAnAbandonedAddPartTakesNothing(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	key := "abandoned-add-part"
	uploadID := mpCreate(t, b, key, "", "")
	abandoned, err := storeFor(b).tasks.Submit(ctx, uploadSpec(uploadID, multipartqueue.AddPartAction, nil))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart behind an abandoned add-part: %v", err)
	}
	var failed *taskrunner.HandlerError
	if _, err := storeFor(b).tasks.Wait(ctx, multipartqueue.Name, abandoned); !errors.As(err, &failed) {
		t.Fatalf("the abandoned add-part: %v, want it failed", err)
	}
	if parts, err := mem.ListParts(ctx, uploadID); err != nil || len(parts) != 1 {
		t.Fatalf("parts after the UploadPart = %d, %v; want its own", len(parts), err)
	}
}

// uploadSpec is a queue-multipart-upload task on uploadID.
func uploadSpec(uploadID, action string, args []byte) taskrunner.Spec {
	return taskrunner.Spec{Queue: multipartqueue.Name, Subject: uploadID, Action: action, Args: args}
}
