package s3frontend

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/taskrunner"
)

// pendingUploadTasks returns the pending queue-multipart-upload tasks of
// uploadID with action, by their due times.
func pendingUploadTasks(t *testing.T, mem *testStore, uploadID, action string) []time.Time {
	t.Helper()
	tasks, err := mem.queued(context.Background(), multipartqueue.Name)
	require.NoError(t, err, "queued")
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
	require.ErrorAs(t, err, &apiErr, "%s: err", what)
	require.Equal(t, code, apiErr.Code, "%s: err", what)
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
	require.Len(t, due, 1, "auto-aborts pending = %v, want one a session TTL out", due)
	require.False(t, due[0].Before(before.Add(multipartqueue.DefaultSessionTTL-time.Minute)), "auto-aborts pending = %v, want one a session TTL out", due)
	_, err := mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	reaps := pendingUploadTasks(t, mem, uploadID, multipartqueue.ReapAction)
	require.Len(t, reaps, 1, "reaps pending")
	require.False(t, reaps[0].Before(before.Add(multipartqueue.DefaultSessionTTL-time.Minute)), "reaps pending = %v, want one a session TTL out", reaps)
}

// TestAbortAndReapLeaveTheOtherState: the auto-abort leaves a completed
// upload alone and a client's abort of one is NoSuchUpload; the reap leaves
// an open upload alone.
func TestAbortAndReapLeaveTheOtherState(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	key := "states"
	uploadID, parts := completeTwoParts(t, b, key)
	_, err := storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.ReapAction, nil))
	require.NoError(t, err, "reap of an open upload")
	s, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session after the reap of an open upload")
	require.Equal(t, registry.SessionOpen, s.State, "session after the reap of an open upload")
	_, err = mpComplete(t, b, key, uploadID, parts, nil)
	require.NoError(t, err, "Complete")
	var auto bytes.Buffer
	require.NoError(t, (&multipartqueue.AbortTask{Auto: true}).MarshalCBOR(&auto))
	_, err = storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.AbortAction, auto.Bytes()))
	require.NoError(t, err, "auto-abort of a completed upload")
	s, err = mem.GetSession(ctx, uploadID)
	require.NoError(t, err, "session after the auto-abort")
	require.Equal(t, registry.SessionCompleted, s.State, "session after the auto-abort")
	sess, err := mem.GetSession(ctx, uploadID)
	require.NoError(t, err)
	wantAPIError(t, "abort of a completed upload", b.abortUpload(ctx, *sess), "NoSuchUpload")
	_, err = storeFor(b).tasks.Run(ctx, uploadSpec(uploadID, multipartqueue.ReapAction, nil))
	require.NoError(t, err, "reap")
	_, err = mem.GetSession(ctx, uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "session after its reap")
}

// TestClosedBucketRefusesWritesAndUploads: once a DeleteBucket has closed a
// bucket, its commits and new multipart uploads are refused, so it stays
// empty and gains no upload while the delete aborts the ones in flight.
func TestClosedBucketRefusesWritesAndUploads(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	require.NoError(t, runBucketTask(ctx, mem, st.Space, bucketqueue.CloseAction, &bucketqueue.BucketTask{Name: "bk"}), "close")
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
	require.NoError(t, err)
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
	require.NoError(t, b.DeleteBucket(ctx, "bk"), "DeleteBucket with an upload in flight")
	_, err := mem.GetSession(ctx, uploadID)
	require.ErrorIs(t, err, registry.ErrNotFound, "the upload survived its bucket's delete")
	require.True(t, pu.abortedDigests()[string(d)], "the upload's blob %x was not aborted", d)
	_, err = mem.Get(ctx, "bk")
	require.ErrorIs(t, err, registry.ErrNotFound, "the bucket survived its delete")
	bucket := "bk"
	_, err = b.ListParts(ctx, &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	require.Error(t, err, "ListParts of the aborted upload succeeded")
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
	require.NoError(t, err)

	_, err = mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart behind an abandoned add-part")
	var failed *taskrunner.HandlerError
	_, err = storeFor(b).tasks.Wait(ctx, multipartqueue.Name, abandoned)
	require.ErrorAs(t, err, &failed, "the abandoned add-part, want it failed")
	parts, err := mem.ListParts(ctx, uploadID)
	require.NoError(t, err, "parts after the UploadPart")
	require.Len(t, parts, 1, "parts after the UploadPart")
}

// uploadSpec is a queue-multipart-upload task on uploadID.
func uploadSpec(uploadID, action string, args []byte) taskrunner.Spec {
	return taskrunner.Spec{Queue: multipartqueue.Name, Subject: uploadID, Action: action, Args: args}
}
