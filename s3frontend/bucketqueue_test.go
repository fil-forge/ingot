package s3frontend

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/taskrunner"
)

// commitTasks counts the queue-bucket.commit tasks run so far.
func commitTasks(t *testing.T, mem *testStore) int {
	t.Helper()
	tasks, err := mem.queued(context.Background(), bucketqueue.Name)
	if err != nil {
		t.Fatalf("queued: %v", err)
	}
	n := 0
	for _, task := range tasks {
		if task.Action == bucketqueue.CommitAction {
			n++
		}
	}
	return n
}

// TestDeleteObjectsIsOneCommit: every key of a DeleteObjects request goes
// into one commit, and a key that fails on its own terms (its ETag no longer
// matches) fails alone while the others are deleted.
func TestDeleteObjectsIsOneCommit(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		putObj(t, b, k, []byte("body of "+k))
	}
	before := commitTasks(t, mem)

	bucket := "bk"
	res, err := b.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("a")},
			{Key: aws.String("b"), ETag: aws.String(`"not-its-etag"`)},
			{Key: aws.String("c")},
		}},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if n := commitTasks(t, mem) - before; n != 1 {
		t.Fatalf("DeleteObjects ran %d commits, want 1", n)
	}
	if len(res.Deleted) != 2 || len(res.Error) != 1 || *res.Error[0].Key != "b" || *res.Error[0].Code != "PreconditionFailed" {
		t.Fatalf("result = deleted %v, errors %v; want a and c deleted and b PreconditionFailed", res.Deleted, res.Error)
	}
	for k, want := range map[string]bool{"a": false, "b": true, "c": false} {
		_, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: aws.String(k)})
		if exists := err == nil; exists != want {
			t.Fatalf("%s exists = %v after the delete, want %v (err %v)", k, exists, want, err)
		}
	}
}

// TestDeleteBucketWithoutItsRequestDoesNothing: a queue-bucket.delete
// resumed without the caller that ran it has no signed request to send
// Hilt, so it fails for good before it touches the bucket; the client, whose
// request failed, retries with a fresh one.
func TestDeleteBucketWithoutItsRequestDoesNothing(t *testing.T) {
	_, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	err := runBucketTaskRaw(ctx, mem, mem.bk(), bucketqueue.DeleteAction, &bucketqueue.BucketTask{Name: "bk"})
	if !taskrunner.IsPermanent(err) {
		t.Fatalf("delete without its request = %v, want a permanent failure", err)
	}
	if _, err := mem.Get(ctx, "bk"); err != nil {
		t.Fatalf("the bucket is gone after a delete that did nothing: %v", err)
	}
}
