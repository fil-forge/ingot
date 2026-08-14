package s3frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// Object-lock tests over the same in-process harness as refindex_test.go,
// covering the docs/s3-object-lock.md §6 check order, the version-state
// tree (upgrade, merge, carry, cleanup), creation-time stamping (§7), the
// Head/Get echo (§8), and the §5 bucket guards.

// lockBucket enables versioning and an object-lock configuration on the
// harness bucket, returning the stored config document.
func lockBucket(t *testing.T, b *Backend) []byte {
	t.Helper()
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	now := time.Now()
	cfg, err := json.Marshal(auth.BucketLockConfig{Enabled: true, CreatedAt: &now})
	if err != nil {
		t.Fatalf("marshal lock config: %v", err)
	}
	if err := b.PutObjectLockConfiguration(context.Background(), "bk", cfg); err != nil {
		t.Fatalf("PutObjectLockConfiguration: %v", err)
	}
	return cfg
}

// wantAPIErr asserts err is exactly the given s3err code's APIError.
func wantAPIErr(t *testing.T, err error, code s3err.ErrorCode) {
	t.Helper()
	want := s3err.GetAPIError(code)
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want APIError %s", err, want.Code)
	}
	if apiErr != want {
		t.Fatalf("err = %+v, want %+v", apiErr, want)
	}
}

func retentionDoc(t *testing.T, mode types.ObjectLockRetentionMode, until time.Time) []byte {
	t.Helper()
	doc, err := json.Marshal(types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until})
	if err != nil {
		t.Fatalf("marshal retention: %v", err)
	}
	return doc
}

func TestObjectLock_BucketConfiguration(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// Never configured: the §2 not-found sentinel; a missing bucket outranks it.
	if _, err := b.GetObjectLockConfiguration(ctx, "bk"); err == nil {
		t.Fatal("unconfigured GetObjectLockConfiguration: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrObjectLockConfigurationNotFound)
	}
	if _, err := b.GetObjectLockConfiguration(ctx, "nope"); err == nil {
		t.Fatal("missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}

	// Lock requires versioning Enabled (unversioned here): 409.
	cfg := []byte(`{"Enabled":true}`)
	if err := b.PutObjectLockConfiguration(ctx, "bk", cfg); err == nil {
		t.Fatal("PutObjectLockConfiguration on unversioned bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrObjectLockConfigurationNotAllowed)
	}

	// Enabling later on a versioned bucket is allowed, and the document is
	// returned verbatim.
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	if err := b.PutObjectLockConfiguration(ctx, "bk", cfg); err != nil {
		t.Fatalf("PutObjectLockConfiguration: %v", err)
	}
	got, err := b.GetObjectLockConfiguration(ctx, "bk")
	if err != nil {
		t.Fatalf("GetObjectLockConfiguration: %v", err)
	}
	if !bytes.Equal(got, cfg) {
		t.Fatalf("config round-trip = %s, want %s", got, cfg)
	}

	// The §5 guard: a lock bucket can never be suspended.
	if err := b.PutBucketVersioning(ctx, "bk", types.BucketVersioningStatusSuspended); err == nil {
		t.Fatal("suspend of lock bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrSuspendedVersioningNotAllowed)
	}
}

func TestObjectLock_CheckOrderSentinels(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// Bucket without lock enabled: the missing-configuration sentinel for an
	// EXISTING key, but key existence outranks it — a missing key answers
	// NoSuchKey even without lock (GetObjectRetention_non_existing_object).
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k", []byte("v1"))
	if _, err := b.GetObjectRetention(ctx, "bk", "absent", ""); err == nil {
		t.Fatal("missing key on non-lock bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}
	if err := b.PutObjectRetention(ctx, "bk", "absent", "", []byte(`{}`)); err == nil {
		t.Fatal("put on missing key of non-lock bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}
	if _, err := b.GetObjectRetention(ctx, "bk", "k", ""); err == nil {
		t.Fatal("disabled lock: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMissingObjectLockConfiguration)
	}
	if err := b.PutObjectLegalHold(ctx, "bk", "k", "", true); err == nil {
		t.Fatal("disabled lock put: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMissingObjectLockConfiguration)
	}
	// A missing bucket outranks everything.
	if _, err := b.GetObjectLegalHold(ctx, "nope", "k", ""); err == nil {
		t.Fatal("missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}

	lockBucket(t, b)
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))

	// Missing key; unknown version; malformed version; unset state.
	if _, err := b.GetObjectRetention(ctx, "bk", "absent", ""); err == nil {
		t.Fatal("missing key: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}
	if err := b.PutObjectRetention(ctx, "bk", "absent", "", ret); err == nil {
		t.Fatal("put on missing key: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}
	if _, err := b.GetObjectRetention(ctx, "bk", "k", "01G65Z755AFWAKHE12NY0CQ9FH"); err == nil {
		t.Fatal("unknown version: want error")
	} else if code := apiErrCode(t, err); code != "NoSuchVersion" {
		t.Fatalf("unknown version code = %s, want NoSuchVersion", code)
	}
	if err := b.PutObjectRetention(ctx, "bk", "k", "not-a-version", ret); err == nil {
		t.Fatal("malformed version: want error")
	} else if code := apiErrCode(t, err); code != "InvalidArgument" {
		t.Fatalf("malformed version code = %s, want InvalidArgument", code)
	}
	if _, err := b.GetObjectRetention(ctx, "bk", "k", ""); err == nil {
		t.Fatal("unset retention: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	}
	if _, err := b.GetObjectLegalHold(ctx, "bk", "k", ""); err == nil {
		t.Fatal("unset hold: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	}

	// A delete marker rejects lock operations with 405.
	del, err := deleteObjV(t, b, "k", "")
	if err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	markerID := *del.VersionId
	if _, err := b.GetObjectRetention(ctx, "bk", "k", markerID); err == nil {
		t.Fatal("marker retention get: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
	if err := b.PutObjectRetention(ctx, "bk", "k", markerID, ret); err == nil {
		t.Fatal("marker retention put: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
	// Current is the marker: the unscoped read is 404-shaped, and the lock
	// path surfaces the marker sentinel for CheckObjectAccess.
	if _, err := b.GetObjectLegalHold(ctx, "bk", "k", ""); err == nil {
		t.Fatal("current-marker hold get: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
}

func TestObjectLock_RetentionAndHoldRoundTrip(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)

	// First lock write upgrades a single-version (manifest-arm) key to a
	// leaf; reads and lists keep working through the upgrade.
	out1 := putObjV(t, b, "k", []byte("v1"))
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour).UTC().Truncate(time.Second))
	if err := b.PutObjectRetention(ctx, "bk", "k", "", ret); err != nil {
		t.Fatalf("PutObjectRetention: %v", err)
	}
	got, err := b.GetObjectRetention(ctx, "bk", "k", "")
	if err != nil {
		t.Fatalf("GetObjectRetention: %v", err)
	}
	if !bytes.Equal(got, ret) {
		t.Fatalf("retention round-trip = %s, want %s", got, ret)
	}
	// The explicit version id resolves to the same state.
	if got, err = b.GetObjectRetention(ctx, "bk", "k", out1.VersionID); err != nil || !bytes.Equal(got, ret) {
		t.Fatalf("versioned GetObjectRetention = (%s, %v), want stored doc", got, err)
	}
	if _, data, err := getObjV(t, b, "k", ""); err != nil || string(data) != "v1" {
		t.Fatalf("read after leaf upgrade = (%q, %v), want v1", data, err)
	}

	// Tri-valued hold: never-set is 400 (asserted above); OFF and ON are
	// explicit states, and the merge carries retention across hold writes.
	if err := b.PutObjectLegalHold(ctx, "bk", "k", "", false); err != nil {
		t.Fatalf("PutObjectLegalHold(off): %v", err)
	}
	hold, err := b.GetObjectLegalHold(ctx, "bk", "k", "")
	if err != nil || hold == nil || *hold {
		t.Fatalf("hold after OFF = (%v, %v), want &false", hold, err)
	}
	if err := b.PutObjectLegalHold(ctx, "bk", "k", "", true); err != nil {
		t.Fatalf("PutObjectLegalHold(on): %v", err)
	}
	if hold, err = b.GetObjectLegalHold(ctx, "bk", "k", ""); err != nil || hold == nil || !*hold {
		t.Fatalf("hold after ON = (%v, %v), want &true", hold, err)
	}
	if got, err = b.GetObjectRetention(ctx, "bk", "k", ""); err != nil || !bytes.Equal(got, ret) {
		t.Fatalf("retention after hold writes = (%s, %v), want carried", got, err)
	}

	// Retention writes carry the hold in return, and replacement is
	// last-write-wins on the owned field.
	ret2 := retentionDoc(t, types.ObjectLockRetentionModeCompliance, time.Now().Add(2*time.Hour).UTC().Truncate(time.Second))
	if err := b.PutObjectRetention(ctx, "bk", "k", "", ret2); err != nil {
		t.Fatalf("replace retention: %v", err)
	}
	if got, err = b.GetObjectRetention(ctx, "bk", "k", ""); err != nil || !bytes.Equal(got, ret2) {
		t.Fatalf("replaced retention = (%s, %v), want new doc", got, err)
	}
	if hold, err = b.GetObjectLegalHold(ctx, "bk", "k", ""); err != nil || hold == nil || !*hold {
		t.Fatalf("hold after retention replace = (%v, %v), want carried &true", hold, err)
	}

	// State survives supersession: the overwrite stacks a new version and
	// the old version keeps its lock state; the new version has none.
	out2 := putObjV(t, b, "k", []byte("v2"))
	if got, err = b.GetObjectRetention(ctx, "bk", "k", out1.VersionID); err != nil || !bytes.Equal(got, ret2) {
		t.Fatalf("noncurrent retention = (%s, %v), want carried", got, err)
	}
	if _, err := b.GetObjectRetention(ctx, "bk", "k", out2.VersionID); err == nil {
		t.Fatal("new version inherited retention: want unset")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	}

	// Hold set directly on a noncurrent version.
	if err := b.PutObjectLegalHold(ctx, "bk", "k", out1.VersionID, false); err != nil {
		t.Fatalf("hold on noncurrent: %v", err)
	}
	if hold, err = b.GetObjectLegalHold(ctx, "bk", "k", out1.VersionID); err != nil || hold == nil || *hold {
		t.Fatalf("noncurrent hold = (%v, %v), want &false", hold, err)
	}
}

func TestObjectLock_CleanupOnVersionDelete(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)

	out1 := putObjV(t, b, "k", []byte("v1"))
	out2 := putObjV(t, b, "k", []byte("v2"))
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))
	if err := b.PutObjectRetention(ctx, "bk", "k", out1.VersionID, ret); err != nil {
		t.Fatalf("retention on noncurrent: %v", err)
	}

	// Deleting the noncurrent version removes its state entry; the emptied
	// tree drops off the leaf.
	if _, err := deleteObjV(t, b, "k", out1.VersionID); err != nil {
		t.Fatalf("delete noncurrent: %v", err)
	}
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	if err != nil {
		t.Fatalf("resolve after delete: %v", err)
	}
	if rv.leaf == nil || rv.leaf.State != nil {
		t.Fatalf("leaf after last state entry removed = %+v, want nil State", rv.leaf)
	}

	// Promotion prunes the removed current's entry and keeps the survivor's.
	out3 := putObjV(t, b, "k", []byte("v3"))
	if err := b.PutObjectRetention(ctx, "bk", "k", out2.VersionID, ret); err != nil {
		t.Fatalf("retention on v2: %v", err)
	}
	if err := b.PutObjectLegalHold(ctx, "bk", "k", out3.VersionID, false); err != nil {
		t.Fatalf("hold on v3: %v", err)
	}
	if _, err := deleteObjV(t, b, "k", out3.VersionID); err != nil {
		t.Fatalf("delete current: %v", err)
	}
	// v2 promoted to current; its retention survives, v3's entry is gone.
	if got, err := b.GetObjectRetention(ctx, "bk", "k", ""); err != nil || !bytes.Equal(got, ret) {
		t.Fatalf("promoted current retention = (%s, %v), want survivor's", got, err)
	}
	if rv, err = b.resolveVersion(ctx, "bk", "k", ""); err != nil || rv.leaf == nil || rv.leaf.State == nil {
		t.Fatalf("promoted leaf state = %+v (%v), want surviving tree", rv.leaf, err)
	}
}

func TestObjectLock_CreationTimeStamping(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"

	// Lock headers against a bucket without lock enabled: every
	// creation-time path reports the NoSpaces variant of the §2 error
	// (PutObject_missing_bucket_lock pins the wording; the spaced variant
	// belongs to the four per-version methods).
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key, mode := "locked", types.ObjectLockModeGovernance
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	_, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")),
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
	})
	if err == nil {
		t.Fatal("lock headers on non-lock bucket: want error")
	}
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfigurationNoSpaces)
	_, err = b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &key,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	if err == nil {
		t.Fatal("MPU lock headers on non-lock bucket: want error")
	}
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfigurationNoSpaces)

	// An absent retain-until header arrives as a pointer to the ZERO time
	// (the controller populates the pointer unconditionally): a plain PUT
	// must not trip the lock-header validation.
	var zero time.Time
	plainOnUnlocked := "plain-zero-date"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &plainOnUnlocked, Body: bytes.NewReader([]byte("x")),
		ObjectLockRetainUntilDate: &zero,
	}); err != nil {
		t.Fatalf("plain PUT with zero retain-until pointer: %v", err)
	}

	lockBucket(t, b)

	// PutObject stamps retention + hold in the same commit; Head echoes them.
	out, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")),
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	if err != nil {
		t.Fatalf("PutObject with lock headers: %v", err)
	}
	gotRet, err := b.GetObjectRetention(ctx, "bk", key, out.VersionID)
	if err != nil {
		t.Fatalf("GetObjectRetention: %v", err)
	}
	var parsed types.ObjectLockRetention
	if err := json.Unmarshal(gotRet, &parsed); err != nil {
		t.Fatalf("parse stamped retention: %v", err)
	}
	if parsed.Mode != types.ObjectLockRetentionModeGovernance || parsed.RetainUntilDate == nil || !parsed.RetainUntilDate.Equal(until) {
		t.Fatalf("stamped retention = %+v, want (GOVERNANCE, %v)", parsed, until)
	}
	if hold, err := b.GetObjectLegalHold(ctx, "bk", key, out.VersionID); err != nil || hold == nil || !*hold {
		t.Fatalf("stamped hold = (%v, %v), want &true", hold, err)
	}
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ObjectLockMode != types.ObjectLockModeGovernance ||
		head.ObjectLockRetainUntilDate == nil || !head.ObjectLockRetainUntilDate.Equal(until) ||
		head.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
		t.Fatalf("Head echo = (%v, %v, %v), want stamped values",
			head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	}
	// A version without state echoes nothing.
	plainKey := "plain"
	putObjV(t, b, plainKey, []byte("y"))
	head, err = b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &plainKey})
	if err != nil {
		t.Fatalf("HeadObject plain: %v", err)
	}
	if head.ObjectLockMode != "" || head.ObjectLockRetainUntilDate != nil || head.ObjectLockLegalHoldStatus != "" {
		t.Fatalf("plain Head echo = (%v, %v, %v), want empty",
			head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	}

	// CopyObject: destination takes only the request's own headers; lock
	// state is never inherited from the source.
	dst, src := "copied", bucket+"/"+key
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dst, CopySource: &src,
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if _, err := b.GetObjectRetention(ctx, "bk", dst, ""); err == nil {
		t.Fatal("copy inherited lock state: want unset")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	}
	dst2 := "copied-locked"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dst2, CopySource: &src,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	}); err != nil {
		t.Fatalf("CopyObject with hold: %v", err)
	}
	if hold, err := b.GetObjectLegalHold(ctx, "bk", dst2, ""); err != nil || hold == nil || !*hold {
		t.Fatalf("copy-stamped hold = (%v, %v), want &true", hold, err)
	}

	// Multipart: Create carries the headers on the session, Complete stamps
	// them like a single-shot PUT.
	mpKey := "mp-locked"
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &mpKey,
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := res.UploadId
	part, err := mpUploadPart(t, b, mpKey, uploadID, 1, []byte("part-1-bytes"), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	if _, err := mpComplete(t, b, mpKey, uploadID, []types.CompletedPart{{ETag: part.ETag, PartNumber: &one}}, nil); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	gotRet, err = b.GetObjectRetention(ctx, "bk", mpKey, "")
	if err != nil {
		t.Fatalf("GetObjectRetention after Complete: %v", err)
	}
	if err := json.Unmarshal(gotRet, &parsed); err != nil || parsed.Mode != types.ObjectLockRetentionModeGovernance {
		t.Fatalf("MPU-stamped retention = (%s, %v), want GOVERNANCE", gotRet, err)
	}
}
