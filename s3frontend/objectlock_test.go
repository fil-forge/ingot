package s3frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "marshal lock config")
	require.NoError(t, b.PutObjectLockConfiguration(context.Background(), "bk", cfg), "PutObjectLockConfiguration")
	return cfg
}

// wantAPIErr asserts err is exactly the given s3err code's APIError.
func wantAPIErr(t *testing.T, err error, code s3err.ErrorCode) {
	t.Helper()
	want := s3err.GetAPIError(code)
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "err = %v, want APIError %s", err, want.Code)
	require.Equal(t, want, apiErr)
}

func retentionDoc(t *testing.T, mode types.ObjectLockRetentionMode, until time.Time) []byte {
	t.Helper()
	doc, err := json.Marshal(types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until})
	require.NoError(t, err, "marshal retention")
	return doc
}

func TestObjectLock_BucketConfiguration(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// Never configured: the §2 not-found sentinel; a missing bucket outranks it.
	_, err := b.GetObjectLockConfiguration(ctx, "bk")
	require.Error(t, err, "unconfigured GetObjectLockConfiguration: want error")
	wantAPIErr(t, err, s3err.ErrObjectLockConfigurationNotFound)
	_, err = b.GetObjectLockConfiguration(ctx, "nope")
	require.Error(t, err, "missing bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)

	// Lock requires versioning Enabled (unversioned here): 409.
	cfg := []byte(`{"Enabled":true}`)
	err = b.PutObjectLockConfiguration(ctx, "bk", cfg)
	require.Error(t, err, "PutObjectLockConfiguration on unversioned bucket: want error")
	wantAPIErr(t, err, s3err.ErrObjectLockConfigurationNotAllowed)

	// Enabling later on a versioned bucket is allowed, and the document is
	// returned verbatim.
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	require.NoError(t, b.PutObjectLockConfiguration(ctx, "bk", cfg), "PutObjectLockConfiguration")
	got, err := b.GetObjectLockConfiguration(ctx, "bk")
	require.NoError(t, err, "GetObjectLockConfiguration")
	require.Equal(t, cfg, got, "config round-trip")

	// The §5 guard: a lock bucket can never be suspended.
	err = b.PutBucketVersioning(ctx, "bk", types.BucketVersioningStatusSuspended)
	require.Error(t, err, "suspend of lock bucket: want error")
	wantAPIErr(t, err, s3err.ErrSuspendedVersioningNotAllowed)
}

func TestObjectLock_CheckOrderSentinels(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	// Bucket without lock enabled: the missing-configuration sentinel for an
	// EXISTING key, but key existence outranks it — a missing key answers
	// NoSuchKey even without lock (GetObjectRetention_non_existing_object).
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k", []byte("v1"))
	_, err := b.GetObjectRetention(ctx, "bk", "absent", "")
	require.Error(t, err, "missing key on non-lock bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)
	err = b.PutObjectRetention(ctx, "bk", "absent", "", []byte(`{}`))
	require.Error(t, err, "put on missing key of non-lock bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)
	_, err = b.GetObjectRetention(ctx, "bk", "k", "")
	require.Error(t, err, "disabled lock: want error")
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfiguration)
	err = b.PutObjectLegalHold(ctx, "bk", "k", "", true)
	require.Error(t, err, "disabled lock put: want error")
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfiguration)
	// A missing bucket outranks everything.
	_, err = b.GetObjectLegalHold(ctx, "nope", "k", "")
	require.Error(t, err, "missing bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)

	lockBucket(t, b)
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))

	// Missing key; unknown version; malformed version; unset state.
	_, err = b.GetObjectRetention(ctx, "bk", "absent", "")
	require.Error(t, err, "missing key: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)
	err = b.PutObjectRetention(ctx, "bk", "absent", "", ret)
	require.Error(t, err, "put on missing key: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)
	_, err = b.GetObjectRetention(ctx, "bk", "k", "01G65Z755AFWAKHE12NY0CQ9FH")
	require.Error(t, err, "unknown version: want error")
	require.Equal(t, "NoSuchVersion", apiErrCode(t, err), "unknown version code")
	err = b.PutObjectRetention(ctx, "bk", "k", "not-a-version", ret)
	require.Error(t, err, "malformed version: want error")
	require.Equal(t, "InvalidArgument", apiErrCode(t, err), "malformed version code")
	_, err = b.GetObjectRetention(ctx, "bk", "k", "")
	require.Error(t, err, "unset retention: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	_, err = b.GetObjectLegalHold(ctx, "bk", "k", "")
	require.Error(t, err, "unset hold: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)

	// A delete marker rejects lock operations with 405.
	del, err := deleteObjV(t, b, "k", "")
	require.NoError(t, err, "insert marker")
	markerID := *del.VersionId
	_, err = b.GetObjectRetention(ctx, "bk", "k", markerID)
	require.Error(t, err, "marker retention get: want error")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	err = b.PutObjectRetention(ctx, "bk", "k", markerID, ret)
	require.Error(t, err, "marker retention put: want error")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	// Current is the marker: the unscoped read is 404-shaped, and the lock
	// path surfaces the marker sentinel for CheckObjectAccess.
	_, err = b.GetObjectLegalHold(ctx, "bk", "k", "")
	require.Error(t, err, "current-marker hold get: want error")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
}

func TestObjectLock_RetentionAndHoldRoundTrip(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)

	// First lock write upgrades a single-version (manifest-arm) key to a
	// leaf; reads and lists keep working through the upgrade.
	out1 := putObjV(t, b, "k", []byte("v1"))
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour).UTC().Truncate(time.Second))
	require.NoError(t, b.PutObjectRetention(ctx, "bk", "k", "", ret), "PutObjectRetention")
	got, err := b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "GetObjectRetention")
	require.Equal(t, ret, got, "retention round-trip")
	// The explicit version id resolves to the same state.
	got, err = b.GetObjectRetention(ctx, "bk", "k", out1.VersionID)
	require.NoError(t, err, "versioned GetObjectRetention")
	require.Equal(t, ret, got, "versioned GetObjectRetention")
	_, data, err := getObjV(t, b, "k", "")
	require.NoError(t, err, "read after leaf upgrade")
	require.Equal(t, "v1", string(data), "read after leaf upgrade")

	// Tri-valued hold: never-set is 400 (asserted above); OFF and ON are
	// explicit states, and the merge carries retention across hold writes.
	require.NoError(t, b.PutObjectLegalHold(ctx, "bk", "k", "", false), "PutObjectLegalHold(off)")
	hold, err := b.GetObjectLegalHold(ctx, "bk", "k", "")
	require.NoError(t, err, "hold after OFF")
	require.NotNil(t, hold, "hold after OFF")
	require.False(t, *hold, "hold after OFF")
	require.NoError(t, b.PutObjectLegalHold(ctx, "bk", "k", "", true), "PutObjectLegalHold(on)")
	hold, err = b.GetObjectLegalHold(ctx, "bk", "k", "")
	require.NoError(t, err, "hold after ON")
	require.NotNil(t, hold, "hold after ON")
	require.True(t, *hold, "hold after ON")
	got, err = b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "retention after hold writes")
	require.Equal(t, ret, got, "retention after hold writes")

	// Retention writes carry the hold in return, and replacement is
	// last-write-wins on the owned field.
	ret2 := retentionDoc(t, types.ObjectLockRetentionModeCompliance, time.Now().Add(2*time.Hour).UTC().Truncate(time.Second))
	require.NoError(t, b.PutObjectRetention(ctx, "bk", "k", "", ret2), "replace retention")
	got, err = b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "replaced retention")
	require.Equal(t, ret2, got, "replaced retention")
	hold, err = b.GetObjectLegalHold(ctx, "bk", "k", "")
	require.NoError(t, err, "hold after retention replace")
	require.NotNil(t, hold, "hold after retention replace")
	require.True(t, *hold, "hold after retention replace")

	// State survives supersession: the overwrite stacks a new version and
	// the old version keeps its lock state; the new version has none.
	out2 := putObjV(t, b, "k", []byte("v2"))
	got, err = b.GetObjectRetention(ctx, "bk", "k", out1.VersionID)
	require.NoError(t, err, "noncurrent retention")
	require.Equal(t, ret2, got, "noncurrent retention")
	_, err = b.GetObjectRetention(ctx, "bk", "k", out2.VersionID)
	require.Error(t, err, "new version inherited retention: want unset")
	wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)

	// Hold set directly on a noncurrent version.
	require.NoError(t, b.PutObjectLegalHold(ctx, "bk", "k", out1.VersionID, false), "hold on noncurrent")
	hold, err = b.GetObjectLegalHold(ctx, "bk", "k", out1.VersionID)
	require.NoError(t, err, "noncurrent hold")
	require.NotNil(t, hold, "noncurrent hold")
	require.False(t, *hold, "noncurrent hold")
}

func TestObjectLock_CleanupOnVersionDelete(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)

	out1 := putObjV(t, b, "k", []byte("v1"))
	out2 := putObjV(t, b, "k", []byte("v2"))
	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))
	require.NoError(t, b.PutObjectRetention(ctx, "bk", "k", out1.VersionID, ret), "retention on noncurrent")

	// Deleting the noncurrent version removes its state entry; the emptied
	// tree drops off the leaf.
	_, err := deleteObjV(t, b, "k", out1.VersionID)
	require.NoError(t, err, "delete noncurrent")
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err, "resolve after delete")
	require.NotNil(t, rv.leaf, "leaf after last state entry removed")
	require.Nil(t, rv.leaf.State, "leaf after last state entry removed")

	// Promotion prunes the removed current's entry and keeps the survivor's.
	out3 := putObjV(t, b, "k", []byte("v3"))
	require.NoError(t, b.PutObjectRetention(ctx, "bk", "k", out2.VersionID, ret), "retention on v2")
	require.NoError(t, b.PutObjectLegalHold(ctx, "bk", "k", out3.VersionID, false), "hold on v3")
	_, err = deleteObjV(t, b, "k", out3.VersionID)
	require.NoError(t, err, "delete current")
	// v2 promoted to current; its retention survives, v3's entry is gone.
	got, err := b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "promoted current retention")
	require.Equal(t, ret, got, "promoted current retention")
	rv, err = b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err, "promoted leaf state")
	require.NotNil(t, rv.leaf, "promoted leaf")
	require.NotNil(t, rv.leaf.State, "promoted leaf state")
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
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x")),
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
	})
	require.Error(t, err, "lock headers on non-lock bucket: want error")
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfigurationNoSpaces)
	_, err = b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &key,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	require.Error(t, err, "MPU lock headers on non-lock bucket: want error")
	wantAPIErr(t, err, s3err.ErrMissingObjectLockConfigurationNoSpaces)

	// An absent retain-until header arrives as a pointer to the ZERO time
	// (the controller populates the pointer unconditionally): a plain PUT
	// must not trip the lock-header validation.
	var zero time.Time
	plainOnUnlocked := "plain-zero-date"
	_, err = b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &plainOnUnlocked, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x")),
		ObjectLockRetainUntilDate: &zero,
	})
	require.NoError(t, err, "plain PUT with zero retain-until pointer")

	lockBucket(t, b)

	// PutObject stamps retention + hold in the same commit; Head echoes them.
	out, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x")),
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	require.NoError(t, err, "PutObject with lock headers")
	gotRet, err := b.GetObjectRetention(ctx, "bk", key, out.VersionID)
	require.NoError(t, err, "GetObjectRetention")
	var parsed types.ObjectLockRetention
	require.NoError(t, json.Unmarshal(gotRet, &parsed), "parse stamped retention")
	require.Equal(t, types.ObjectLockRetentionModeGovernance, parsed.Mode, "stamped retention")
	require.NotNil(t, parsed.RetainUntilDate, "stamped retention")
	require.True(t, parsed.RetainUntilDate.Equal(until), "stamped retention = %+v, want (GOVERNANCE, %v)", parsed, until)
	hold, err := b.GetObjectLegalHold(ctx, "bk", key, out.VersionID)
	require.NoError(t, err, "stamped hold")
	require.NotNil(t, hold, "stamped hold")
	require.True(t, *hold, "stamped hold")
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "HeadObject")
	require.Equal(t, types.ObjectLockModeGovernance, head.ObjectLockMode, "Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	require.NotNil(t, head.ObjectLockRetainUntilDate, "Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	require.True(t, head.ObjectLockRetainUntilDate.Equal(until), "Head echo = (%v, %v, %v), want stamped values", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	require.Equal(t, types.ObjectLockLegalHoldStatusOn, head.ObjectLockLegalHoldStatus, "Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	// A version without state echoes nothing.
	plainKey := "plain"
	putObjV(t, b, plainKey, []byte("y"))
	head, err = b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &plainKey})
	require.NoError(t, err, "HeadObject plain")
	require.Empty(t, head.ObjectLockMode, "plain Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	require.Nil(t, head.ObjectLockRetainUntilDate, "plain Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)
	require.Empty(t, head.ObjectLockLegalHoldStatus, "plain Head echo = (%v, %v, %v)", head.ObjectLockMode, head.ObjectLockRetainUntilDate, head.ObjectLockLegalHoldStatus)

	// CopyObject: destination takes only the request's own headers; lock
	// state is never inherited from the source.
	dst, src := "copied", bucket+"/"+key
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dst, CopySource: &src,
	})
	require.NoError(t, err, "CopyObject")
	_, err = b.GetObjectRetention(ctx, "bk", dst, "")
	require.Error(t, err, "copy inherited lock state: want unset")
	wantAPIErr(t, err, s3err.ErrNoSuchObjectLockConfiguration)
	dst2 := "copied-locked"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dst2, CopySource: &src,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	require.NoError(t, err, "CopyObject with hold")
	hold, err = b.GetObjectLegalHold(ctx, "bk", dst2, "")
	require.NoError(t, err, "copy-stamped hold")
	require.NotNil(t, hold, "copy-stamped hold")
	require.True(t, *hold, "copy-stamped hold")

	// Multipart: Create carries the headers on the session, Complete stamps
	// them like a single-shot PUT.
	mpKey := "mp-locked"
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &mpKey,
		ObjectLockMode: mode, ObjectLockRetainUntilDate: &until,
	})
	require.NoError(t, err, "CreateMultipartUpload")
	uploadID := res.UploadId
	part, err := mpUploadPart(t, b, mpKey, uploadID, 1, []byte("part-1-bytes"), nil)
	require.NoError(t, err, "UploadPart")
	one := int32(1)
	_, err = mpComplete(t, b, mpKey, uploadID, []types.CompletedPart{{ETag: part.ETag, PartNumber: &one}}, nil)
	require.NoError(t, err, "CompleteMultipartUpload")
	gotRet, err = b.GetObjectRetention(ctx, "bk", mpKey, "")
	require.NoError(t, err, "GetObjectRetention after Complete")
	require.NoError(t, json.Unmarshal(gotRet, &parsed), "MPU-stamped retention %s", gotRet)
	require.Equal(t, types.ObjectLockRetentionModeGovernance, parsed.Mode, "MPU-stamped retention %s", gotRet)
}
