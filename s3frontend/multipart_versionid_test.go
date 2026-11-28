package s3frontend

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
)

// A multipart Complete reports its version id before it accepts the parts
// (backend.CompleteMultipartUploadReporter), so versitygw can send
// x-amz-version-id with a streamed response's early 200, as S3 does. An
// Enabled bucket's Complete reports the id it reserved; the others report "".

// completeHooks is a registry that runs hook once, right after its next
// version seq allocation: in an Enabled bucket's Complete, the reservation,
// before the parts are accepted and the version commits.
type completeHooks struct {
	registry.Registry
	hook func()
	// multipart is the backend's multipart store.
	multipart registry.MultipartStore
}

func (h *completeHooks) AllocVersionSeq(ctx context.Context, name string) (uint64, error) {
	seq, err := h.Registry.AllocVersionSeq(ctx, name)
	if h.hook != nil {
		hook := h.hook
		h.hook = nil
		hook()
	}
	return seq, err
}

// newHookedCompleteBackend is a deferred backend whose Complete runs the
// returned hook between its reservation and its commit.
func newHookedCompleteBackend(t *testing.T) (*Backend, *completeHooks) {
	t.Helper()
	h := &completeHooks{}
	b, _ := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		h.Registry = d.Registry
		d.Registry = h
		h.multipart = d.Multipart
	})
	return b, h
}

// completeAnnounced completes an upload, recording every version id it
// reports.
func completeAnnounced(t *testing.T, b *Backend, key, uploadID string, parts []types.CompletedPart) (announced []string, versionID string, err error) {
	t.Helper()
	return completeReporting(t, b, key, uploadID, parts, func(id string) {
		announced = append(announced, id)
	})
}

// completeReporting completes an upload with report seeing each version id
// it reports, and checks that a successful result carries the encryption
// it reported.
func completeReporting(t *testing.T, b *Backend, key, uploadID string, parts []types.CompletedPart, report func(string)) (announced []string, versionID string, err error) {
	t.Helper()
	bucket := "bk"
	var reported []string
	var encryptions []types.ServerSideEncryption
	res, versionID, err := b.CompleteMultipartUploadReportingHeaders(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}, func(r backend.CompleteMultipartUploadHeaders) {
		reported = append(reported, r.VersionID)
		encryptions = append(encryptions, r.ServerSideEncryption)
		report(r.VersionID)
	})
	if err == nil {
		for _, sse := range encryptions {
			assert.Equal(t, res.ServerSideEncryption, sse, "reported encryption")
		}
	}
	return reported, versionID, err
}

// currentVersionID is the version id HEAD reports for key's current version.
func currentVersionID(t *testing.T, b *Backend, key string) string {
	t.Helper()
	bucket := "bk"
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "HeadObject %s", key)
	if out.VersionId == nil {
		return ""
	}
	return *out.VersionId
}

func sessionState(t *testing.T, store registry.MultipartStore, uploadID string) string {
	t.Helper()
	sess, err := store.GetSession(context.Background(), uploadID)
	require.NoError(t, err, "GetSession")
	return sess.State
}

func TestCompleteAnnouncesTheVersionItCommits(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "announced"
	uploadID, parts := completeTwoParts(t, b, key)
	// A write to another key in the meantime takes a later seq and leaves
	// this key's reservation its newest version.
	mp.hook = func() { putObjV(t, b, "other", []byte("other")) }

	announced, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	require.NoError(t, err, "Complete")
	require.Len(t, announced, 1, "announced")
	require.NotEmpty(t, announced[0], "announced")
	require.Equal(t, announced[0], versionID, "Complete returned version")
	got := currentVersionID(t, b, key)
	require.Equal(t, versionID, got, "current version %q", got)
}

func TestCompleteReportsNoVersionOutsideAnEnabledBucket(t *testing.T) {
	for name, status := range map[string]types.BucketVersioningStatus{
		"unversioned": "",
		"suspended":   types.BucketVersioningStatusSuspended,
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := newHookedCompleteBackend(t)
			if status != "" {
				setVersioning(t, b, status)
			}
			key := "unannounced"
			uploadID, parts := completeTwoParts(t, b, key)

			announced, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
			require.NoError(t, err, "Complete")
			require.True(t, slices.Equal(announced, []string{""}), "reported %q, returned %q; want one empty report and no version", announced, versionID)
			require.Empty(t, versionID, "reported %q, returned %q; want one empty report and no version", announced, versionID)
		})
	}
}

// A write to the key that commits while Complete accepts the parts holds a
// later seq than the announced version, which can no longer be the key's
// newest. Complete fails retryably and reopens the upload, and the retry
// commits a fresh version over the write.
func TestCompleteRefusedWhenTheKeyIsWrittenDuringIt(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "raced"
	uploadID, parts := completeTwoParts(t, b, key)
	var put s3response.PutObjectOutput
	mp.hook = func() { put = putObjV(t, b, key, []byte("written during the completion")) }

	announced, _, err := completeAnnounced(t, b, key, uploadID, parts)
	require.ErrorIs(t, err, bucketqueue.ErrReservationSuperseded, "Complete: err")
	got := currentVersionID(t, b, key)
	require.Equal(t, put.VersionID, got, "current version %q", got)
	st := sessionState(t, mp.multipart, uploadID)
	require.Equal(t, registry.SessionOpen, st, "session state %q after the refusal", st)

	retried, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	require.NoError(t, err, "retried Complete")
	require.Len(t, retried, 1, "retry announced %q and returned %q, want one fresh id (first attempt announced %q)", retried, versionID, announced)
	require.Equal(t, versionID, retried[0], "retry announced %q and returned %q, want one fresh id (first attempt announced %q)", retried, versionID, announced)
	require.NotEqual(t, announced[0], versionID, "retry announced %q and returned %q, want one fresh id (first attempt announced %q)", retried, versionID, announced)
	require.Equal(t, versionID, currentVersionID(t, b, key), "current version")
}

// Suspending versioning while Complete accepts the parts would make its
// version null, so the announced id can't be committed.
func TestCompleteRefusedWhenVersioningIsSuspendedDuringIt(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "suspended"
	uploadID, parts := completeTwoParts(t, b, key)
	mp.hook = func() { setVersioning(t, b, types.BucketVersioningStatusSuspended) }

	_, _, err := completeAnnounced(t, b, key, uploadID, parts)
	require.ErrorIs(t, err, bucketqueue.ErrReservationSuperseded, "Complete")
	st := sessionState(t, mp.multipart, uploadID)
	require.Equal(t, registry.SessionOpen, st, "session state %q after the refusal", st)

	announced, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	require.NoError(t, err, "retried Complete")
	require.True(t, slices.Equal(announced, []string{""}), "retry in the suspended bucket reported %q and returned %q, want one empty report and no version", announced, versionID)
	require.Empty(t, versionID, "retry in the suspended bucket reported %q and returned %q, want one empty report and no version", announced, versionID)
}

// An idempotent re-Complete replays the version the first Complete
// committed: the same id, reported at once, in an Enabled bucket, and no id
// in the others, whose first Complete sends none either.
func TestReCompleteReplaysTheVersionID(t *testing.T) {
	for name, status := range map[string]types.BucketVersioningStatus{
		"enabled":     types.BucketVersioningStatusEnabled,
		"unversioned": "",
		"suspended":   types.BucketVersioningStatusSuspended,
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := newHookedCompleteBackend(t)
			if status != "" {
				setVersioning(t, b, status)
			}
			key := "replayed"
			uploadID, parts := completeTwoParts(t, b, key)
			_, first, err := completeAnnounced(t, b, key, uploadID, parts)
			require.NoError(t, err, "Complete")
			require.Equal(t, status == types.BucketVersioningStatusEnabled, first != "", "Complete returned version %q", first)

			announced, again, err := completeAnnounced(t, b, key, uploadID, parts)
			require.NoError(t, err, "re-Complete")
			require.Equal(t, first, again, "re-Complete returned version %q", again)
			want := []string{first}
			require.True(t, slices.Equal(announced, want), "re-Complete announced %q, want %q", announced, want)
		})
	}
}

// Enabling versioning while a Complete in an unversioned bucket accepts its
// parts would have it create a numbered version after reporting none, so the
// commit is refused retryably, and the retry reports the id it commits.
func TestCompleteRefusedWhenVersioningIsEnabledDuringIt(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	key := "enabled"
	uploadID, parts := completeTwoParts(t, b, key)

	reported, _, err := completeReporting(t, b, key, uploadID, parts, func(string) {
		setVersioning(t, b, types.BucketVersioningStatusEnabled)
	})
	require.ErrorIs(t, err, bucketqueue.ErrReservationSuperseded, "Complete: err")
	require.True(t, slices.Equal(reported, []string{""}), "reported %q, want one empty report", reported)
	st := sessionState(t, mp.multipart, uploadID)
	require.Equal(t, registry.SessionOpen, st, "session state %q after the refusal", st)

	retried, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	require.NoError(t, err, "retried Complete")
	require.Len(t, retried, 1, "retry reported %q and returned %q, want the same non-empty id", retried, versionID)
	require.NotEmpty(t, retried[0], "retry reported %q and returned %q, want the same non-empty id", retried, versionID)
	require.Equal(t, versionID, retried[0], "retry reported %q and returned %q, want the same non-empty id", retried, versionID)
}

// TestCompleteRunAgainKeepsItsReportedVersion: a Complete whose commit fails
// its guard runs again from scratch, after it has reported its version id.
// The run again installs that id, which the client already has, rather than
// reserving another one.
func TestCompleteRunAgainKeepsItsReportedVersion(t *testing.T) {
	b, mem := newDeferredBackend(t, &parkingUploader{})
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "rerun"
	uploadID, parts := completeTwoParts(t, b, key)

	// The first move of the session to completed affects no row, so the
	// Complete's guard fails its first commit. The sequence is not
	// transactional, so the rolled-back commit still counts.
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE SEQUENCE ingot.test_skip_complete`,
		`CREATE FUNCTION ingot.test_skip_complete() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN
		   IF NEW.state = 'completed' AND nextval('ingot.test_skip_complete') = 1 THEN
		     RETURN NULL;
		   END IF;
		   RETURN NEW;
		 END $$`,
		`CREATE TRIGGER test_skip_complete BEFORE UPDATE ON ingot.multipart_sessions
		 FOR EACH ROW EXECUTE FUNCTION ingot.test_skip_complete()`,
	} {
		_, err := mem.pool.Exec(ctx, sql)
		require.NoError(t, err, "install the skip")
	}

	announced, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	require.NoError(t, err, "Complete")
	var n int64
	err = mem.pool.QueryRow(ctx, `SELECT last_value FROM ingot.test_skip_complete`).Scan(&n)
	require.NoError(t, err, "read the Complete's commit count")
	require.GreaterOrEqual(t, n, int64(2), "the Complete's commit ran %d times", n)
	require.Len(t, announced, 1, "reported")
	require.NotEmpty(t, announced[0], "reported")
	require.Equal(t, announced[0], versionID, "Complete returned version")
	require.Equal(t, announced[0], currentVersionID(t, b, key), "current version")
}
