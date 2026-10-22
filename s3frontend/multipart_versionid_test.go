package s3frontend

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/inmem"
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
	b, _ := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
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
	for _, sse := range encryptions {
		if err == nil && sse != res.ServerSideEncryption {
			t.Errorf("reported encryption %q, result has %q", sse, res.ServerSideEncryption)
		}
	}
	return reported, versionID, err
}

// currentVersionID is the version id HEAD reports for key's current version.
func currentVersionID(t *testing.T, b *Backend, key string) string {
	t.Helper()
	bucket := "bk"
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("HeadObject %s: %v", key, err)
	}
	if out.VersionId == nil {
		return ""
	}
	return *out.VersionId
}

func sessionState(t *testing.T, store registry.MultipartStore, uploadID string) string {
	t.Helper()
	sess, err := store.GetSession(context.Background(), uploadID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(announced) != 1 || announced[0] == "" {
		t.Fatalf("announced %q, want one version id", announced)
	}
	if versionID != announced[0] {
		t.Fatalf("Complete returned version %q, announced %q", versionID, announced[0])
	}
	if got := currentVersionID(t, b, key); got != versionID {
		t.Fatalf("current version %q, want the announced %q", got, versionID)
	}
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
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if !slices.Equal(announced, []string{""}) || versionID != "" {
				t.Fatalf("reported %q, returned %q; want one empty report and no version", announced, versionID)
			}
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
	if !errors.Is(err, errReservationSuperseded) {
		t.Fatalf("Complete: err = %v, want errReservationSuperseded", err)
	}
	if got := currentVersionID(t, b, key); got != put.VersionID {
		t.Fatalf("current version %q, want the PUT's %q", got, put.VersionID)
	}
	if st := sessionState(t, mp.multipart, uploadID); st != registry.SessionOpen {
		t.Fatalf("session state %q after the refusal, want open", st)
	}

	retried, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	if err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	if len(retried) != 1 || retried[0] != versionID || versionID == announced[0] {
		t.Fatalf("retry announced %q and returned %q, want one fresh id (first attempt announced %q)", retried, versionID, announced)
	}
	if got := currentVersionID(t, b, key); got != versionID {
		t.Fatalf("current version %q, want the retry's %q", got, versionID)
	}
}

// Suspending versioning while Complete accepts the parts would make its
// version null, so the announced id can't be committed.
func TestCompleteRefusedWhenVersioningIsSuspendedDuringIt(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "suspended"
	uploadID, parts := completeTwoParts(t, b, key)
	mp.hook = func() { setVersioning(t, b, types.BucketVersioningStatusSuspended) }

	if _, _, err := completeAnnounced(t, b, key, uploadID, parts); !errors.Is(err, errReservationSuperseded) {
		t.Fatalf("Complete: err = %v, want errReservationSuperseded", err)
	}
	if st := sessionState(t, mp.multipart, uploadID); st != registry.SessionOpen {
		t.Fatalf("session state %q after the refusal, want open", st)
	}

	announced, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	if err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	if !slices.Equal(announced, []string{""}) || versionID != "" {
		t.Fatalf("retry in the suspended bucket reported %q and returned %q, want one empty report and no version", announced, versionID)
	}
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
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if (first != "") != (status == types.BucketVersioningStatusEnabled) {
				t.Fatalf("Complete returned version %q", first)
			}

			announced, again, err := completeAnnounced(t, b, key, uploadID, parts)
			if err != nil {
				t.Fatalf("re-Complete: %v", err)
			}
			if again != first {
				t.Fatalf("re-Complete returned version %q, want the first Complete's %q", again, first)
			}
			want := []string{first}
			if !slices.Equal(announced, want) {
				t.Fatalf("re-Complete announced %q, want %q", announced, want)
			}
		})
	}
}

// A Complete that finds another Complete of its upload in progress reports
// "" before it waits, since the winner's id isn't known yet. When the winner
// fails and this caller then wins the latch, its commit would create a
// numbered version it never reported, so it is refused retryably, and the
// retry reports the id it commits.
func TestCompleteThatWaitsOnTheWinnerReportsNoVersion(t *testing.T) {
	b, mp := newHookedCompleteBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	key := "loser"
	uploadID, parts := completeTwoParts(t, b, key)
	ctx := context.Background()
	if won, err := mp.multipart.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
		t.Fatalf("latch as another Complete: won=%v err=%v", won, err)
	}
	// The other Complete fails once this one has reported, reopening the
	// upload as its revert does.
	reopen := func(string) {
		if won, err := mp.multipart.LatchSession(ctx, uploadID, registry.SessionCompleting, registry.SessionOpen); err != nil || !won {
			t.Errorf("reopen as the failed Complete: won=%v err=%v", won, err)
		}
	}

	reported, _, err := completeReporting(t, b, key, uploadID, parts, reopen)
	if !errors.Is(err, errReservationSuperseded) {
		t.Fatalf("Complete: err = %v, want errReservationSuperseded", err)
	}
	if !slices.Equal(reported, []string{""}) {
		t.Fatalf("reported %q, want one empty report", reported)
	}
	if st := sessionState(t, mp.multipart, uploadID); st != registry.SessionOpen {
		t.Fatalf("session state %q after the refusal, want open", st)
	}

	retried, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	if err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	if len(retried) != 1 || retried[0] == "" || retried[0] != versionID || currentVersionID(t, b, key) != versionID {
		t.Fatalf("retry reported %q and returned %q, current is %q; want the same non-empty id", retried, versionID, currentVersionID(t, b, key))
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
	if !errors.Is(err, errReservationSuperseded) {
		t.Fatalf("Complete: err = %v, want errReservationSuperseded", err)
	}
	if !slices.Equal(reported, []string{""}) {
		t.Fatalf("reported %q, want one empty report", reported)
	}
	if st := sessionState(t, mp.multipart, uploadID); st != registry.SessionOpen {
		t.Fatalf("session state %q after the refusal, want open", st)
	}

	retried, versionID, err := completeAnnounced(t, b, key, uploadID, parts)
	if err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	if len(retried) != 1 || retried[0] == "" || retried[0] != versionID {
		t.Fatalf("retry reported %q and returned %q, want the same non-empty id", retried, versionID)
	}
}
