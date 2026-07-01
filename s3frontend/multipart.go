package s3frontend

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ipfs/go-cid"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// newUploadID returns a random 128-bit hex upload id.
func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// CreateMultipartUpload opens a multipart session: it records the destination
// bucket/key plus the content-type and user metadata so Complete can write the
// manifest without the client resupplying them, and returns the upload id.
func (b *Backend) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket, key := *input.Bucket, *input.Key
	if !mst.IsValidKey(key) {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}
	if _, err := b.reg.Get(ctx, bucket); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("s3frontend: create mpu: %w", err)
	}
	uploadID, err := newUploadID()
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	ct := backend.GetStringFromPtr(input.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	if err := b.multipart.CreateSession(ctx, registry.MultipartSession{
		UploadID:    uploadID,
		Bucket:      bucket,
		ObjectKey:   key,
		State:       registry.SessionOpen,
		ContentType: ct,
		Metadata:    input.Metadata,
	}); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("s3frontend: create session: %w", err)
	}
	return s3response.InitiateMultipartUploadResult{Bucket: bucket, Key: key, UploadId: uploadID}, nil
}

// UploadPart ingests one part: it coarse-splits the part body into blobs and
// spools each to local disk (recording upload_intents), then records the part
// (its ordered blob digests, md5, size). The part's blobs are NOT uploaded to
// Forge yet — that is deferred to Complete (so an Abort only ever cleans up
// local state). The part ETag is the hex md5 of the part bytes.
func (b *Backend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.PartNumber == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return nil, fmt.Errorf("s3frontend: upload part: %w", err)
	}

	body, err := b.splitSpool(ctx, sess.Bucket, input.Body)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: upload part ingest: %w", err)
	}
	if err := b.multipart.PutPart(ctx, registry.MultipartPart{
		UploadID:    uploadID,
		PartNumber:  int(*input.PartNumber),
		ETagMD5:     body.MD5,
		Size:        body.Size,
		BlobDigests: bodyDigests(body),
		State:       registry.PartParked,
	}); err != nil {
		return nil, fmt.Errorf("s3frontend: record part: %w", err)
	}
	etag := `"` + hex.EncodeToString(body.MD5) + `"`
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

// CompleteMultipartUpload assembles the final object from the uploaded parts:
// it latches the session (single-winner vs Abort), validates the client's part
// list against the recorded parts, accepts every part's blobs on Forge, and
// commits a manifest whose Body is the ordered union of the parts' blobs. The
// object ETag is hex(md5(concat of part md5s)) + "-N".
func (b *Backend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket, key, uploadID := *input.Bucket, *input.Key, *input.UploadId
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: complete: %w", err)
	}
	// Single-winner latch vs a racing Abort: only the writer that moves the
	// session off 'open' proceeds (§7.3).
	won, err := b.multipart.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionCompleting)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: latch: %w", err)
	}
	if !won {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	// If anything below fails before the object is committed, revert the session
	// to 'open' so the upload stays abortable / retriable rather than zombied in
	// 'completing'. committed is set once the manifest is durable (the point of
	// no return).
	committed := false
	defer func() {
		if !committed {
			_, _ = b.multipart.LatchSession(ctx, uploadID, registry.SessionCompleting, registry.SessionOpen)
		}
	}()

	if input.MultipartUpload == nil || len(input.MultipartUpload.Parts) == 0 {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	stored, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: list parts: %w", err)
	}
	byNum := make(map[int]registry.MultipartPart, len(stored))
	for _, p := range stored {
		byNum[p.PartNumber] = p
	}

	// Validate the requested parts (ascending, each matching a recorded part by
	// number + ETag) and assemble the ordered body + the multipart ETag.
	var blobs []msbucket.BlobRef
	var partSizes []int64
	var offset int64
	etagHasher := md5.New()
	prev := 0
	for _, rp := range input.MultipartUpload.Parts {
		if rp.PartNumber == nil {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		num := int(*rp.PartNumber)
		if num <= prev {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prev = num
		sp, ok := byNum[num]
		if !ok {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if rp.ETag != nil && !etagsEqual(*rp.ETag, hex.EncodeToString(sp.ETagMD5)) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		etagHasher.Write(sp.ETagMD5)
		// Record this part's byte span (it may span several blobs) so a later
		// GET/HEAD ?partNumber=N can address it (§7.2).
		partStart := offset
		for _, d := range sp.BlobDigests {
			in, err := b.intents.GetIntent(ctx, d)
			if err != nil {
				return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: part blob %x: %w", d, err)
			}
			blobs = append(blobs, msbucket.BlobRef{Digest: d, Offset: offset, Length: in.Size})
			offset += in.Size
		}
		partSizes = append(partSizes, offset-partStart)
	}

	// Accept every part's blobs on Forge (no-op in the harness), then commit.
	if err := b.uploadBlobs(ctx, blobs); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: accept parts: %w", err)
	}

	etag := hex.EncodeToString(etagHasher.Sum(nil)) + "-" + strconv.Itoa(len(input.MultipartUpload.Parts))
	mf := &msbucket.ObjectManifest{
		Key:         key,
		ContentType: sess.ContentType,
		Created:     time.Now().Unix(),
		Body:        msbucket.Body{Size: offset, Blobs: blobs, PartSizes: partSizes},
		ETag:        etag,
		Metadata:    sess.Metadata,
	}

	if err := b.commitManifest(ctx, bucket, key, mf, bodyDigests(mf.Body)); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	committed = true
	// The object is durable; session cleanup is best-effort (a lingering session
	// is harmless and reapable later, and must not fail an otherwise-good
	// Complete).
	_ = b.multipart.DeleteSession(ctx, uploadID)

	etagQ := `"` + etag + `"`
	return s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etagQ}, "", nil
}

// AbortMultipartUpload cancels a multipart upload: it latches the session
// (single-winner vs Complete) and drops it (cascading its parts). The spooled
// part blobs are content-addressed and may be shared, so they are left for GC
// rather than deleted here; no reference claims were taken (those happen only at
// Complete), so nothing else needs unwinding.
func (b *Backend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	if input.UploadId == nil {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	if _, err := b.multipart.GetSession(ctx, uploadID); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return fmt.Errorf("s3frontend: abort: %w", err)
	}
	won, err := b.multipart.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionAborting)
	if err != nil {
		return fmt.Errorf("s3frontend: latch: %w", err)
	}
	if !won {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if err := b.multipart.DeleteSession(ctx, uploadID); err != nil {
		return fmt.Errorf("s3frontend: delete session: %w", err)
	}
	return nil
}

// commitManifest splices mf into (bucket, key) and reconciles the reference
// index against the prior version's digests, releasing dropped blobs after the
// commit. Shared by CopyObject and CompleteMultipartUpload (a plain commit with
// no precondition callback).
func (b *Backend) commitManifest(ctx context.Context, bucket, key string, mf *msbucket.ObjectManifest, newDigests [][]byte) error {
	var oldDigests [][]byte
	err := b.txns.WithTx(ctx, bucket, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		mfCid, err := tx.Put(ctx, mf)
		if err != nil {
			return cid.Undef, fmt.Errorf("manifest put: %w", err)
		}
		t := tx.LoadTree()

		oldCid, gerr := t.Get(ctx, key)
		switch {
		case gerr == nil:
			var oldMf msbucket.ObjectManifest
			if err := tx.Get(ctx, oldCid, &oldMf); err != nil {
				return cid.Undef, fmt.Errorf("load prior manifest: %w", err)
			}
			oldDigests = bodyDigests(oldMf.Body)
			if err := b.gc.AddGCCandidate(ctx, oldCid.Bytes(), bucket); err != nil {
				return cid.Undef, fmt.Errorf("gc candidate: %w", err)
			}
		case errors.Is(gerr, mst.ErrNotFound):
		default:
			return cid.Undef, fmt.Errorf("mst get prior: %w", gerr)
		}

		t2, err := t.Add(ctx, key, mfCid, -1)
		if errors.Is(err, mst.ErrAlreadyExists) {
			t2, err = t.Update(ctx, key, mfCid)
		}
		if err != nil {
			return cid.Undef, fmt.Errorf("mst write: %w", err)
		}

		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return mapCommitError(err, "commit")
	}
	// Reconcile the reference index AFTER the commit is durable (so a commit
	// failure can't diverge blob_refs from the catalog).
	toRemove, err := b.reconcileClaims(ctx, bucket, key, oldDigests, newDigests)
	if err != nil {
		return fmt.Errorf("s3frontend: commit reconcile: %w", err)
	}
	b.releaseBlobs(ctx, toRemove)
	return nil
}

// etagsEqual compares two ETags ignoring surrounding quotes.
func etagsEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}
