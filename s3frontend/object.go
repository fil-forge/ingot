package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

const defaultMaxKeys = 1000

// PutObject writes an object. Tagging, ACLs, checksums, retention,
// and preconditions are dropped on the floor for now — the manifest
// schema has no place for them yet (see bucket-metadata.rfc
// §"Canonical state vs service state"). ETag is the hex md5 of the
// body, quoted per S3 wire format.
func (b *Backend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	bucketName := *input.Bucket
	key := *input.Key
	if !mst.IsValidKey(key) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	contentType := backend.GetStringFromPtr(input.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Bucket must exist before we spool + upload, so a PUT to a missing bucket
	// doesn't waste an upload. WithTx re-checks under the per-bucket lock.
	if _, err := b.reg.Get(ctx, bucketName); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put: %w", err)
	}

	// PRECONDITIONS (no lock): If-Match / If-None-Match. Evaluated here to fail
	// fast before ingest, then RE-CHECKED under the per-bucket lock at commit so
	// a concurrent writer can't slip between this read and the swap (§3).
	if input.IfMatch != nil || input.IfNoneMatch != nil {
		etag, exists, err := b.currentObjectETag(ctx, bucketName, key)
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
		if err := backend.EvaluateObjectPutPreconditions(etag, input.IfMatch, input.IfNoneMatch, exists); err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	// Additional checksum (x-amz-checksum-*): wrap the body so the requested
	// algorithm is computed — and validated against a client-supplied value —
	// during the single ingest pass.
	spec, err := checksumFromInput(input)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	bodyReader := input.Body
	var hr *utils.HashReader
	if spec != nil {
		src := input.Body
		if src == nil {
			src = bytes.NewReader(nil)
		}
		hr, err = utils.NewHashReader(src, spec.expected, spec.hashType)
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
		bodyReader = hr
	}

	// INGEST (no lock): stream the body → coarse-split into blobs → spool each
	// to local disk → upload each to Forge by digest (allocate→PUT→accept). A
	// 200 means every body blob is durable and accepted before the manifest
	// that references it is committed (docs/architecture.md §7.1).
	bodyRec, err := b.ingestBody(ctx, bucketName, bodyReader)
	if err != nil {
		// A checksum mismatch surfaces as an API error from the HashReader.
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return s3response.PutObjectOutput{}, apiErr
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put ingest: %w", err)
	}
	var ckAlgo, ckVal string
	if hr != nil {
		ckAlgo, ckVal = string(spec.algo), hr.Sum()
	}

	// mf is captured by the closure and read after WithTx commits, so the
	// response ETag/size come from the same manifest that was committed.
	// oldDigests captures the superseded version's body digests (an overwrite),
	// so the reference index can be reconciled AFTER the commit is durable.
	var mf *msbucket.ObjectManifest
	var oldDigests [][]byte

	// COMMIT (short per-bucket critical section): write the manifest + MST
	// splice + guarded root swap. No large-body work happens under the lock; the
	// reference-index reconcile runs after, so a failed commit can't diverge
	// blob_refs from the committed catalog.
	err = b.txns.WithTx(ctx, bucketName, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		mf = &msbucket.ObjectManifest{
			Key:                key,
			ContentType:        contentType,
			Created:            time.Now().Unix(),
			Body:               bodyRec,
			ETag:               hex.EncodeToString(bodyRec.MD5),
			ChecksumAlgorithm:  ckAlgo,
			Checksum:           ckVal,
			ContentEncoding:    backend.GetStringFromPtr(input.ContentEncoding),
			ContentDisposition: backend.GetStringFromPtr(input.ContentDisposition),
			ContentLanguage:    backend.GetStringFromPtr(input.ContentLanguage),
			CacheControl:       backend.GetStringFromPtr(input.CacheControl),
			Expires:            backend.GetStringFromPtr(input.Expires),
			Metadata:           input.Metadata,
		}
		mfCid, err := tx.Put(ctx, mf)
		if err != nil {
			return cid.Undef, fmt.Errorf("manifest put: %w", err)
		}

		t := tx.LoadTree()

		// Capture the prior version's body digests + ETag (if this is an
		// overwrite) before replacing the leaf, so the reference index can
		// release blobs the new body no longer references and the precondition
		// re-check sees the current state.
		var oldETag string
		oldCid, gerr := t.Get(ctx, key)
		oldExists := gerr == nil
		switch {
		case gerr == nil:
			var oldMf msbucket.ObjectManifest
			if err := tx.Get(ctx, oldCid, &oldMf); err != nil {
				return cid.Undef, fmt.Errorf("load prior manifest: %w", err)
			}
			oldDigests = bodyDigests(oldMf.Body)
			oldETag = etagOf(&oldMf)
			if err := b.gc.AddGCCandidate(ctx, oldCid.Bytes(), bucketName); err != nil {
				return cid.Undef, fmt.Errorf("gc candidate: %w", err)
			}
		case errors.Is(gerr, mst.ErrNotFound):
			// new key — no prior version
		default:
			return cid.Undef, fmt.Errorf("mst get prior: %w", gerr)
		}

		// Race-safe re-check of If-Match / If-None-Match against the current
		// state under the lock (no-ops when neither header is set).
		if err := backend.EvaluateObjectPutPreconditions(oldETag, input.IfMatch, input.IfNoneMatch, oldExists); err != nil {
			return cid.Undef, err
		}

		t2, err := t.Add(ctx, key, mfCid, -1)
		if errors.Is(err, mst.ErrAlreadyExists) {
			t2, err = t.Update(ctx, key, mfCid) // unversioned overwrite-in-place
		}
		if err != nil {
			return cid.Undef, fmt.Errorf("mst write: %w", err)
		}

		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return s3response.PutObjectOutput{}, mapCommitError(err, "put")
	}

	// Reference index, after the commit is durable: claim the new body's blobs
	// and release the superseded ones (overwrite). Done post-commit so a commit
	// failure can never leave blob_refs out of step with the catalog.
	toRemove, err := b.reconcileClaims(ctx, bucketName, key, oldDigests, bodyDigests(bodyRec))
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put reconcile: %w", err)
	}
	b.releaseBlobs(ctx, toRemove)

	size := mf.Body.Size
	out := s3response.PutObjectOutput{
		ETag: etagOf(mf),
		Size: &size,
	}
	out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumType = checksumFields(ckAlgo, ckVal)
	return out, nil
}

// ingestBody streams an object body off-lock: it coarse-splits into blobs and
// spools each to local disk (SplitBody → Spool), records an upload_intents row
// per blob, uploads each to Forge by digest, and advances the intent to
// accepted. It returns the Body the manifest will pin. A zero-byte body yields
// a Body with no blobs and uploads nothing.
//
// On any error the already-spooled/parked blobs and their intents are left for
// crash recovery to reconcile (a later phase); no manifest is written, so no
// catalog entry ever references a non-durable blob.
func (b *Backend) ingestBody(ctx context.Context, bucket string, r io.Reader) (msbucket.Body, error) {
	body, err := b.splitSpool(ctx, bucket, r)
	if err != nil {
		return msbucket.Body{}, err
	}
	if err := b.uploadBlobs(ctx, body.Blobs); err != nil {
		return msbucket.Body{}, err
	}
	return body, nil
}

// splitSpool coarse-splits a body into blobs, writes each to the local spool,
// and records a spooled upload_intents row per blob — WITHOUT uploading. It is
// the shared first half of ingest: a single-shot PUT follows it with
// uploadBlobs immediately; a multipart UploadPart spools here and defers the
// upload to Complete.
func (b *Backend) splitSpool(ctx context.Context, bucket string, r io.Reader) (msbucket.Body, error) {
	if r == nil {
		r = bytes.NewReader(nil)
	}
	body, err := msbucket.SplitBody(ctx, b.spool, r, b.maxBlobSize)
	if err != nil {
		return msbucket.Body{}, fmt.Errorf("split body: %w", err)
	}
	for _, blob := range body.Blobs {
		if err := b.intents.PutIntent(ctx, registry.UploadIntent{
			Digest:    blob.Digest,
			LocalPath: b.spool.Path(multihash.Multihash(blob.Digest)),
			Size:      blob.Length,
			State:     registry.IntentSpooled,
			Bucket:    bucket,
		}); err != nil {
			return msbucket.Body{}, fmt.Errorf("record intent: %w", err)
		}
	}
	return body, nil
}

// uploadBlobs uploads each spooled blob to Forge by digest (allocate→PUT→
// accept), advances its intent to accepted, and records its location. A no-op
// in the in-memory harness (the spool serves reads).
//
// A blob already durably stored for this space (a re-PUT of identical content,
// an overwrite-in-place, or a blob shared with an earlier object) is skipped:
// its bytes are already on Forge, so re-uploading is pure waste — and worse,
// re-adding an already-stored blob makes the upload service return an accept
// receipt with no fresh location commitment, which the edge client (correctly)
// rejects. We detect this via the recorded location and short-circuit, matching
// guppy's "blob already has location; skip /blob/add". (A crash between accept
// and the location record could still re-add; that window closes with the
// deferred upload_intents × blob_locations crash recovery — see §12.)
func (b *Backend) uploadBlobs(ctx context.Context, blobs []msbucket.BlobRef) error {
	for _, blob := range blobs {
		digest := multihash.Multihash(blob.Digest)
		if existing, err := b.locations.GetLocation(ctx, b.space, blob.Digest); err == nil && existing != nil {
			// Already durable for this space — advance the intent and move on.
			if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
				return fmt.Errorf("mark accepted (dedup): %w", err)
			}
			continue
		} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup location: %w", err)
		}
		loc, err := b.uploader.UploadBlob(ctx, digest, blob.Length, b.spool.Path(digest))
		if err != nil {
			return fmt.Errorf("upload blob: %w", err)
		}
		if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
			return fmt.Errorf("mark accepted: %w", err)
		}
		// Best-effort location record (unused in the harness, where reads come
		// from the spool); keyed by (space, digest).
		if err := b.locations.PutLocation(ctx, registry.BlobLocation{
			Space:    b.space,
			Digest:   blob.Digest,
			Provider: loc.Provider,
			URL:      loc.URL,
			Size:     loc.Size,
		}); err != nil {
			return fmt.Errorf("record location: %w", err)
		}
	}
	return nil
}

// reconcileClaims updates blob_refs for an object version under (bucket, key)
// whose body changes from oldDigests to newDigests, and returns the digests
// whose (space, digest) claim reached zero so the caller can release them after
// the commit. The diff is the crux of safe dedup + delete:
//
//   - a digest in new but not old gains a claim (newly referenced);
//   - a digest in old but not new loses its claim, and when no version anywhere
//     still references it, is queued for RemoveBlob;
//   - a digest in BOTH keeps its single claim untouched, so a re-PUT of
//     identical bytes (or a split object that shares blobs) never churns the row.
//
// versionId is the unversioned sentinel for now — one claim row per
// (digest, bucket, key). When versioning lands, each version carries its own id.
//
// It MUST run only after the catalog/root commit succeeds: it mutates blob_refs
// in its own (non-transactional) store, so applying it before a commit that then
// fails would diverge blob_refs from the committed catalog (and a later delete
// of a shared blob could drop a still-referenced one). Iterates over the
// DEDUPLICATED digest sets, so a manifest carrying the same digest in two blobs
// adds/deletes one claim and releases the blob at most once.
func (b *Backend) reconcileClaims(ctx context.Context, bucket, key string, oldDigests, newDigests [][]byte) (toRemove [][]byte, err error) {
	oldSet := digestSet(oldDigests)
	newSet := digestSet(newDigests)

	for k, d := range newSet {
		if _, ok := oldSet[k]; ok {
			continue // unchanged reference
		}
		if err := b.blobRefs.AddBlobClaim(ctx, registry.BlobClaim{
			Digest: d, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: b.space,
		}); err != nil {
			return nil, fmt.Errorf("add blob claim: %w", err)
		}
	}
	for k, d := range oldSet {
		if _, ok := newSet[k]; ok {
			continue // still referenced by the new body
		}
		if err := b.blobRefs.DeleteBlobClaim(ctx, d, bucket, key, registry.NullVersionID); err != nil {
			return nil, fmt.Errorf("delete blob claim: %w", err)
		}
		n, err := b.blobRefs.CountClaims(ctx, b.space, d)
		if err != nil {
			return nil, fmt.Errorf("count claims: %w", err)
		}
		if n == 0 {
			toRemove = append(toRemove, d)
		}
	}
	return toRemove, nil
}

// releaseBlobs calls RemoveBlob for each digest whose last claim was dropped.
// Run after the commit lands, off the critical section — a 200 is not gated on
// the (currently no-op) network release. Failures are logged, not fatal: a
// missed release leaks bytes on Piri but never loses referenced data, and crash
// recovery reconciles upload_intents × blob_refs (a later phase).
func (b *Backend) releaseBlobs(ctx context.Context, digests [][]byte) {
	for _, d := range digests {
		if err := b.remover.RemoveBlob(ctx, multihash.Multihash(d)); err != nil {
			// best-effort; see method doc.
			_ = err
		}
	}
}

// bodyDigests returns the digests of a body's blobs in order.
func bodyDigests(body msbucket.Body) [][]byte {
	out := make([][]byte, 0, len(body.Blobs))
	for _, blob := range body.Blobs {
		out = append(out, blob.Digest)
	}
	return out
}

// digestSet maps each distinct digest (by its bytes) to one representative
// []byte, so callers can both test membership and recover the digest while
// processing every digest exactly once.
func digestSet(ds [][]byte) map[string][]byte {
	s := make(map[string][]byte, len(ds))
	for _, d := range ds {
		s[string(d)] = d
	}
	return s
}

// selectBytes resolves which bytes a GET/HEAD addresses and the parts-count to
// report. A ?partNumber=N request (partNumber != nil) selects a part via
// partRange; otherwise the Range header is parsed (with a nil parts-count).
// versitygw rejects supplying both upstream, so at most one applies here.
func selectBytes(body msbucket.Body, partNumber *int32, rangeHeader string) (start, length int64, isRange bool, partsCount *int32, err error) {
	if partNumber != nil {
		return partRange(body, *partNumber)
	}
	start, length, isRange, err = backend.ParseObjectRange(body.Size, rangeHeader)
	return start, length, isRange, nil, err
}

// partRange maps a ?partNumber=N request to its byte span and the
// x-amz-mp-parts-count to report. For a multipart object (Body.PartSizes set)
// part N is the N-th recorded part and the parts-count is the number of parts.
// A single-PUT object has no parts: the whole object is the one addressable part
// (N must be 1, parts-count nil — S3 omits x-amz-mp-parts-count for non-multipart
// objects), and a zero-byte object yields no Content-Range at all. A partNumber
// beyond the part count is ErrInvalidPartNumberRange (HTTP 416). partNumber < 1
// is rejected by versitygw before reaching the backend.
func partRange(body msbucket.Body, partNumber int32) (start, length int64, isRange bool, partsCount *int32, err error) {
	if parts := body.PartSizes; len(parts) > 0 {
		if partNumber < 1 || int(partNumber) > len(parts) {
			return 0, 0, false, nil, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)
		}
		for _, sz := range parts[:partNumber-1] {
			start += sz
		}
		n := int32(len(parts))
		length := parts[partNumber-1]
		// A zero-length part has no byte span: omit Content-Range (as for a
		// zero-byte object) rather than emit a malformed "bytes start-(start-1)".
		// The parts-count is still reported.
		return start, length, length > 0, &n, nil
	}
	// Non-multipart object: a single logical part covering the whole body.
	if partNumber != 1 {
		return 0, 0, false, nil, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)
	}
	if body.Size == 0 {
		// A zero-byte object has no range; S3 omits Content-Range and returns 200.
		return 0, 0, false, nil, nil
	}
	return 0, body.Size, true, nil, nil
}

// HeadObject returns an object's metadata, honoring the conditional-request
// preconditions and the same byte-selection as GetObject (?partNumber=N or a
// Range header → a 206 with Content-Range, plus x-amz-mp-parts-count for a
// multipart part). Versioning and tagging are not implemented.
func (b *Backend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	mf, err := b.lookupManifest(ctx, *input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	lastModified := time.Unix(mf.Created, 0)
	if err := backend.EvaluatePreconditions(etagOf(mf), lastModified, backend.PreConditions{
		IfMatch:       input.IfMatch,
		IfNoneMatch:   input.IfNoneMatch,
		IfModSince:    input.IfModifiedSince,
		IfUnmodeSince: input.IfUnmodifiedSince,
	}); err != nil {
		return nil, err
	}
	etag := etagOf(mf)
	objSize := mf.Body.Size

	// A request selects bytes either by ?partNumber=N or by Range (versitygw
	// rejects both together upstream). Both yield a 206 with Content-Range; a
	// partNumber also carries x-amz-mp-parts-count for a multipart object.
	startOffset, length, isRange, partsCount, err := selectBytes(mf.Body, input.PartNumber, backend.GetStringFromPtr(input.Range))
	if err != nil {
		return nil, err
	}
	var contentRange *string
	if isRange {
		cr := fmt.Sprintf("bytes %d-%d/%d", startOffset, startOffset+length-1, objSize)
		contentRange = &cr
	}

	contentType := mf.ContentType
	out := &s3.HeadObjectOutput{
		AcceptRanges:       backend.GetPtrFromString("bytes"),
		ContentLength:      &length,
		ContentType:        &contentType,
		ContentEncoding:    strPtrOrNil(mf.ContentEncoding),
		ContentDisposition: strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:    strPtrOrNil(mf.ContentLanguage),
		CacheControl:       strPtrOrNil(mf.CacheControl),
		ExpiresString:      strPtrOrNil(mf.Expires),
		Metadata:           mf.Metadata,
		ContentRange:       contentRange,
		PartsCount:         partsCount,
		ETag:               &etag,
		LastModified:       &lastModified,
		StorageClass:       types.StorageClassStandard,
	}
	// Echo the stored checksum only for a whole-object HEAD with checksum mode on
	// (a ranged HEAD's checksum would not match the full object).
	if input.ChecksumMode == types.ChecksumModeEnabled && !isRange {
		out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum)
	}
	return out, nil
}

// GetObject returns an object body, optionally restricted to a byte
// range supplied via the Range header. The body io.ReadCloser is
// owned by the caller (versitygw closes it after streaming).
func (b *Backend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	mf, err := b.lookupManifest(ctx, *input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	if err := backend.EvaluatePreconditions(etagOf(mf), time.Unix(mf.Created, 0), backend.PreConditions{
		IfMatch:       input.IfMatch,
		IfNoneMatch:   input.IfNoneMatch,
		IfModSince:    input.IfModifiedSince,
		IfUnmodeSince: input.IfUnmodifiedSince,
	}); err != nil {
		return nil, err
	}

	objSize := mf.Body.Size
	// Select bytes by ?partNumber=N or by Range (versitygw rejects both together);
	// partsCount is non-nil only for a multipart object addressed by part number.
	startOffset, length, isRange, partsCount, err := selectBytes(mf.Body, input.PartNumber, backend.GetStringFromPtr(input.Range))
	if err != nil {
		return nil, err
	}

	var contentRange *string
	var body = msbucket.OpenBody(ctx, b.read, mf.Body)
	if isRange {
		body = msbucket.OpenBodyRange(ctx, b.read, mf.Body, startOffset, startOffset+length-1)
		cr := fmt.Sprintf("bytes %d-%d/%d", startOffset, startOffset+length-1, objSize)
		contentRange = &cr
	}

	etag := etagOf(mf)
	lastModified := time.Unix(mf.Created, 0)
	contentType := mf.ContentType
	out := &s3.GetObjectOutput{
		AcceptRanges:       backend.GetPtrFromString("bytes"),
		Body:               body,
		ContentLength:      &length,
		ContentType:        &contentType,
		ContentEncoding:    strPtrOrNil(mf.ContentEncoding),
		ContentDisposition: strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:    strPtrOrNil(mf.ContentLanguage),
		CacheControl:       strPtrOrNil(mf.CacheControl),
		ExpiresString:      strPtrOrNil(mf.Expires),
		Metadata:           mf.Metadata,
		ContentRange:       contentRange,
		PartsCount:         partsCount,
		ETag:               &etag,
		LastModified:       &lastModified,
		StorageClass:       types.StorageClassStandard,
	}
	// Echo the stored checksum only for a whole-object GET with checksum mode on
	// (a ranged GET's checksum would not match the full object).
	if input.ChecksumMode == types.ChecksumModeEnabled && !isRange {
		out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum)
	}
	return out, nil
}

// DeleteObject removes an object. Missing keys are no-ops (matching
// S3's idempotent DELETE semantics).
func (b *Backend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	preconds := &backend.ObjectDeletePreconditions{
		IfMatch:            input.IfMatch,
		IfMatchLastModTime: input.IfMatchLastModifiedTime,
		IfMatchSize:        input.IfMatchSize,
	}
	if err := b.deleteObjectKey(ctx, *input.Bucket, *input.Key, preconds); err != nil {
		return nil, err
	}
	return &s3.DeleteObjectOutput{}, nil
}

// deleteObjectKey removes one key, releasing its body blobs through the
// reference index. Missing keys (and an empty bucket) are idempotent no-ops.
// preconds, when non-nil, gates the delete on If-Match / size / mod-time under
// the lock. Shared by DeleteObject and DeleteObjects.
func (b *Backend) deleteObjectKey(ctx context.Context, bucketName, key string, preconds *backend.ObjectDeletePreconditions) error {
	var oldDigests [][]byte
	err := b.txns.WithTx(ctx, bucketName, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		// Empty bucket: nothing to delete. Returning cid.Undef tells WithTx to
		// discard with no commit — the equivalent of "no-op success."
		if !tx.State().Root.Defined() {
			return cid.Undef, nil
		}
		t := tx.LoadTree()

		// Load the manifest being removed so its body blobs can be released
		// through the reference index.
		oldCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, nil // idempotent DELETE: missing key isn't an error
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		var oldMf msbucket.ObjectManifest
		if err := tx.Get(ctx, oldCid, &oldMf); err != nil {
			return cid.Undef, fmt.Errorf("load manifest: %w", err)
		}

		// Preconditions (If-Match / size / mod-time) under the lock against the
		// version being removed.
		if preconds != nil {
			if err := backend.EvaluateObjectDeletePreconditions(etagOf(&oldMf), time.Unix(oldMf.Created, 0), oldMf.Body.Size, *preconds); err != nil {
				return cid.Undef, err
			}
		}

		t2, err := t.Delete(ctx, key)
		if err != nil {
			return cid.Undef, fmt.Errorf("mst delete: %w", err)
		}
		if err := b.gc.AddGCCandidate(ctx, oldCid.Bytes(), bucketName); err != nil {
			return cid.Undef, fmt.Errorf("gc candidate: %w", err)
		}
		oldDigests = bodyDigests(oldMf.Body)
		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return mapCommitError(err, "delete")
	}
	// Release the removed version's blobs through the reference index AFTER the
	// commit is durable (so a commit failure can't diverge blob_refs). When the
	// key was absent, oldDigests is nil and this is a no-op.
	toRemove, err := b.reconcileClaims(ctx, bucketName, key, oldDigests, nil)
	if err != nil {
		return fmt.Errorf("s3frontend: delete reconcile: %w", err)
	}
	b.releaseBlobs(ctx, toRemove)
	return nil
}

// DeleteObjects deletes up to 1000 keys in one request, best-effort (not
// atomic): each key is deleted independently and reported in the per-key result
// (Deleted, or Error on failure). Quiet mode omits the successful entries.
func (b *Backend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if input.Bucket == nil {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Delete == nil {
		return s3response.DeleteResult{}, nil
	}
	if len(input.Delete.Objects) > defaultMaxKeys {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	bucketName := *input.Bucket

	// A missing bucket fails the whole request (matches S3).
	if _, err := b.reg.Get(ctx, bucketName); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.DeleteResult{}, fmt.Errorf("s3frontend: delete objects: %w", err)
	}

	quiet := input.Delete.Quiet != nil && *input.Delete.Quiet
	var res s3response.DeleteResult
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		if err := b.deleteObjectKey(ctx, bucketName, key, nil); err != nil {
			k := key
			code, msg := deleteErrorFields(err)
			res.Error = append(res.Error, types.Error{Key: &k, Code: &code, Message: &msg})
			continue
		}
		if !quiet {
			k := key
			res.Deleted = append(res.Deleted, types.DeletedObject{Key: &k})
		}
	}
	return res, nil
}

// deleteErrorFields maps a per-key delete failure to an S3 error code +
// message for the DeleteObjects per-entry result.
func deleteErrorFields(err error) (code, msg string) {
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code, apiErr.Description
	}
	return "InternalError", err.Error()
}

// ListObjects (V1) walks the MST in lexicographic order, applying
// S3-style prefix / delimiter filtering with V1's Marker-based
// pagination.
func (b *Backend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucketName := *input.Bucket
	prefix := backend.GetStringFromPtr(input.Prefix)
	delimiter := backend.GetStringFromPtr(input.Delimiter)
	marker := backend.GetStringFromPtr(input.Marker)

	maxKeys := int32(0)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	limit := int(maxKeys)
	if limit <= 0 {
		limit = defaultMaxKeys
	}

	from := prefix
	if marker != "" && marker > from {
		// V1 Marker: list strictly after this key.
		from = marker + "\x01"
	}

	res, err := b.listWalk(ctx, bucketName, prefix, delimiter, from, limit)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	out := s3response.ListObjectsResult{
		Name:           &bucketName,
		Prefix:         &prefix,
		Delimiter:      &delimiter,
		MaxKeys:        &maxKeys,
		IsTruncated:    &res.truncated,
		Contents:       res.contents,
		CommonPrefixes: res.commonPrefixes,
	}
	if input.Marker != nil {
		out.Marker = input.Marker
	}
	// NextMarker is only set when delimiter is specified and the
	// page was truncated, per AWS docs. Without delimiter, callers
	// use the last Key in Contents as the marker for the next page.
	if res.truncated && delimiter != "" && res.nextKey != "" {
		next := res.nextKey
		out.NextMarker = &next
	}
	return out, nil
}

// ListObjectsV2 walks the MST in lexicographic order, applying
// S3-style prefix and delimiter filtering with V2's
// ContinuationToken-based pagination.
func (b *Backend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucketName := *input.Bucket
	prefix := backend.GetStringFromPtr(input.Prefix)
	delimiter := backend.GetStringFromPtr(input.Delimiter)

	// ContinuationToken (resumption) takes precedence over StartAfter
	// (first-page hint) per S3 semantics.
	startAfter := backend.GetStringFromPtr(input.StartAfter)
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		startAfter = *input.ContinuationToken
	}

	maxKeys := int32(0)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	limit := int(maxKeys)
	if limit <= 0 {
		limit = defaultMaxKeys
	}

	from := prefix
	if startAfter != "" && startAfter > from {
		// Walk strictly past startAfter by appending a low byte.
		from = startAfter + "\x01"
	}

	res, err := b.listWalk(ctx, bucketName, prefix, delimiter, from, limit)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	keyCount := int32(len(res.contents) + len(res.commonPrefixes))
	out := s3response.ListObjectsV2Result{
		Name:           &bucketName,
		Prefix:         &prefix,
		Delimiter:      &delimiter,
		MaxKeys:        &maxKeys,
		KeyCount:       &keyCount,
		IsTruncated:    &res.truncated,
		Contents:       res.contents,
		CommonPrefixes: res.commonPrefixes,
	}
	if input.ContinuationToken != nil {
		out.ContinuationToken = input.ContinuationToken
	}
	if input.StartAfter != nil {
		out.StartAfter = input.StartAfter
	}
	if res.truncated && res.nextKey != "" {
		next := res.nextKey
		out.NextContinuationToken = &next
	}
	return out, nil
}

// listWalkResult is the shared output of one MST walk for V1 and V2
// list. nextKey is the last key (or common prefix) that ended the
// page when truncated; empty when the walk completed.
type listWalkResult struct {
	contents       []s3response.Object
	commonPrefixes []types.CommonPrefix
	truncated      bool
	nextKey        string
}

// listWalk drives a single MST walk shared by ListObjects and
// ListObjectsV2. The version-specific pieces (Marker vs.
// ContinuationToken / StartAfter, NextMarker vs.
// NextContinuationToken) live in the callers; this helper only
// understands prefix, delimiter, and the [from, ...) starting key.
func (b *Backend) listWalk(ctx context.Context, bucketName, prefix, delimiter, from string, limit int) (listWalkResult, error) {
	out := listWalkResult{
		contents:       []s3response.Object{},
		commonPrefixes: []types.CommonPrefix{},
	}

	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return out, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return out, err
	}
	if !st.Root.Defined() {
		return out, nil
	}

	t := mst.LoadMST(b.read, st.Root)
	seenPrefix := map[string]struct{}{}
	walkErr := t.WalkLeavesFromNocache(ctx, from, func(k string, mfCid cid.Cid) error {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			return mst.ErrStopWalk
		}

		if delimiter != "" {
			tail := k[len(prefix):]
			if i := strings.Index(tail, delimiter); i >= 0 {
				cp := prefix + tail[:i+len(delimiter)]
				if _, dup := seenPrefix[cp]; !dup {
					seenPrefix[cp] = struct{}{}
					cpCopy := cp
					out.commonPrefixes = append(out.commonPrefixes, types.CommonPrefix{Prefix: &cpCopy})
					if len(out.contents)+len(out.commonPrefixes) >= limit {
						out.truncated = true
						out.nextKey = cp
						return mst.ErrStopWalk
					}
				}
				return nil
			}
		}

		var mf msbucket.ObjectManifest
		if err := b.read.Get(ctx, mfCid, &mf); err != nil {
			return fmt.Errorf("manifest get %s: %w", mfCid, err)
		}
		key := k
		etag := etagOf(&mf)
		size := mf.Body.Size
		lastModified := time.Unix(mf.Created, 0)
		out.contents = append(out.contents, s3response.Object{
			Key:          &key,
			ETag:         &etag,
			Size:         &size,
			LastModified: &lastModified,
			StorageClass: types.ObjectStorageClassStandard,
		})
		if len(out.contents)+len(out.commonPrefixes) >= limit {
			out.truncated = true
			out.nextKey = k
			return mst.ErrStopWalk
		}
		return nil
	})
	if walkErr != nil {
		return out, fmt.Errorf("s3frontend: walk: %w", walkErr)
	}
	return out, nil
}

// lookupManifest is the shared HEAD/GET path: registry → MST → CBOR
// decode of the manifest pointed at by the leaf. Maps "missing
// bucket" / "missing key" to S3 errors.
func (b *Backend) lookupManifest(ctx context.Context, bucketName, key string) (*msbucket.ObjectManifest, error) {
	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	if !st.Root.Defined() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	t := mst.LoadMST(b.read, st.Root)
	mfCid, err := t.Get(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return nil, fmt.Errorf("s3frontend: mst get: %w", err)
	}
	var mf msbucket.ObjectManifest
	if err := b.read.Get(ctx, mfCid, &mf); err != nil {
		return nil, fmt.Errorf("s3frontend: manifest get: %w", err)
	}
	return &mf, nil
}

// etagOf returns the manifest's S3 ETag, double-quoted per the wire
// format. The ETag is stored verbatim on the manifest (hex md5 for a
// single-part object; "<md5-of-md5s>-<N>" for a multipart object, which
// cannot be re-derived from the body bytes). Falls back to the body md5
// for any manifest written without a stored ETag.
func etagOf(mf *msbucket.ObjectManifest) string {
	tag := mf.ETag
	if tag == "" {
		tag = hex.EncodeToString(mf.Body.MD5)
	}
	return `"` + tag + `"`
}

// strPtrOrNil returns a pointer to s, or nil when s is empty, so empty
// system headers are omitted from S3 responses rather than emitted blank.
func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
