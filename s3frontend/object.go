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
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/filecoin-project/go-fee"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

const defaultMaxKeys = 1000

// PutObject writes an object. Tagging and ACLs are dropped on the floor for
// now (see bucket-metadata.rfc §"Canonical state vs service state"); lock
// headers stamp the new version's state (docs/s3-object-lock.md §7). ETag is
// the hex md5 of the body, quoted per S3 wire format.
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
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	contentType := backend.GetStringFromPtr(input.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Bucket must exist before we spool + upload, so a PUT to a missing bucket
	// doesn't waste an upload. WithTx re-checks under the per-bucket lock.
	bucketState, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put: %w", err)
	}

	// x-amz-object-lock-* headers: validated against the bucket's lock
	// configuration before ingest (fail fast), stamped into the commit below
	// (docs/s3-object-lock.md §7). The x-amz-tagging header joins the same
	// initial state (docs/s3-object-tagging.md §4); an invalid header fails
	// before ingest and uploads nothing.
	initState, err := lockStateFromHeaders(bucketState, input.ObjectLockMode, input.ObjectLockRetainUntilDate, input.ObjectLockLegalHoldStatus)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	tags, err := backend.ParseObjectTags(backend.GetStringFromPtr(input.Tagging))
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	initState = applyTagsIfPresent(initState, tags)

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
	bodyRec, err := b.ingestBody(ctx, bucketState, bodyReader)
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

	// COMMIT (short per-bucket critical section): the §5 write rule — seq
	// allocation, manifest + rebuilt leaf + MST splice + guarded root swap,
	// then the post-commit reference-index reconcile (docs/s3-versioning.md).
	mf := &msbucket.ObjectManifest{
		Key:                     key,
		ContentType:             contentType,
		Created:                 time.Now().Unix(),
		Body:                    bodyRec,
		ETag:                    hex.EncodeToString(bodyRec.MD5),
		ChecksumAlgorithm:       ckAlgo,
		Checksum:                ckVal,
		ChecksumType:            string(types.ChecksumTypeFullObject),
		ContentEncoding:         backend.GetStringFromPtr(input.ContentEncoding),
		ContentDisposition:      backend.GetStringFromPtr(input.ContentDisposition),
		ContentLanguage:         backend.GetStringFromPtr(input.ContentLanguage),
		CacheControl:            backend.GetStringFromPtr(input.CacheControl),
		Expires:                 backend.GetStringFromPtr(input.Expires),
		WebsiteRedirectLocation: backend.GetStringFromPtr(input.WebsiteRedirectLocation),
		Metadata:                input.Metadata,
	}
	node, effState, err := b.commitVersion(ctx, bucketState, key, mf, initState, func(superseded *msbucket.ObjectManifest) error {
		// Race-safe re-check of If-Match / If-None-Match under the lock. A
		// delete-marker current means "no object" for precondition purposes.
		oldETag, oldExists := "", false
		if superseded != nil && !superseded.DeleteMarker {
			oldETag, oldExists = etagOf(superseded), true
		}
		return backend.EvaluateObjectPutPreconditions(oldETag, input.IfMatch, input.IfNoneMatch, oldExists)
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	size := mf.Body.Size
	out := s3response.PutObjectOutput{
		ETag: etagOf(mf),
		Size: &size,
	}
	if effState.Configured() {
		out.VersionID = node.VersionID
	}
	out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(ckAlgo, ckVal, mf.ChecksumType)
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
func (b *Backend) ingestBody(ctx context.Context, bucket *registry.State, r io.Reader) (msbucket.Body, error) {
	body, err := b.splitSpool(ctx, bucket.Name, bucket.Space, r)
	if err != nil {
		return msbucket.Body{}, err
	}
	if err := b.uploadBlobs(ctx, bucket.Space, body.Blobs); err != nil {
		return msbucket.Body{}, err
	}
	return body, nil
}

// splitSpool coarse-splits a body into blobs, encrypts each into a FEE
// envelope (recipient: the tenant's wrap key) written to the local spool
// under its ciphertext digest, and records a spooled upload_intents row plus
// a blob_encryption_params row per blob — WITHOUT uploading. It is the
// shared first half of ingest: a single-shot PUT follows it with uploadBlobs
// immediately; a multipart UploadPart spools here and defers the upload to
// Complete.
//
// The Body it returns is entirely plaintext-coordinate (Size, spans,
// SHA256/MD5 — all computed before encryption); the intents record the
// SPOOLED (ciphertext) byte count, which is what the uploader ships.
func (b *Backend) splitSpool(ctx context.Context, bucket string, space did.DID, r io.Reader) (msbucket.Body, error) {
	if r == nil {
		r = bytes.NewReader(nil)
	}
	// One tenant recipient per request, resolved before anything is spooled:
	// a body that cannot be wrapped to its tenant is not stored at all.
	recipient, err := b.tenantRecipient(ctx)
	if err != nil {
		return msbucket.Body{}, err
	}
	enc := newEncryptingBlobWriter(b.spool, b.regionKeys, space, []fee.Recipient{recipient})
	body, err := msbucket.SplitBody(ctx, enc, r, b.maxBlobSize)
	if err != nil {
		return msbucket.Body{}, fmt.Errorf("split body: %w", err)
	}
	for _, blob := range body.Blobs {
		storedSize, err := enc.storedSize(blob.Digest)
		if err != nil {
			return msbucket.Body{}, err
		}
		if err := b.intents.PutIntent(ctx, registry.UploadIntent{
			Digest:    blob.Digest,
			LocalPath: b.spool.Path(blob.Digest),
			Size:      storedSize,
			State:     registry.IntentSpooled,
			Bucket:    bucket,
		}); err != nil {
			return msbucket.Body{}, fmt.Errorf("record intent: %w", err)
		}
		// The read path decrypts from this row; it must exist before any
		// manifest referencing the blob can commit.
		params, err := enc.params(space, blob.Digest)
		if err != nil {
			return msbucket.Body{}, err
		}
		if err := b.encParams.PutEncryptionParams(ctx, params); err != nil {
			return msbucket.Body{}, fmt.Errorf("record encryption params: %w", err)
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
func (b *Backend) uploadBlobs(ctx context.Context, space did.DID, blobs []msbucket.BlobRef) error {
	for _, blob := range blobs {
		digest := blob.Digest
		if existing, err := b.locations.GetLocation(ctx, space, blob.Digest); err == nil && existing != nil {
			// Already durable for this space — advance the intent and move on.
			if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
				return fmt.Errorf("mark accepted (dedup): %w", err)
			}
			continue
		} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup location: %w", err)
		}
		// The uploaded bytes are the spooled envelope, so the size is the
		// intent's stored byte count, not the blob's plaintext span.
		in, err := b.intents.GetIntent(ctx, digest)
		if err != nil {
			return fmt.Errorf("lookup intent: %w", err)
		}
		res, err := b.uploader.UploadBlob(ctx, space, digest, in.Size, b.spool.Path(digest))
		if err != nil {
			return fmt.Errorf("upload blob: %w", err)
		}
		// A concluding UploadBlob (the default) returns an accepted location
		// or errors; guard the contract rather than deref-panic on a bad impl.
		if res.Location == nil {
			return fmt.Errorf("upload blob %x: concluding upload returned no location", blob.Digest)
		}
		loc := res.Location
		if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
			return fmt.Errorf("mark accepted: %w", err)
		}
		// Best-effort location record (unused in the harness, where reads come
		// from the spool); keyed by (space, digest).
		if err := b.locations.PutLocation(ctx, registry.BlobLocation{
			Space:    space,
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

// reconcileClaims updates blob_refs for ONE version id under (bucket, key)
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
// versionID names the claim rows: the version's ULID token, or "null" for a
// null version (unversioned buckets thus produce the same rows as before
// versioning). Callers releasing a DIFFERENT version than the one they created
// call this once per version id (docs/s3-versioning.md §8).
//
// It MUST run only after the catalog/root commit succeeds: it mutates blob_refs
// in its own (non-transactional) store, so applying it before a commit that then
// fails would diverge blob_refs from the committed catalog (and a later delete
// of a shared blob could drop a still-referenced one). Iterates over the
// DEDUPLICATED digest sets, so a manifest carrying the same digest in two blobs
// adds/deletes one claim and releases the blob at most once.
func (b *Backend) reconcileClaims(ctx context.Context, bucketState *registry.State, key, versionID string, oldDigests, newDigests []multihash.Multihash) (toRemove []multihash.Multihash, err error) {
	oldSet := digestSet(oldDigests)
	newSet := digestSet(newDigests)

	for k, d := range newSet {
		if _, ok := oldSet[k]; ok {
			continue // unchanged reference
		}
		if err := b.blobRefs.AddBlobClaim(ctx, registry.BlobClaim{
			Digest: d, Bucket: bucketState.Name, ObjectKey: key, VersionID: versionID, Space: bucketState.Space,
		}); err != nil {
			return nil, fmt.Errorf("add blob claim: %w", err)
		}
	}
	for k, d := range oldSet {
		if _, ok := newSet[k]; ok {
			continue // still referenced by the new body
		}
		if err := b.blobRefs.DeleteBlobClaim(ctx, d, bucketState.Name, key, versionID); err != nil {
			return nil, fmt.Errorf("delete blob claim: %w", err)
		}
		n, err := b.blobRefs.CountClaims(ctx, bucketState.Space, d)
		if err != nil {
			return nil, fmt.Errorf("count claims: %w", err)
		}
		if n == 0 {
			toRemove = append(toRemove, d)
		}
	}
	return toRemove, nil
}

// releaseBlobs runs for each digest whose last claim was dropped: it deletes
// the blob's encryption-params row (the crypto-shred — without the wrapped
// CEK the region can no longer decrypt the blob, per the encryption RFC's
// DELETE semantics), drops the location row, and calls RemoveBlob. Run after
// the commit lands, off the critical section — a 200 is not gated on the
// (currently no-op) network release. Failures are logged, not fatal: a
// missed release leaks bytes on Piri but never loses referenced data, and
// crash recovery reconciles upload_intents × blob_refs (a later phase).
func (b *Backend) releaseBlobs(ctx context.Context, space did.DID, digests []multihash.Multihash) {
	for _, d := range digests {
		if err := b.encParams.DeleteEncryptionParams(ctx, space, d); err != nil {
			b.logger.Warn("crypto-shred: delete encryption params failed",
				zap.String("digest", hex.EncodeToString(d)), zap.Error(err))
		}
		if err := b.locations.DeleteLocation(ctx, space, d); err != nil {
			b.logger.Warn("release: delete location failed",
				zap.String("digest", hex.EncodeToString(d)), zap.Error(err))
		}
		if err := b.remover.RemoveBlob(ctx, space, d); err != nil {
			// best-effort; see method doc.
			_ = err
		}
	}
}

// bodyDigests returns the digests of a body's blobs in order.
func bodyDigests(body msbucket.Body) []multihash.Multihash {
	out := make([]multihash.Multihash, 0, len(body.Blobs))
	for _, blob := range body.Blobs {
		out = append(out, blob.Digest)
	}
	return out
}

// digestSet maps each distinct digest (by its bytes) to one representative
// multihash, so callers can both test membership and recover the digest while
// processing every digest exactly once.
func digestSet(ds []multihash.Multihash) map[string]multihash.Multihash {
	s := make(map[string]multihash.Multihash, len(ds))
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

// HeadObject returns an object's metadata, honoring `?versionId`, the
// conditional-request preconditions, and the same byte-selection as GetObject
// (?partNumber=N or a Range header → a 206 with Content-Range, plus
// x-amz-mp-parts-count for a multipart part). Tagging is not implemented.
func (b *Backend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	versionID := backend.GetStringFromPtr(input.VersionId)
	rv, err := b.resolveVersion(ctx, *input.Bucket, *input.Key, versionID)
	if err != nil {
		return nil, err
	}
	mf := rv.mf
	if mf.DeleteMarker {
		// Current-is-marker reads 404; a version-scoped read of a marker is 405
		// (docs/s3-versioning.md §6.2). The populated output rides along with
		// the error so the controller emits x-amz-delete-marker and
		// Last-Modified on the error response (it dereferences LastModified —
		// keep it set).
		lm := time.Unix(mf.Created, 0)
		marker := true
		mout := &s3.HeadObjectOutput{DeleteMarker: &marker, LastModified: &lm}
		if versionID == "" {
			return mout, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return mout, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
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

	// x-amz-object-lock-* headers and the tag count of the resolved version
	// (docs/s3-object-lock.md §8; docs/s3-object-tagging.md §5).
	lockMode, lockUntil, lockHold, tagCount, err := b.stateHeaderFields(ctx, rv)
	if err != nil {
		return nil, err
	}

	contentType := mf.ContentType
	out := &s3.HeadObjectOutput{
		AcceptRanges:              backend.GetPtrFromString("bytes"),
		ContentLength:             &length,
		ContentType:               &contentType,
		ContentEncoding:           strPtrOrNil(mf.ContentEncoding),
		ContentDisposition:        strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:           strPtrOrNil(mf.ContentLanguage),
		CacheControl:              strPtrOrNil(mf.CacheControl),
		ExpiresString:             strPtrOrNil(mf.Expires),
		WebsiteRedirectLocation:   strPtrOrNil(mf.WebsiteRedirectLocation),
		Metadata:                  mf.Metadata,
		ContentRange:              contentRange,
		PartsCount:                partsCount,
		ETag:                      &etag,
		LastModified:              &lastModified,
		ObjectLockMode:            lockMode,
		ObjectLockRetainUntilDate: lockUntil,
		ObjectLockLegalHoldStatus: lockHold,
		TagCount:                  tagCount,
		StorageClass:              types.StorageClassStandard,
	}
	if rv.versioned() {
		out.VersionId = &rv.node.VersionID
	}
	// Echo the stored checksum only for a whole-object HEAD with checksum mode on
	// (a ranged HEAD's checksum would not match the full object).
	if input.ChecksumMode == types.ChecksumModeEnabled && !isRange {
		out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum, mf.ChecksumType)
	}
	return out, nil
}

// GetObject returns an object body — the current version or the one named by
// `?versionId` — optionally restricted to a byte range supplied via the Range
// header. The body io.ReadCloser is owned by the caller (versitygw closes it
// after streaming).
func (b *Backend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	versionID := backend.GetStringFromPtr(input.VersionId)
	rv, err := b.resolveVersion(ctx, *input.Bucket, *input.Key, versionID)
	if err != nil {
		return nil, err
	}
	mf, st := rv.mf, rv.st
	if mf.DeleteMarker {
		// Current-is-marker reads 404; a version-scoped read of a marker is 405
		// (docs/s3-versioning.md §6.2). The populated output rides along with
		// the error so the controller emits x-amz-delete-marker and
		// Last-Modified on the error response.
		lm := time.Unix(mf.Created, 0)
		marker := true
		mout := &s3.GetObjectOutput{DeleteMarker: &marker, LastModified: &lm}
		if versionID == "" {
			return mout, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return mout, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
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

	// Resolve how each blob's bytes become plaintext — the plain opener for
	// unencrypted blobs, the decrypting opener where an encryption row
	// exists. Doing it here (not at first Read) fails a broken encrypted
	// object as a request error, before response headers are written.
	opener, err := b.bodyOpener(ctx, st.Space, mf.Body)
	if err != nil {
		return nil, err
	}

	var contentRange *string
	var body = msbucket.OpenBody(ctx, opener, st.Space, mf.Body)
	if isRange {
		body = msbucket.OpenBodyRange(ctx, opener, st.Space, mf.Body, startOffset, startOffset+length-1)
		cr := fmt.Sprintf("bytes %d-%d/%d", startOffset, startOffset+length-1, objSize)
		contentRange = &cr
	}

	// x-amz-object-lock-* headers and the tag count of the resolved version
	// (docs/s3-object-lock.md §8; docs/s3-object-tagging.md §5).
	lockMode, lockUntil, lockHold, tagCount, err := b.stateHeaderFields(ctx, rv)
	if err != nil {
		return nil, err
	}

	etag := etagOf(mf)
	lastModified := time.Unix(mf.Created, 0)
	contentType := mf.ContentType
	out := &s3.GetObjectOutput{
		AcceptRanges:              backend.GetPtrFromString("bytes"),
		Body:                      body,
		ContentLength:             &length,
		ContentType:               &contentType,
		ContentEncoding:           strPtrOrNil(mf.ContentEncoding),
		ContentDisposition:        strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:           strPtrOrNil(mf.ContentLanguage),
		CacheControl:              strPtrOrNil(mf.CacheControl),
		ExpiresString:             strPtrOrNil(mf.Expires),
		WebsiteRedirectLocation:   strPtrOrNil(mf.WebsiteRedirectLocation),
		Metadata:                  mf.Metadata,
		ContentRange:              contentRange,
		PartsCount:                partsCount,
		ETag:                      &etag,
		LastModified:              &lastModified,
		ObjectLockMode:            lockMode,
		ObjectLockRetainUntilDate: lockUntil,
		ObjectLockLegalHoldStatus: lockHold,
		TagCount:                  tagCount,
		StorageClass:              types.StorageClassStandard,
	}
	if rv.versioned() {
		out.VersionId = &rv.node.VersionID
	}
	// Echo the stored checksum only for a whole-object GET with checksum mode on
	// (a ranged GET's checksum would not match the full object).
	if input.ChecksumMode == types.ChecksumModeEnabled && !isRange {
		out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum, mf.ChecksumType)
	}
	return out, nil
}

// DeleteObject deletes an object per the bucket's versioning state
// (docs/s3-versioning.md §7): with `?versionId` it permanently removes that one
// version; without it, an unversioned bucket takes the permanent-delete path
// (missing keys are idempotent no-ops), while a versioned bucket inserts a
// delete marker — a numbered one when Enabled, the null one when Suspended —
// even for a key that does not exist.
func (b *Backend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	// A missing bucket fails the whole request (matches S3).
	bucketState, err := b.reg.Get(ctx, *input.Bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, fmt.Errorf("s3frontend: delete object: %w", err)
	}
	key := *input.Key
	preconds := &backend.ObjectDeletePreconditions{
		IfMatch:            input.IfMatch,
		IfMatchLastModTime: input.IfMatchLastModifiedTime,
		IfMatchSize:        input.IfMatchSize,
	}
	versioned := bucketState.Versioning.Configured()

	// Version-scoped: permanently remove that one version.
	if versionID := backend.GetStringFromPtr(input.VersionId); versionID != "" {
		res, err := b.deleteVersionScoped(ctx, bucketState, key, versionID, preconds)
		if err != nil {
			return nil, err
		}
		out := &s3.DeleteObjectOutput{}
		// Gate on the state read under the commit lock, not the snapshot above.
		if res.versioning.Configured() {
			vid := versionID
			out.VersionId = &vid
			if res.wasMarker {
				marker := true
				out.DeleteMarker = &marker
			}
		}
		return out, nil
	}

	// Unversioned: permanent delete.
	if !versioned {
		if err := b.deleteObjectKey(ctx, bucketState, key, preconds); err != nil {
			return nil, err
		}
		return &s3.DeleteObjectOutput{}, nil
	}

	// Versioned: insert a delete marker via the write rule.
	node, err := b.insertDeleteMarker(ctx, bucketState, key, preconds)
	if err != nil {
		return nil, err
	}
	marker := true
	return &s3.DeleteObjectOutput{DeleteMarker: &marker, VersionId: &node.VersionID}, nil
}

// insertDeleteMarker writes a delete-marker version for key via the §5 write
// rule: a manifest with DeleteMarker set, a zero Body, and no claims. Under
// Enabled it is a numbered version; under Suspended it is the null version
// (replacing any existing null). S3 inserts a marker even when the key does
// not exist.
func (b *Backend) insertDeleteMarker(ctx context.Context, bucketState *registry.State, key string, preconds *backend.ObjectDeletePreconditions) (msbucket.VersionNode, error) {
	mf := &msbucket.ObjectManifest{
		Key:          key,
		Created:      time.Now().Unix(),
		DeleteMarker: true,
	}
	node, _, err := b.commitVersion(ctx, bucketState, key, mf, nil, func(superseded *msbucket.ObjectManifest) error {
		if preconds == nil || superseded == nil || superseded.DeleteMarker {
			return nil
		}
		return backend.EvaluateObjectDeletePreconditions(etagOf(superseded), time.Unix(superseded.Created, 0), superseded.Body.Size, *preconds)
	})
	return node, err
}

// deleteObjectKey permanently removes one key — the unversioned bucket's
// delete: it drops the key's value (its manifest; a leaf never occurs
// on a purely-unversioned bucket, but is handled for completeness) and
// releases the (single, null) version's body blobs through the reference
// index. Missing keys (and an empty bucket) are idempotent no-ops. preconds,
// when non-nil, gates the delete on If-Match / size / mod-time under the
// lock. Shared by DeleteObject and DeleteObjects.
func (b *Backend) deleteObjectKey(ctx context.Context, bucketState *registry.State, key string, preconds *backend.ObjectDeletePreconditions) error {
	var oldDigests []multihash.Multihash
	var oldVersionID string
	err := b.txns.WithTx(ctx, bucketState.Name, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		// Empty bucket: nothing to delete. Returning cid.Undef tells WithTx to
		// discard with no commit — the equivalent of "no-op success."
		if !tx.State().Root.Defined() {
			return cid.Undef, nil
		}
		t := tx.LoadTree()

		// Load the value + manifest being removed so the body blobs can be
		// released through the reference index.
		valCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, nil // idempotent DELETE: missing key isn't an error
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		var val msbucket.ObjectValue
		if err := tx.Get(ctx, tx.State().Space, valCid, &val); err != nil {
			return cid.Undef, fmt.Errorf("load value: %w", err)
		}
		oldMf := val.Manifest
		if val.Leaf != nil {
			var em msbucket.EnvelopedManifest
			if err := tx.Get(ctx, tx.State().Space, val.Leaf.Current.Manifest, &em); err != nil {
				return cid.Undef, fmt.Errorf("load manifest: %w", err)
			}
			oldMf = em.Manifest
		}

		// Preconditions (If-Match / size / mod-time) under the lock against the
		// version being removed.
		if preconds != nil {
			if err := backend.EvaluateObjectDeletePreconditions(etagOf(oldMf), time.Unix(oldMf.Created, 0), oldMf.Body.Size, *preconds); err != nil {
				return cid.Undef, err
			}
		}

		t2, err := t.Delete(ctx, key)
		if err != nil {
			return cid.Undef, fmt.Errorf("mst delete: %w", err)
		}
		// A manifest-valued key's block is the manifest itself — one candidate
		// covers it; a leaf key contributes the leaf block and its current
		// manifest.
		if val.Leaf != nil {
			if err := b.gc.AddGCCandidate(ctx, val.Leaf.Current.Manifest.Bytes(), bucketState.Name); err != nil {
				return cid.Undef, fmt.Errorf("gc candidate: %w", err)
			}
		}
		if err := b.gc.AddGCCandidate(ctx, valCid.Bytes(), bucketState.Name); err != nil {
			return cid.Undef, fmt.Errorf("gc candidate: %w", err)
		}
		oldDigests = bodyDigests(oldMf.Body)
		oldVersionID = oldMf.VersionID
		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return mapCommitError(err, "delete")
	}
	// Release the removed version's blobs through the reference index AFTER the
	// commit is durable (so a commit failure can't diverge blob_refs). When the
	// key was absent, oldDigests is nil and this is a no-op.
	if oldVersionID == "" {
		oldVersionID = registry.NullVersionID
	}
	toRemove, err := b.reconcileClaims(ctx, bucketState, key, oldVersionID, oldDigests, nil)
	if err != nil {
		return fmt.Errorf("s3frontend: delete reconcile: %w", err)
	}
	b.releaseBlobs(ctx, bucketState.Space, toRemove)
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
	bucketState, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.DeleteResult{}, fmt.Errorf("s3frontend: delete objects: %w", err)
	}

	versioned := bucketState.Versioning.Configured()
	quiet := input.Delete.Quiet != nil && *input.Delete.Quiet
	var res s3response.DeleteResult
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		versionID := backend.GetStringFromPtr(obj.VersionId)
		entry := types.DeletedObject{Key: &key}
		var derr error
		switch {
		case versionID != "":
			// Version-scoped: permanently remove that one version. DeleteMarker
			// is set explicitly either way — upstream conformance expects an
			// explicit false for a non-marker version delete.
			var sres scopedDeleteResult
			sres, derr = b.deleteVersionScoped(ctx, bucketState, key, versionID, nil)
			if derr == nil && sres.versioning.Configured() {
				vid := versionID
				entry.VersionId = &vid
				dm := sres.wasMarker
				entry.DeleteMarker = &dm
				if sres.wasMarker {
					entry.DeleteMarkerVersionId = &vid
				}
			}
		case versioned:
			// Insert a delete marker via the write rule.
			var node msbucket.VersionNode
			node, derr = b.insertDeleteMarker(ctx, bucketState, key, nil)
			if derr == nil {
				marker := true
				entry.DeleteMarker = &marker
				entry.DeleteMarkerVersionId = &node.VersionID
			}
		default:
			derr = b.deleteObjectKey(ctx, bucketState, key, nil)
		}
		if derr != nil {
			k := key
			code, msg := deleteErrorFields(derr)
			e := types.Error{Key: &k, Code: &code, Message: &msg}
			if versionID != "" {
				vid := versionID
				e.VersionId = &vid
			}
			res.Error = append(res.Error, e)
			continue
		}
		if !quiet {
			res.Deleted = append(res.Deleted, entry)
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

	t := mst.LoadMST(b.read, st.Space, st.Root)
	seenPrefix := map[string]struct{}{}
	walkErr := t.WalkLeavesFromNocache(ctx, from, func(k string, valCid cid.Cid) error {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			return mst.ErrStopWalk
		}

		// Resolve the key's current version first: a key whose current version
		// is a delete marker is invisible to ListObjects — it produces neither
		// a Contents entry nor a CommonPrefix (docs/s3-versioning.md §9.1).
		// A manifest-valued key's block is the manifest itself (§2.1); a leaf key
		// costs one more fetch.
		var val msbucket.ObjectValue
		if err := b.read.Get(ctx, st.Space, valCid, &val); err != nil {
			return fmt.Errorf("value get %s: %w", valCid, err)
		}
		mfp := val.Manifest
		if val.Leaf != nil {
			var em msbucket.EnvelopedManifest
			if err := b.read.Get(ctx, st.Space, val.Leaf.Current.Manifest, &em); err != nil {
				return fmt.Errorf("manifest get %s: %w", val.Leaf.Current.Manifest, err)
			}
			mfp = em.Manifest
		}
		mf := *mfp
		if mf.DeleteMarker {
			return nil
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
