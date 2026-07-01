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

	// INGEST (no lock): stream the body → coarse-split into blobs → spool each
	// to local disk → upload each to Forge by digest (allocate→PUT→accept). A
	// 200 means every body blob is durable and accepted before the manifest
	// that references it is committed (docs/architecture.md §7.1).
	bodyRec, err := b.ingestBody(ctx, bucketName, input.Body)
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put ingest: %w", err)
	}

	// mf is captured by the closure and read after WithTx commits, so the
	// response ETag/size come from the same manifest that was committed.
	var mf *msbucket.ObjectManifest

	// COMMIT (short per-bucket critical section): write the manifest + MST
	// splice + guarded root swap. No large-body work happens under the lock.
	err = b.txns.WithTx(ctx, bucketName, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		mf = &msbucket.ObjectManifest{
			Key:                key,
			ContentType:        contentType,
			Created:            time.Now().Unix(),
			Body:               bodyRec,
			ETag:               hex.EncodeToString(bodyRec.MD5),
			ContentEncoding:    backend.GetStringFromPtr(input.ContentEncoding),
			ContentDisposition: backend.GetStringFromPtr(input.ContentDisposition),
			ContentLanguage:    backend.GetStringFromPtr(input.ContentLanguage),
			CacheControl:       backend.GetStringFromPtr(input.CacheControl),
			Metadata:           input.Metadata,
		}
		mfCid, err := tx.Put(ctx, mf)
		if err != nil {
			return cid.Undef, fmt.Errorf("manifest put: %w", err)
		}

		t := tx.LoadTree()
		t2, err := t.Add(ctx, key, mfCid, -1)
		if errors.Is(err, mst.ErrAlreadyExists) {
			// Unversioned overwrite-in-place. Phase 4 drives the superseded
			// body's digests through the reference index here (blob_refs -=,
			// remove(digest) at zero claims); for now it just re-points the leaf.
			t2, err = t.Update(ctx, key, mfCid)
		}
		if err != nil {
			return cid.Undef, fmt.Errorf("mst write: %w", err)
		}

		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		if errors.Is(err, bucketop.ErrBucketNotFound) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("s3frontend: put: %w", err)
	}

	size := mf.Body.Size
	return s3response.PutObjectOutput{
		ETag: etagOf(mf),
		Size: &size,
	}, nil
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
	if r == nil {
		r = bytes.NewReader(nil)
	}
	body, err := msbucket.SplitBody(ctx, b.spool, r, b.maxBlobSize)
	if err != nil {
		return msbucket.Body{}, fmt.Errorf("split body: %w", err)
	}
	for _, blob := range body.Blobs {
		digest := multihash.Multihash(blob.Digest)
		path := b.spool.Path(digest)
		if err := b.intents.PutIntent(ctx, registry.UploadIntent{
			Digest:    blob.Digest,
			LocalPath: path,
			Size:      blob.Length,
			State:     registry.IntentSpooled,
			Bucket:    bucket,
		}); err != nil {
			return msbucket.Body{}, fmt.Errorf("record intent: %w", err)
		}
		loc, err := b.uploader.UploadBlob(ctx, digest, blob.Length, path)
		if err != nil {
			return msbucket.Body{}, fmt.Errorf("upload blob: %w", err)
		}
		if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
			return msbucket.Body{}, fmt.Errorf("mark accepted: %w", err)
		}
		// Record where the accepted blob can be retrieved from, keyed by
		// (space, digest). A later read resolves it from this table (in the
		// harness the uploader is a no-op and reads come from the spool, so the
		// recorded URL is empty and unused). Best-effort: a failure here does
		// not fail the PUT — the spool still serves read-after-write.
		if err := b.locations.PutLocation(ctx, registry.BlobLocation{
			Space:    b.space,
			Digest:   blob.Digest,
			Provider: loc.Provider,
			URL:      loc.URL,
			Size:     loc.Size,
		}); err != nil {
			return msbucket.Body{}, fmt.Errorf("record location: %w", err)
		}
	}
	return body, nil
}

// HeadObject returns the manifest's metadata. Range, partNumber,
// preconditions, versioning, and checksums are not implemented.
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
	etag := etagOf(mf)
	size := mf.Body.Size
	lastModified := time.Unix(mf.Created, 0)
	contentType := mf.ContentType
	return &s3.HeadObjectOutput{
		AcceptRanges:       backend.GetPtrFromString("bytes"),
		ContentLength:      &size,
		ContentType:        &contentType,
		ContentEncoding:    strPtrOrNil(mf.ContentEncoding),
		ContentDisposition: strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:    strPtrOrNil(mf.ContentLanguage),
		CacheControl:       strPtrOrNil(mf.CacheControl),
		Metadata:           mf.Metadata,
		ETag:               &etag,
		LastModified:       &lastModified,
		StorageClass:       types.StorageClassStandard,
	}, nil
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

	objSize := mf.Body.Size
	startOffset, length, isRange, err := backend.ParseObjectRange(objSize, backend.GetStringFromPtr(input.Range))
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
	return &s3.GetObjectOutput{
		AcceptRanges:       backend.GetPtrFromString("bytes"),
		Body:               body,
		ContentLength:      &length,
		ContentType:        &contentType,
		ContentEncoding:    strPtrOrNil(mf.ContentEncoding),
		ContentDisposition: strPtrOrNil(mf.ContentDisposition),
		ContentLanguage:    strPtrOrNil(mf.ContentLanguage),
		CacheControl:       strPtrOrNil(mf.CacheControl),
		Metadata:           mf.Metadata,
		ContentRange:       contentRange,
		ETag:               &etag,
		LastModified:       &lastModified,
		StorageClass:       types.StorageClassStandard,
	}, nil
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
	bucketName := *input.Bucket
	key := *input.Key

	err := b.txns.WithTx(ctx, bucketName, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		// Empty bucket: nothing to delete. Returning cid.Undef from
		// the closure tells WithTx to discard cleanly with no
		// commit — the equivalent of "no-op success."
		if !tx.State().Root.Defined() {
			return cid.Undef, nil
		}
		t := tx.LoadTree()
		t2, err := t.Delete(ctx, key)
		if errors.Is(err, mst.ErrNotFound) {
			// Idempotent DELETE: missing key isn't an error.
			return cid.Undef, nil
		}
		if err != nil {
			return cid.Undef, fmt.Errorf("mst delete: %w", err)
		}
		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		if errors.Is(err, bucketop.ErrBucketNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, fmt.Errorf("s3frontend: delete: %w", err)
	}
	return &s3.DeleteObjectOutput{}, nil
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
