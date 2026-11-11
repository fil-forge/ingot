package s3frontend

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/google/uuid"

	"github.com/fil-forge/ingot/blake3tree"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// defaultMaxListing is the S3 default and cap for max-parts / max-uploads.
const defaultMaxListing = 1000

// newUploadID returns a random 128-bit hex upload id.
func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// CreateMultipartUpload opens a multipart session: it records the destination
// bucket/key plus the content-type, the passthrough HTTP metadata headers, and
// user metadata so Complete can write the manifest without the client
// resupplying them, and returns the upload id.
func (b *Backend) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket, key := *input.Bucket, *input.Key
	if err := objectKeyError(key); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	if unsupportedObjectACL(input.ACL, input.GrantFullControl, input.GrantRead, input.GrantReadACP, input.GrantWriteACP) {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	sse, err := requestedEncryption(ctx)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	// A directory object (trailing "/") is zero-length by definition; a
	// multipart upload to one necessarily carries data.
	if strings.HasSuffix(key, "/") {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrDirectoryObjectContainsData)
	}
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("s3frontend: create mpu: %w", err)
	}
	// x-amz-object-lock-* headers are validated here (the MPU variant of the
	// missing-configuration error) and carried on the session; Complete stamps
	// them onto the version it commits (docs/s3-object-lock.md §7). An absent
	// retain-until header arrives as a pointer to the zero time (the
	// controller populates the pointer unconditionally); keep it out of the
	// session row.
	if _, err := lockStateFromHeaders(st, input.ObjectLockMode, input.ObjectLockRetainUntilDate, input.ObjectLockLegalHoldStatus); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	// The x-amz-tagging header is validated at create (an invalid header
	// fails the create) and carried raw on the session; Complete stamps the
	// parsed set (docs/s3-object-tagging.md §4).
	if _, err := backend.ParseObjectTags(backend.GetStringFromPtr(input.Tagging)); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	lockUntil := input.ObjectLockRetainUntilDate
	if lockUntil != nil && lockUntil.IsZero() {
		lockUntil = nil
	}
	uploadID, err := newUploadID()
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	ct := backend.GetStringFromPtr(input.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	// Whether the object is stored as received is the session's to carry:
	// decided here, as on S3, and applied to every part (sse.go).
	plaintext := storesPlaintext(st, sse)
	// The upload's queue-multipart-upload.create records it once its bucket
	// admits it, and schedules its auto-abort.
	if err := b.uploads.Create(ctx, &registry.MultipartSession{
		UploadID:                uploadID,
		Bucket:                  bucket,
		ObjectKey:               key,
		Space:                   st.Space,
		State:                   registry.SessionOpen,
		ContentType:             ct,
		ContentEncoding:         normalizeContentEncoding(backend.GetStringFromPtr(input.ContentEncoding)),
		ContentDisposition:      backend.GetStringFromPtr(input.ContentDisposition),
		ContentLanguage:         backend.GetStringFromPtr(input.ContentLanguage),
		CacheControl:            backend.GetStringFromPtr(input.CacheControl),
		Expires:                 backend.GetStringFromPtr(input.Expires),
		WebsiteRedirectLocation: backend.GetStringFromPtr(input.WebsiteRedirectLocation),
		ChecksumAlgorithm:       string(input.ChecksumAlgorithm),
		ChecksumType:            string(input.ChecksumType),
		LockMode:                string(input.ObjectLockMode),
		LockRetainUntil:         lockUntil,
		LockLegalHold:           string(input.ObjectLockLegalHoldStatus),
		Tagging:                 backend.GetStringFromPtr(input.Tagging),
		Metadata:                input.Metadata,
		Plaintext:               plaintext,
	}); err != nil {
		return s3response.InitiateMultipartUploadResult{}, mapUploadError(err, "create session")
	}
	return s3response.InitiateMultipartUploadResult{Bucket: bucket, Key: key, UploadId: uploadID, ServerSideEncryption: encryptionFor(plaintext)}, nil
}

// openSession fetches uploadID's session and maps anything that is not an
// in-flight upload for (bucket, key) to NoSuchUpload: unknown id, a bucket or
// key that doesn't match the session's, or a session no longer open (completed
// uploads are retained for Complete idempotency but are gone as far as the
// other multipart operations are concerned). Upload ids are global, so the
// bucket check is what keeps a request addressed to one bucket from acting on
// a session that belongs to another.
func (b *Backend) openSession(ctx context.Context, uploadID string, bucket, key *string) (*registry.MultipartSession, error) {
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return nil, fmt.Errorf("s3frontend: get session: %w", err)
	}
	if bucket != nil && *bucket != sess.Bucket {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if key != nil && *key != sess.ObjectKey {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if sess.State != registry.SessionOpen {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if bucket != nil {
		if err := b.checkSessionSpace(ctx, sess); err != nil {
			return nil, err
		}
	}
	return sess, nil
}

// checkSessionSpace rejects a session whose bucket has since been deleted and
// recreated under the same name: the parts were parked in the old bucket's
// space, and the current bucket's space is another. Such an upload is no
// upload of this bucket's; its auto-abort tears it down against its own space.
func (b *Backend) checkSessionSpace(ctx context.Context, sess *registry.MultipartSession) error {
	space, err := b.bucketSpace(ctx, sess.Bucket)
	if err != nil {
		return err
	}
	if space != sess.Space {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	return nil
}

// bucketSpace resolves the Forge space owning bucketName. Every network-side
// blob action is space-scoped (the space is the UCAN subject), so the
// multipart paths resolve it once per operation from the bucket registry.
func (b *Backend) bucketSpace(ctx context.Context, bucketName string) (did.DID, error) {
	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return did.Undef, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return did.Undef, fmt.Errorf("s3frontend: resolve bucket space: %w", err)
	}
	return st.Space, nil
}

// UploadPart ingests one part: it coarse-splits the part body into blobs,
// streams each to its provider (recording upload_intents), records the part
// (its ordered blob digests, md5, size, checksum), and parks each blob on its
// provider — PARKED, not accepted: the /http/put conclude that triggers
// /blob/accept is deferred to Complete, so the bytes are durable but stay
// out of the PDP pipeline, and an Abort unwinds them with /blob/abort
// (§7.2). Re-uploading a part number supersedes the prior part; the
// superseded part's now-unreferenced blobs get a release queued. The part
// ETag is the hex md5 of the part bytes: a Content-MD5 the checksum
// middleware verified against the stream is reused, else it is computed
// during ingest.
//
// The part checksum follows the session's CreateMultipartUpload declaration:
// a declared algorithm is computed (and validated against a client-supplied
// value) on every part, persisted, and echoed; a session without one persists
// an internal full-object CRC64NVME — Complete's default final checksum —
// and echoes a client-requested algorithm without persisting it.
func (b *Backend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.PartNumber == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	sess, err := b.openSession(ctx, uploadID, input.Bucket, input.Key)
	if err != nil {
		return nil, err
	}

	partAlgo, expected := partChecksumFromInput(input)
	src := input.Body
	if src == nil {
		src = bytes.NewReader(nil)
	}
	var md5Src bodyMD5Source
	if digest := decodeContentMD5(contentMD5Header(ctx)); digest != nil {
		md5Src = knownMD5(digest)
	}
	rec, err := b.ingestPart(ctx, sess, int(*input.PartNumber), src, declaredLength(input.ContentLength), partAlgo, expected, md5Src)
	if err != nil {
		return nil, err
	}
	out := &s3.UploadPartOutput{ETag: &rec.etag, ServerSideEncryption: encryptionFor(sess.Plaintext)}
	setUploadPartChecksum(out, rec.echoAlgo, rec.echoSum)
	return out, nil
}

// ingestedPart is what ingestPart records and reports for one part: its quoted
// ETag (hex md5 of the part bytes) and the checksum to echo to the client, if
// any (the session's algorithm, or one the client asked for on this part).
type ingestedPart struct {
	etag     string
	size     int64
	echoAlgo types.ChecksumAlgorithm
	echoSum  string
}

// ingestPart is the body-source-agnostic core of UploadPart and UploadPartCopy:
// it negotiates the part checksum against the session, streams body through
// the checksum readers into splitBody, records the part (superseding a prior
// part of the same number), parks its blobs, and drops the superseded part's
// blobs. partAlgo/expected are the checksum the request names, if any: an
// explicit value is validated on the stream; an algorithm alone is computed.
// A non-nil md5Src is the part's MD5 the caller already holds (a verified
// Content-MD5, or a single-part copy source's digest); without one, a
// checksum reader that computes MD5 anyway (an MD5 session, or an MD5 part
// checksum on a session that declared none) stands in. Either way the MD5
// pass over the part is skipped and the part ETag / stored ETagMD5 come from
// that source.
//
// The checksum negotiation: a part checksum for an algorithm other than the
// session's declared one is rejected, and a COMPOSITE session requires one on
// every part (the composite final checksum is derived from them). The reader
// stack: hr computes the persisted checksum (the declared algorithm, or the
// internal CRC64NVME); clientRdr additionally computes/validates a
// client-requested algorithm the session didn't declare (echoed, never
// persisted). A client-supplied value mismatch surfaces from the ingest read
// as a BadDigest API error.
//
// size is the part's declared length (see splitBody).
func (b *Backend) ingestPart(ctx context.Context, sess *registry.MultipartSession, partNumber int, body io.Reader, size int64, partAlgo types.ChecksumAlgorithm, expected string, md5Src bodyMD5Source) (*ingestedPart, error) {
	uploadID := sess.UploadID
	sessAlgo := types.ChecksumAlgorithm(sess.ChecksumAlgorithm)
	if sessAlgo != "" && partAlgo != "" && partAlgo != sessAlgo {
		return nil, s3err.GetChecksumTypeMismatchErr(sessAlgo, partAlgo)
	}
	if sess.ChecksumType == string(types.ChecksumTypeComposite) && partAlgo == "" {
		return nil, s3err.GetChecksumTypeMismatchErr(sessAlgo, types.ChecksumAlgorithm("null"))
	}

	var hr, clientRdr *utils.HashReader
	var err error
	switch {
	case sessAlgo != "":
		ht, err := hashTypeForAlgo(sessAlgo)
		if err != nil {
			return nil, err
		}
		if hr, err = utils.NewHashReader(body, expected, ht); err != nil {
			return nil, err
		}
	case partAlgo == "" || partAlgo == types.ChecksumAlgorithmCrc64nvme:
		if hr, err = utils.NewHashReader(body, expected, utils.HashTypeCRC64NVME); err != nil {
			return nil, err
		}
	default:
		if hr, err = utils.NewHashReader(body, "", utils.HashTypeCRC64NVME); err != nil {
			return nil, err
		}
		ht, err := hashTypeForAlgo(partAlgo)
		if err != nil {
			return nil, err
		}
		if clientRdr, err = utils.NewHashReader(hr, expected, ht); err != nil {
			return nil, err
		}
	}
	bodyReader := io.Reader(hr)
	if clientRdr != nil {
		bodyReader = clientRdr
	}
	if md5Src == nil {
		switch {
		case sessAlgo == types.ChecksumAlgorithmMd5:
			md5Src = checksumReaderMD5(hr)
		case clientRdr != nil && partAlgo == types.ChecksumAlgorithmMd5:
			md5Src = checksumReaderMD5(clientRdr)
		}
	}

	// The parts so far place this one in the object (assumedPartOffset).
	prior, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return nil, fmt.Errorf("s3frontend: list parts: %w", err)
	}
	space := sess.Space
	// The part's BLAKE3 tree, hashed at the offset the part is assumed to
	// sit at in the object (see assumedPartOffset), whole chunks only, with
	// the bytes either side of them kept raw; Complete merges the parts'
	// trees into the object's digest and blocks and hashes the chunks that
	// straddle part boundaries from those raw bytes. The split's own whole-body tree is
	// skipped: it would be the tree of the part alone, at offset 0.
	//
	// The blocks are recorded at the block size of a treeOffset+size body.
	// Complete uses the tree only when treeOffset proves to be the part's
	// true offset, and the object then holds at least those bytes, so its
	// block is no finer (blake3tree.ChunkLog is monotone). Sizing the blocks
	// by the part alone would record far more than the object needs: a
	// 5 GiB part has 10,240 blocks of 512 KiB, where a 10,000-part upload of
	// them has 2 GiB blocks.
	treeOffset := assumedPartOffset(prior, partNumber, size)
	tree, err := blake3tree.NewHasherAtChunkLog(treeOffset, blake3tree.ChunkLog(treeOffset+size))
	if err != nil {
		tree = nil
	} else {
		bodyReader = io.TeeReader(bodyReader, tree)
	}
	// A part keeps no local copy: nothing reads it before Complete, and
	// Complete's object reads go to the provider.
	written, err := b.splitBody(ctx, space, bodyReader, size, md5Src, sess.Plaintext, false, msbucket.WithoutTree())
	if err != nil {
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return nil, apiErr
		}
		return nil, fmt.Errorf("s3frontend: upload part ingest: %w", err)
	}
	defer written.done()
	// Park the part's blobs on their providers before returning 200 — the
	// part is durable on the network as soon as the client sees success.
	// Each blob's add records its park and takes the write's reference to
	// it, which the part row takes over.
	refs, err := b.parkBlobs(ctx, sess.Bucket, space, written)
	if err != nil {
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return nil, apiErr
		}
		return nil, fmt.Errorf("s3frontend: park part blobs: %w", err)
	}
	rec := written.Body
	part := registry.MultipartPart{
		UploadID:    uploadID,
		PartNumber:  partNumber,
		ETagMD5:     rec.MD5,
		Size:        rec.Size,
		Checksum:    hr.Sum(),
		BlobDigests: written.Blobs.DigestList(),
		SpanLengths: spanLengths(written.Blobs),
		State:       registry.PartParked,
		Refs:        refs,
	}
	var partTree *registry.PartTree
	if tree != nil {
		partTree = partTreeOf(tree.FinishRange())
	}
	// Recording the part replaces any prior row for the part number, and
	// queues the releases of the blobs that row named and this part does
	// not, in the same write: nothing can separate the two, and a
	// concurrent upload of the same part number that wrote its row after
	// the listing above is replaced and released like any other. A digest
	// another part of the session also names (parts stored as received
	// share the blobs of identical bytes) is recorded all the same: the
	// release is stale while that part references it, and the session's
	// teardown queues it again.
	// Skipping it would race a concurrent re-upload of the sibling, each
	// seeing the other's old part and neither queueing the release. The
	// same write queues the upload's accept.
	superseding, err := b.uploads.AddPart(ctx, part, partTree, b.releaseAuthority(ctx, space))
	if err != nil {
		// No part row points at the written blobs now, whatever went wrong:
		// the upload was completed or aborted after this one was admitted
		// (NoSuchUpload), a reference expired, or the write failed. The
		// write's references to them expire here, before the error, and
		// release the blobs nothing else references. Should the row in fact
		// have landed and only its result been lost, it took the references
		// over, and the upload's teardown releases its blobs.
		b.blobs.DropRefsNow(ctx, space, refs, written.Blobs.DigestList())
		return nil, mapUploadError(err, "record part")
	}
	b.blobs.ReleaseNow(ctx, space, superseding)
	out := &ingestedPart{etag: `"` + hex.EncodeToString(rec.MD5) + `"`, size: rec.Size}
	switch {
	case sessAlgo != "":
		out.echoAlgo, out.echoSum = sessAlgo, hr.Sum()
	case clientRdr != nil:
		out.echoAlgo, out.echoSum = partAlgo, clientRdr.Sum()
	case partAlgo != "":
		out.echoAlgo, out.echoSum = partAlgo, hr.Sum()
	}
	return out, nil
}

// CompleteMultipartUpload assembles the final object from the uploaded parts
// as the upload's queue-multipart-upload.complete: it validates the client's
// part list against the recorded parts, accepts every part's blobs on Forge,
// and commits a manifest whose Body is the ordered union of the parts' blobs.
// The object ETag is hex(md5(concat of part md5s)) + "-N". It runs on the
// upload's subject, so no part lands and no Abort or teardown takes the
// upload meanwhile.
//
// A successful Complete retains the session in state 'completed' (with its
// parts), so a duplicate Complete with an identical part list is idempotent
// per S3, until its queue-multipart-upload.reap.

func (b *Backend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	return b.CompleteMultipartUploadReportingHeaders(ctx, input, func(backend.CompleteMultipartUploadHeaders) {})
}

var _ backend.CompleteMultipartUploadReporter = (*Backend)(nil)

// CompleteMultipartUploadReportingHeaders is CompleteMultipartUpload that
// reports its version id and encryption before accepting the parts, the
// slow step, for versitygw to send with a streamed response's early
// headers, as S3 does. An Enabled bucket's winner reports the id it
// reserved; every other path reports "" before it waits on anything.
func (b *Backend) CompleteMultipartUploadReportingHeaders(ctx context.Context, input *s3.CompleteMultipartUploadInput, reportHeaders func(backend.CompleteMultipartUploadHeaders)) (s3response.CompleteMultipartUploadResult, string, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket, key := *input.Bucket, *input.Key
	req := multipartqueue.CompleteRequest{
		Bucket:       bucket,
		Key:          key,
		UploadID:     *input.UploadId,
		ChecksumType: input.ChecksumType,
		Checksums:    completeChecksums(input),
		IfMatch:      input.IfMatch,
		IfNoneMatch:  input.IfNoneMatch,
		ObjectSize:   input.MpuObjectSize,
		Report: func(versionID string, plaintext bool) {
			reportHeaders(backend.CompleteMultipartUploadHeaders{VersionID: versionID, ServerSideEncryption: encryptionFor(plaintext)})
		},
	}
	if input.MultipartUpload != nil {
		req.Parts = input.MultipartUpload.Parts
	}
	res, err := b.uploads.Complete(ctx, req)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", mapUploadError(err, "complete multipart upload")
	}
	// The parts the Complete left out are released now rather than when a
	// worker reaches them.
	b.blobs.ReleaseNow(ctx, res.Space, res.Orphans)
	etag := `"` + res.ETag + `"`
	out := s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etag, ServerSideEncryption: encryptionFor(res.Plaintext)}
	setCompleteResultChecksum(&out, res.ChecksumAlgorithm, res.Checksum, res.ChecksumType)
	return out, res.VersionID, nil
}

// AbortMultipartUpload cancels a multipart upload through its
// queue-multipart-upload.abort, which drops it (cascading its parts) and
// queues a release for each of the parts' blobs (which unallocates any
// parked on a provider: an upload ends in exactly one of accept or abort).
// The abort runs on the upload's subject, so it never overlaps a Complete.
func (b *Backend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	if input.UploadId == nil {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	sess, err := b.openSession(ctx, uploadID, input.Bucket, input.Key)
	if err != nil {
		return err
	}
	// If-Match-Initiated-Time: reject when the provided timestamp predates the
	// upload's initiation (compared at second precision — Initiated is
	// presented via RFC3339); future timestamps are ignored per S3.
	if input.IfMatchInitiatedTime != nil &&
		input.IfMatchInitiatedTime.Truncate(time.Second).Before(sess.CreatedAt.Truncate(time.Second)) {
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	}
	return b.abortUpload(ctx, *sess)
}

// parkBlobs runs the queue-space-shard.add of each of a part's blobs, which
// records the park its stream left on its provider, and returns the
// references the adds took. A blob the space has parked or accepted already
// is reused, and its stream's allocation given up. Every part blob streams:
// a blob that did not (its PUT failed, or the upload service could not add it
// by digest code) fails the part, and the client uploads it again.
func (b *Backend) parkBlobs(ctx context.Context, bucket string, space did.DID, body writtenBody) ([]taskrunner.TaskID, error) {
	release := b.releaseAuthority(ctx, space)
	var refs []taskrunner.TaskID
	// Each digest once: a part stored as received can hold the same bytes
	// twice, and the provider needs them once.
	seen := make(map[string]bool, len(body.Blobs))
	for _, blob := range body.Blobs {
		k := string(blob.Digest)
		if seen[k] {
			continue
		}
		seen[k] = true
		sb, ok := body.streamed[k]
		if !ok {
			return nil, fmt.Errorf("part blob %x did not stream to its provider", blob.Digest)
		}
		ref, err := b.addBlob(ctx, space, bucket, blob.Digest, body.sizes[k], spaceshardqueue.Part, &sb, release)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// ListParts returns the recorded parts of an in-flight upload, paginated by
// part number. Per-part checksums are echoed only for a session that declared
// a checksum algorithm (the internal CRC64NVME of an undeclared session stays
// hidden); such a session reports the literal "null" algorithm and type.
func (b *Backend) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	sess, err := b.openSession(ctx, uploadID, input.Bucket, input.Key)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	sessAlgo := types.ChecksumAlgorithm(sess.ChecksumAlgorithm)

	marker := 0
	if input.PartNumberMarker != nil && *input.PartNumberMarker != "" {
		m, err := strconv.Atoi(*input.PartNumberMarker)
		if err != nil {
			return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
		}
		marker = m
	}
	maxParts := defaultMaxListing
	if input.MaxParts != nil && *input.MaxParts > 0 && int(*input.MaxParts) < defaultMaxListing {
		maxParts = int(*input.MaxParts)
	}

	stored, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return s3response.ListPartsResult{}, fmt.Errorf("s3frontend: list parts: %w", err)
	}
	var parts []s3response.Part
	truncated := false
	next := 0
	for _, p := range stored {
		if p.PartNumber <= marker {
			continue
		}
		if len(parts) == maxParts {
			truncated = true
			break
		}
		part := s3response.Part{
			PartNumber:   p.PartNumber,
			ETag:         `"` + hex.EncodeToString(p.ETagMD5) + `"`,
			Size:         p.Size,
			LastModified: p.CreatedAt.UTC(),
		}
		if sessAlgo != "" {
			setResponsePartChecksum(&part, sessAlgo, p.Checksum)
		}
		parts = append(parts, part)
		next = p.PartNumber
	}
	res := s3response.ListPartsResult{
		Bucket:            *input.Bucket,
		Key:               *input.Key,
		UploadID:          uploadID,
		StorageClass:      types.StorageClassStandard,
		PartNumberMarker:  marker,
		MaxParts:          maxParts,
		IsTruncated:       truncated,
		Parts:             parts,
		ChecksumAlgorithm: types.ChecksumAlgorithm("null"),
		ChecksumType:      types.ChecksumType("null"),
	}
	if sessAlgo != "" {
		res.ChecksumAlgorithm = sessAlgo
		res.ChecksumType = types.ChecksumType(sess.ChecksumType)
	}
	if truncated {
		res.NextPartNumberMarker = next
	}
	return res, nil
}

// ListMultipartUploads lists the bucket's in-flight (open) multipart uploads
// in (key, initiation) order, with S3's prefix/delimiter/marker pagination.
func (b *Backend) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	if input.Bucket == nil {
		return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	st, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.ListMultipartUploadsResult{}, fmt.Errorf("s3frontend: list mpu: %w", err)
	}

	prefix := backend.GetStringFromPtr(input.Prefix)
	delimiter := backend.GetStringFromPtr(input.Delimiter)
	keyMarker := backend.GetStringFromPtr(input.KeyMarker)
	uploadIDMarker := backend.GetStringFromPtr(input.UploadIdMarker)
	maxUploads := defaultMaxListing
	if input.MaxUploads != nil && *input.MaxUploads > 0 && int(*input.MaxUploads) < defaultMaxListing {
		maxUploads = int(*input.MaxUploads)
	}

	all, err := b.multipart.ListSessions(ctx, bucket, st.Space)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, fmt.Errorf("s3frontend: list sessions: %w", err)
	}
	// In-flight uploads only, honoring the prefix. The listing is of this
	// bucket's space, so a predecessor bucket's uploads are not in it.
	inflight := all[:0]
	for _, s := range all {
		if s.State == registry.SessionOpen && strings.HasPrefix(s.ObjectKey, prefix) {
			inflight = append(inflight, s)
		}
	}

	// Marker positioning, mirroring upstream's MultipartUploadLister: an
	// upload-id marker is meaningful only alongside a key marker; it must be
	// a valid UUID and must name an upload of the FIRST key group at or
	// after the key marker (else InvalidArgument), and the listing resumes
	// just past it.
	start := 0
	if keyMarker != "" {
		if uploadIDMarker != "" {
			if _, err := uuid.Parse(uploadIDMarker); err != nil {
				return s3response.ListMultipartUploadsResult{},
					s3err.GetInvalidArgumentErr(s3err.InvalidArgUploadIdMarker, uploadIDMarker)
			}
			i := 0
			for i < len(inflight) && inflight[i].ObjectKey < keyMarker {
				i++
			}
			pos := -1
			if i < len(inflight) {
				firstKey := inflight[i].ObjectKey
				for j := i; j < len(inflight) && inflight[j].ObjectKey == firstKey; j++ {
					if inflight[j].UploadID == uploadIDMarker {
						pos = j
						break
					}
				}
			}
			if pos < 0 {
				return s3response.ListMultipartUploadsResult{},
					s3err.GetInvalidArgumentErr(s3err.InvalidArgUploadIdMarker, uploadIDMarker)
			}
			start = pos + 1
		} else {
			for start < len(inflight) && inflight[start].ObjectKey <= keyMarker {
				start++
			}
		}
	}

	// Walk the ordered tail, collapsing keys under the delimiter into common
	// prefixes; uploads and prefixes count toward max-uploads together.
	var uploads []s3response.Upload
	var prefixes []s3response.CommonPrefix
	emittedPrefix := map[string]bool{}
	count := 0
	truncated := false
	nextKeyMarker, nextUploadIDMarker := "", ""
	for _, s := range inflight[start:] {
		itemKey := s.ObjectKey
		isPrefix := false
		if delimiter != "" {
			if i := strings.Index(s.ObjectKey[len(prefix):], delimiter); i >= 0 {
				itemKey = s.ObjectKey[:len(prefix)+i+len(delimiter)]
				isPrefix = true
			}
		}
		if isPrefix && emittedPrefix[itemKey] {
			continue
		}
		if count == maxUploads {
			truncated = true
			break
		}
		if isPrefix {
			emittedPrefix[itemKey] = true
			prefixes = append(prefixes, s3response.CommonPrefix{Prefix: itemKey})
		} else {
			uploads = append(uploads, s3response.Upload{
				Key:               s.ObjectKey,
				UploadID:          s.UploadID,
				StorageClass:      types.StorageClassStandard,
				Initiated:         s.CreatedAt.UTC(),
				ChecksumAlgorithm: types.ChecksumAlgorithm(s.ChecksumAlgorithm),
				ChecksumType:      types.ChecksumType(s.ChecksumType),
			})
		}
		count++
		nextKeyMarker, nextUploadIDMarker = itemKey, s.UploadID
	}
	res := s3response.ListMultipartUploadsResult{
		Bucket:         bucket,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		Delimiter:      delimiter,
		Prefix:         prefix,
		MaxUploads:     maxUploads,
		IsTruncated:    truncated,
		Uploads:        uploads,
		CommonPrefixes: prefixes,
	}
	if truncated {
		res.NextKeyMarker = nextKeyMarker
		res.NextUploadIDMarker = nextUploadIDMarker
	}
	return res, nil
}

// spanLengths returns the length of the span each of blobs covers, in order.
func spanLengths(blobs msbucket.BlobRefs) []int64 {
	out := make([]int64, 0, len(blobs))
	for _, blob := range blobs {
		out = append(out, blob.Len())
	}
	return out
}
