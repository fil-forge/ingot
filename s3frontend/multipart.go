package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/google/uuid"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blake3tree"
	msbucket "github.com/fil-forge/ingot/bucket"
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
	if err := b.createUpload(ctx, &registry.MultipartSession{
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
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return s3response.InitiateMultipartUploadResult{}, apiErr
		}
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("s3frontend: create session: %w", err)
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
	superseding, err := b.addPart(ctx, part, partTree, b.releaseAuthority(ctx, space))
	if err != nil {
		// No part row points at the written blobs now, whatever went wrong:
		// the upload was completed or aborted after this one was admitted
		// (NoSuchUpload), a reference expired, or the write failed. The
		// write's references to them expire here, before the error, and
		// release the blobs nothing else references. Should the row in fact
		// have landed and only its result been lost, it took the references
		// over, and the upload's teardown releases its blobs.
		b.blobs.DropRefsNow(ctx, space, refs, written.Blobs.DigestList())
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return nil, apiErr
		}
		if errors.Is(err, registry.ErrReferenceExpired) {
			// A reference to one of the part's blobs expired before its row
			// was recorded, and the blob may be released: the client sends
			// the part again, as it does an object write's (mapCommitError).
			return nil, s3err.GetAPIError(s3err.ErrSlowDown)
		}
		return nil, fmt.Errorf("s3frontend: record part: %w", err)
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

// maxBodyBlobs is the most blobs Complete assembles into one object
// (msbucket.MaxBodyBlobs); a variable so tests can lower it.
var maxBodyBlobs = msbucket.MaxBodyBlobs

// replayWaitBudget bounds how long a Complete waits for its upload's other
// changes, another Complete included, before giving up with
// errCompleteInProgress. Package-level so tests can shrink it.
var replayWaitBudget = 10 * time.Second

// errCompleteInProgress is S3's OperationAborted, a 409: another Complete of
// this upload is still running past the loser's wait budget, or the upload's
// accept did not start within its budget. The AWS CLI and other
// botocore-based clients treat it as final (botocore retries neither 409 nor
// OperationAborted), report the upload failed and abort it. Only a client
// that retries on its own gets the replay of the winner's result. (Not in the
// versitygw error table, hence built here.)
var errCompleteInProgress = s3err.APIError{
	Code:           "OperationAborted",
	Description:    "A conflicting conditional operation is currently in progress against this resource. Please try again.",
	HTTPStatusCode: http.StatusConflict,
}

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
	// A Complete waits for its upload's other changes, a Complete already
	// running included, within replayWaitBudget: a Complete that lost to
	// another then replays its result. Past it the answer is
	// OperationAborted, never NoSuchUpload. The budget is not time for the
	// other Complete to finish: one that waits silently longer than the
	// client's read timeout is cut off as the other was, so raising it
	// cannot help.
	call := &completeCall{input: input, report: reportHeaders}
	spec := uploadSpec(*input.UploadId, registry.MultipartCompleteAction, nil)
	expires := time.Now().Add(replayWaitBudget)
	spec.Expires = &expires
	_, err := b.uploads.Run(context.WithValue(ctx, completeKey{}, call), spec)
	if errors.Is(err, taskrunner.ErrExpired) {
		return s3response.CompleteMultipartUploadResult{}, "", errCompleteInProgress
	}
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	// The parts the Complete left out are released now rather than when a
	// worker reaches them.
	b.blobs.ReleaseNow(ctx, call.space, call.orphans)
	return call.res, call.versionID, nil
}

// completeCall is a CompleteMultipartUpload's request, for its
// queue-multipart-upload.complete, and what the Complete tells it back.
type completeCall struct {
	input  *s3.CompleteMultipartUploadInput
	report func(backend.CompleteMultipartUploadHeaders)

	res       s3response.CompleteMultipartUploadResult
	versionID string
	// space and orphans are the upload's space and the blobs of the parts the
	// Complete left out, whose releases it queued.
	space   did.DID
	orphans []mh.Multihash
	// reported is whether the Complete has reported its headers. It outlives
	// a run of the task's tree, because versitygw keeps only the first report
	// and a tree whose commit fails runs again from scratch.
	reported bool
	// reserved is the version the Complete reserved and reported, kept for
	// the same reason: a run again installs the id the client already has.
	reserved *versionReservation
}

// runComplete is queue-multipart-upload.complete (completeUpload).
func (b *Backend) runComplete(ctx context.Context, ex *taskrunner.Exec, _ taskrunner.Task) ([]byte, error) {
	call, _ := ctx.Value(completeKey{}).(*completeCall)
	if call == nil {
		return nil, taskrunner.Permanent(errors.New("complete: its request has gone"))
	}
	res, versionID, err := b.completeUpload(ctx, ex, call)
	if err != nil {
		return nil, err
	}
	call.res, call.versionID = res, versionID
	return nil, nil
}

// completeUpload assembles the object from the parts the request lists and
// commits it, on the upload's subject: no part lands and no Abort runs
// meanwhile. A completed upload replays the Complete that committed it.
func (b *Backend) completeUpload(ctx context.Context, ex *taskrunner.Exec, call *completeCall) (s3response.CompleteMultipartUploadResult, string, error) {
	input, reportHeaders := call.input, call.report
	// Only the first report counts (versitygw ignores the rest), and a
	// report is binding, so the reservation below needs to know whether ""
	// went out first. Every report comes after the session has loaded, and
	// carries its encryption.
	var sse types.ServerSideEncryption
	report := func(versionID string) {
		if !call.reported {
			call.reported = true
			reportHeaders(backend.CompleteMultipartUploadHeaders{VersionID: versionID, ServerSideEncryption: sse})
		}
	}
	bucket, key, uploadID := *input.Bucket, *input.Key, *input.UploadId
	bucketState, err := b.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: complete mpu: %w", err)
	}
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: complete: %w", err)
	}
	// Upload ids are global: the session must be this bucket and key's, or the
	// assembly below would commit another upload's parts under the request's
	// target. Unlike openSession this admits a completed session, which the
	// idempotent re-Complete replays.
	if sess.Bucket != bucket || sess.ObjectKey != key {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	// Nor a session of an earlier bucket by this name: its parts live in
	// that bucket's space, and this bucket's is another.
	if sess.Space.Defined() && sess.Space != bucketState.Space {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	sse = encryptionFor(sess.Plaintext)

	// A Complete naming a checksum type must match the CreateMultipartUpload
	// declaration (a session without one reports the literal "null").
	if input.ChecksumType != "" && types.ChecksumType(sess.ChecksumType) != input.ChecksumType {
		t := types.ChecksumType(sess.ChecksumType)
		if t == "" {
			t = types.ChecksumType("null")
		}
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetChecksumTypeMismatchOnMpErr(t)
	}

	// Conditional writes: S3 documents only If-None-Match: * (don't overwrite)
	// and If-Match (require current ETag) for Complete; a concrete If-None-Match
	// value — or combining If-None-Match with If-Match — is NotImplemented.
	ifMatch, ifNoneMatch := input.IfMatch, input.IfNoneMatch
	if ifNoneMatch != nil && (*ifNoneMatch != "*" || ifMatch != nil) {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	// Not for a completed upload, which replays its result: after its
	// commit, the precheck would newly fail an If-None-Match repeat.
	if sess.State != registry.SessionCompleted && (ifMatch != nil || ifNoneMatch != nil) {
		curETag, exists, lerr := b.currentObjectETag(ctx, bucket, key)
		if lerr != nil {
			return s3response.CompleteMultipartUploadResult{}, "", lerr
		}
		if ifMatch != nil {
			if !exists {
				return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			if !etagsEqual(*ifMatch, curETag) {
				return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrPreconditionFailed)
			}
		}
		if ifNoneMatch != nil && exists {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrPreconditionFailed)
		}
	}

	if input.MultipartUpload == nil || len(input.MultipartUpload.Parts) == 0 {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	// Without the trees: validation and the replay of a completed upload
	// need part numbers, sizes, ETags and checksums only, and the trees are
	// gone once an upload completes. They load once the parts are accepted.
	stored, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: list parts: %w", err)
	}
	byNum := make(map[int]registry.MultipartPart, len(stored))
	for _, p := range stored {
		byNum[p.PartNumber] = p
	}

	// The final checksum: the session's declared algorithm/type, or S3's
	// default full-object CRC64NVME (derived from the internal per-part
	// values) when none was declared. COMPOSITE folds the parts' raw digests
	// through a checksum-of-checksums; FULL_OBJECT combines the parts' CRCs
	// without re-reading bytes. missingStored disables checksum derivation
	// for sessions whose parts predate per-part checksum recording.
	sessAlgo := types.ChecksumAlgorithm(sess.ChecksumAlgorithm)
	mpHadChecksum := sessAlgo != ""
	ckAlgo, ckType := sessAlgo, types.ChecksumType(sess.ChecksumType)
	if !mpHadChecksum {
		ckAlgo, ckType = types.ChecksumAlgorithmCrc64nvme, types.ChecksumTypeFullObject
	}
	var compositeRdr *utils.CompositeChecksumReader
	if ckType == types.ChecksumTypeComposite {
		compositeRdr, err = utils.NewCompositeChecksumReader(utils.HashType(strings.ToLower(string(ckAlgo))))
		if err != nil {
			return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: composite checksum reader: %w", err)
		}
	}
	var composable string
	missingStored := false

	// Validate the requested parts (in-range and ascending, each matching a
	// recorded part by number + ETag + checksum) and compute the multipart
	// ETag while accumulating the final checksum.
	requested := make([]registry.MultipartPart, 0, len(input.MultipartUpload.Parts))
	etagHasher := md5.New()
	prev := 0
	for _, rp := range input.MultipartUpload.Parts {
		// A part entry missing either field is malformed XML; a part number
		// below 1 is an InvalidArgument (both per the upstream posix
		// backend). Out-of-range numbers fall through to the membership
		// check (no stored part can match) and report InvalidPart.
		if rp.PartNumber == nil || rp.ETag == nil {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		num := int(*rp.PartNumber)
		if num < 1 {
			return s3response.CompleteMultipartUploadResult{}, "",
				s3err.GetInvalidArgumentErr(s3err.InvalidArgCompleteMpPartNumber, strconv.Itoa(num))
		}
		if num <= prev {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prev = num
		sp, ok := byNum[num]
		if !ok {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if !etagsEqual(*rp.ETag, hex.EncodeToString(sp.ETagMD5)) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		stored := ""
		if mpHadChecksum {
			stored = sp.Checksum
		}
		if err := validatePartChecksum(sessAlgo, stored, rp, uploadID); err != nil {
			return s3response.CompleteMultipartUploadResult{}, "", err
		}
		requested = append(requested, sp)
		etagHasher.Write(sp.ETagMD5)
		// Accumulate from the STORED checksum (validation just proved any
		// client-supplied value equal to it, and it also covers the internal
		// CRC64NVME and the idempotent re-Complete).
		switch {
		case sp.Checksum == "":
			missingStored = true
		case compositeRdr != nil:
			if err := compositeRdr.Process(sp.Checksum); err != nil {
				return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: process part %d checksum: %w", num, err)
			}
		case len(requested) == 1:
			composable = sp.Checksum
		default:
			composable, err = utils.AddCRCChecksum(ckAlgo, composable, sp.Checksum, sp.Size)
			if err != nil {
				return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: combine part %d checksum: %w", num, err)
			}
		}
	}
	// Every part but the last must meet S3's protocol-level 5 MiB minimum
	// (backend.MinPartSize — an S3 constant clients and SDKs assume, not an
	// operator knob).
	var total int64
	trueOffsets := make([]int64, 0, len(requested))
	blobCount := 0
	for i, sp := range requested {
		if i < len(requested)-1 && sp.Size < backend.MinPartSize {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrEntityTooSmall)
		}
		trueOffsets = append(trueOffsets, total)
		total += sp.Size
		blobCount += len(sp.BlobDigests)
	}
	// A manifest holds at most maxBodyBlobs blobs. The default blob size never
	// reaches it; a small max_blob_size can, and the completion is refused
	// here. The parts' blobs may be accepted already; an abort releases them
	// as such.
	if blobCount > maxBodyBlobs {
		b.logger.Info("multipart completion refused: too many blobs for one object",
			zap.String("key", key), zap.Int("blobs", blobCount), zap.Int("max", maxBodyBlobs))
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrEntityTooLarge)
	}
	if input.MpuObjectSize != nil {
		if *input.MpuObjectSize < 0 {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetNegatvieMpObjectSizeErr(*input.MpuObjectSize)
		}
		if *input.MpuObjectSize != total {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetIncorrectMpObjectSizeErr(total, *input.MpuObjectSize)
		}
	}
	etag := hex.EncodeToString(etagHasher.Sum(nil)) + "-" + strconv.Itoa(len(requested))

	// Derive the final checksum and verify a client-supplied value against
	// it. A composite value may arrive bare (no "-N" part-count suffix); a
	// session that declared no checksum ignores any client value — the
	// derived CRC64NVME is authoritative.
	ckValue := ""
	if !missingStored {
		if compositeRdr != nil {
			ckValue = fmt.Sprintf("%s-%d", compositeRdr.Sum(), len(requested))
		} else {
			ckValue = composable
		}
		if mpHadChecksum {
			if got := finalChecksumFromInput(ckAlgo, input); got != "" {
				if ckType == types.ChecksumTypeComposite && !strings.Contains(got, "-") {
					got = fmt.Sprintf("%s-%d", got, len(requested))
				}
				if got != ckValue {
					return s3response.CompleteMultipartUploadResult{}, "", s3err.GetChecksumBadDigestErr(ckAlgo)
				}
			}
		}
	}

	// Idempotent re-Complete: the Complete that committed the upload
	// recorded its result on the session with its commit. The validation
	// above proved the caller's part list against the retained parts; guard
	// against a divergent list that happens to validate by comparing the
	// derived ETag with the committed one. The session's copy is
	// authoritative — the live key may since have been overwritten.
	if sess.State == registry.SessionCompleted {
		if !etagsEqual(sess.CommittedETag, etag) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		etagQ := `"` + sess.CommittedETag + `"`
		// A re-Complete of an already-completed upload returns the ETag
		// but no checksum: AWS omits it on the idempotent replay (for both
		// COMPOSITE and FULL_OBJECT), unlike the first Complete.
		res := s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etagQ, ServerSideEncryption: encryptionFor(sess.Plaintext)}
		// A null version is one the first Complete committed outside an
		// Enabled bucket, and sent no id for.
		versionid := sess.CommittedVersionID
		if versionid == registry.NullVersionID {
			versionid = ""
		}
		report(versionid)
		return res, versionid, nil
	}

	// The winner alone needs the parts' trees: the object's digest is
	// assembled from them, and a non-final part hashed at a wrong assumed
	// offset (variable sizes uploaded out of order) is read back from its
	// storage node first, within a budget. Over it the completion is refused
	// here and the upload stays open for a retry; parts the upload's accept
	// has accepted meanwhile stay accepted, and an abort releases them as
	// such. A replay never gets here, which matters because a
	// completed upload's trees are deleted (below) and would count as
	// re-hash in full.
	trees, err := b.multipart.ListPartTrees(ctx, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: list part trees: %w", err)
	}
	if need := rehashNeeded(requested, trees, trueOffsets); need > b.rehashBudget {
		b.logger.Info("multipart completion refused: re-hash over budget",
			zap.String("key", key), zap.Int("parts", len(requested)), zap.Int64("size", total), zap.Int64("rehash", need), zap.Int64("budget", b.rehashBudget))
		return s3response.CompleteMultipartUploadResult{}, "", errPartLayoutOverBudget(need, b.rehashBudget)
	}

	// An Enabled bucket's new version id goes out now, before the parts are
	// accepted: versitygw commits the response headers at the report, as S3
	// commits its early 200 with x-amz-version-id, and an error after it can
	// only go in the 200's body. The re-hash refusal above comes first so
	// that it keeps its 400. The commit then installs exactly this id, or
	// fails (errReservationSuperseded). Every other Complete reports no id,
	// and the commit then refuses to create a numbered version, which the
	// client could never learn.
	if call.reserved == nil {
		call.reserved = &versionReservation{}
		if bucketState.Versioning == registry.VersioningEnabled && !call.reported {
			r, err := b.reserveVersion(ctx, bucket)
			if err != nil {
				call.reserved = nil
				return s3response.CompleteMultipartUploadResult{}, "", err
			}
			call.reserved = &r
			report(r.VersionID)
		}
	}
	reserved := call.reserved
	report("")

	// Assemble the ordered body: each part's byte span (it may span several
	// blobs) is recorded so a later GET/HEAD ?partNumber=N can address it (§7.2).
	var blobs msbucket.BlobRefs
	var partSizes, partOffsets []int64
	// Per-part checksums are retained only for a COMPOSITE checksummed upload
	// (and only when every part recorded one). AWS exposes the per-part list and
	// a ?partNumber checksum solely for composite multipart objects; a
	// FULL_OBJECT upload reports only the whole-object checksum and part count.
	var partChecksums []string
	recordPartChecksums := mpHadChecksum && !missingStored && ckType == types.ChecksumTypeComposite
	for _, sp := range requested {
		partStart := blobs.Size()
		lengths, err := b.partSpanLengths(ctx, bucketState.Space, sess.Plaintext, sp)
		if err != nil {
			return s3response.CompleteMultipartUploadResult{}, "", err
		}
		for i, d := range sp.BlobDigests {
			blobs.Append(d, lengths[i])
		}
		partSizes = append(partSizes, blobs.Size()-partStart)
		partOffsets = append(partOffsets, partStart)
		if recordPartChecksums {
			partChecksums = append(partChecksums, sp.Checksum)
		}
	}

	// Every part's blobs must be accepted on Forge before the commit.
	if err := b.acceptRequested(ctx, uploadID, sess.Space, requested); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	// The object's BLAKE3 digest and blocks, assembled from the parts' trees,
	// re-reading a part only where its record cannot serve (bounded above,
	// before the parts were accepted).
	body := msbucket.Body{Size: blobs.Size(), PartSizes: partSizes, PartChecksums: partChecksums}
	tree, err := b.multipartTree(ctx, bucketState.Space, sess.Plaintext, requested, trees, partOffsets, blobs)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: object tree: %w", err)
	}
	if err := body.SetTree(tree); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: object tree: %w", err)
	}

	mf := &msbucket.ObjectManifest{
		Key:                     key,
		ContentType:             sess.ContentType,
		Created:                 time.Now().Unix(),
		Body:                    body,
		ETag:                    etag,
		ContentEncoding:         sess.ContentEncoding,
		ContentDisposition:      sess.ContentDisposition,
		ContentLanguage:         sess.ContentLanguage,
		CacheControl:            sess.CacheControl,
		Expires:                 sess.Expires,
		WebsiteRedirectLocation: sess.WebsiteRedirectLocation,
		Metadata:                sess.Metadata,
		Plaintext:               sess.Plaintext,
	}
	// Persist the final checksum so GET/HEAD/ListObjectVersions echo it —
	// including the derived default of a session that declared none.
	if ckValue != "" {
		mf.ChecksumAlgorithm, mf.Checksum, mf.ChecksumType = string(ckAlgo), ckValue, string(ckType)
	}

	// CreateMultipartUpload's lock headers, carried on the session, stamp the
	// version this commit installs — exactly as a single-shot PUT would have
	// (docs/s3-object-lock.md §7).
	initState, err := lockStateFromHeaders(bucketState, types.ObjectLockMode(sess.LockMode), sess.LockRetainUntil, types.ObjectLockLegalHoldStatus(sess.LockLegalHold))
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	tags, err := backend.ParseObjectTags(sess.Tagging)
	if err != nil {
		// Validated at CreateMultipartUpload; a parse failure here is a bug.
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: parse session tagging: %w", err)
	}
	initState = applyTagsIfPresent(initState, tags)

	// Commit through the §5 write rule (docs/s3-versioning.md), as a child:
	// seq allocation, supersession per the bucket's versioning state, and
	// the reference-index claims and drops with the root's advance, all
	// committed with this task. The commit re-checks the conditional-write
	// preconditions against the version it supersedes.
	put := &PutChange{Manifest: mf, HasBlobs: true, Blobs: blobs, InitState: initState, Reserved: &ReservedVersion{Seq: reserved.Seq, VersionID: reserved.VersionID}, Completes: true}
	if ifMatch != nil {
		put.HasIfMatch, put.IfMatch = true, *ifMatch
	}
	if ifNoneMatch != nil {
		put.HasIfNoneMatch, put.IfNoneMatch = true, *ifNoneMatch
	}
	node, effState, err := b.commitOneWith(ctx, ex, bucketState, KeyChange{Key: key, Put: put}, "commit version")
	if errors.Is(err, errReservationSuperseded) {
		b.logger.Info("multipart completion refused: the key or the bucket's versioning changed since it reported its version",
			zap.String("key", key), zap.String("uploadID", uploadID), zap.String("versionID", reserved.VersionID))
	}
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	// With the object: the session, recorded completed with the committed
	// ETag and version id, is retained with its parts so a repeated Complete
	// replays this result, until its reap. The parts' tree material is dead
	// now that the manifest holds the object's tree, and it is the bulk of a
	// large upload's rows (up to about 320 KiB per 5 GiB part).
	done := registry.CompletedSession(uploadID, etag, node.VersionID)
	if err := ex.WriteConditional(ctx, done.SQL, done.Args...); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	dropTrees := registry.DeletedPartTrees(uploadID)
	if err := ex.Write(ctx, dropTrees.SQL, dropTrees.Args...); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if ttl := b.sessionTTL(); ttl >= 0 {
		spec := uploadSpec(uploadID, registry.MultipartReapAction, nil)
		at := time.Now().Add(ttl)
		spec.NotBefore = &at
		if _, err := ex.Submit(ctx, spec); err != nil {
			return s3response.CompleteMultipartUploadResult{}, "", err
		}
	}

	// Release parts uploaded but omitted from the winning list: no claim was
	// ever added for them. The winners are the live set; the retained part
	// rows would otherwise mark every digest, orphans included, as live. The
	// releases are queued with the commit, so none is lost.
	winners := make(map[string]bool, len(blobs))
	for _, ref := range blobs {
		winners[string(ref.Digest)] = true
	}
	var orphans []mh.Multihash
	seen := map[string]bool{}
	for _, sp := range stored {
		for _, d := range sp.BlobDigests {
			if k := string(d); !winners[k] && !seen[k] {
				seen[k] = true
				orphans = append(orphans, d)
			}
		}
	}
	if err := spawnReleases(ctx, ex, bucketState.Space, orphans, b.releaseAuthority(ctx, bucketState.Space), registry.DueNow()); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	call.space, call.orphans = bucketState.Space, orphans

	// The x-amz-version-id of the new version. Only an enabled bucket echoes it;
	// a suspended bucket stores the object under the "null" version id but omits
	// it from the CompleteMultipartUpload response, as does an unversioned bucket
	// (docs/s3-versioning.md §4.3).
	versionid := ""
	if effState == registry.VersioningEnabled {
		versionid = node.VersionID
	}
	etagQ := `"` + etag + `"`
	res := s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etagQ, ServerSideEncryption: encryptionOf(mf)}
	setCompleteResultChecksum(&res, ckAlgo, ckValue, ckType)
	return res, versionid, nil
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

// sessionReleases returns the blobs of uploadID's parts that are still there
// to release, each once, for an open upload's abort to queue their releases,
// as every reference that goes does (see runRelease). The blob's upload
// intent says which those are. A published intent is a committed blob — the
// state its first reference claim wrote — so it may have been readable
// through an object: those are returned apart, as published, for releases
// after the reader grace. While the upload was open its parts were
// references, so an object's release that ran then found the blob referenced
// and did nothing, and this release is the one that frees it. No intent at
// all is a blob already released in full: every written blob gets an intent, and only a release's commit
// removes one. Releasing such a blob again would ask to remove a blob that
// may never have left this node, which nothing authorizes and no retry can
// complete. Whether a blob is in fact free to go is the release's question,
// answered when it runs: it does nothing for a digest anything in the space
// still references.
func (b *Backend) sessionReleases(ctx context.Context, space did.DID, uploadID string) (digests, published []mh.Multihash, err error) {
	parts, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return nil, nil, fmt.Errorf("list parts: %w", err)
	}
	seen := map[string]bool{}
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			if seen[string(d)] {
				continue
			}
			seen[string(d)] = true
			in, err := b.intents.GetIntent(ctx, space, d)
			switch {
			case errors.Is(err, registry.ErrNotFound):
				// Already released in full, intent and all.
				continue
			case err != nil:
				return nil, nil, fmt.Errorf("lookup intent: %w", err)
			case in.State == registry.IntentPublished:
				// Committed, so possibly read: released after the grace.
				published = append(published, d)
				continue
			}
			digests = append(digests, d)
		}
	}
	return digests, published, nil
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

// acceptWaitBudget bounds how long Complete waits for its parts' blobs to be
// accepted, behind accepts and releases already queued on them, under the
// 60 s read timeout clients such as the AWS CLI apply. Package-level so tests
// can shrink it.
var acceptWaitBudget = 30 * time.Second

// acceptWaits bounds how many blobs' subjects Complete waits on at once.
const acceptWaits = 16

// errAcceptWaitExpired is a wait for a blob's accept that ran out of
// acceptWaitBudget.
var errAcceptWaitExpired = errors.New("s3frontend: the parts' accepts did not finish in time")

// acceptRequested makes sure every part Complete names has its blobs
// accepted on Forge. The upload's accept has been queueing its parts' blobs
// for acceptance while the client uploaded them; Complete queues the accept
// of each blob of the parts it names that are still parked, after the
// response's headers have gone out, and then waits for each of those blobs'
// accepts, brought forward from any retry's backoff. It queues them and waits
// outside its own commit: the accepts' writes commit before Complete reads
// the parts again, and before the object's tree is assembled, which reads
// accepted blobs back through their locations.
// Parts the upload holds and Complete omits are left out: they are orphans of
// this completion. Then every named part must be accepted. One with a blob
// released since the request validated it is no longer the part the client
// named, and Complete answers InvalidPart; the client uploads it again. One
// whose accept failed fails the request, which clients retry. Accepts that do
// not finish within acceptWaitBudget answer OperationAborted
// (errCompleteInProgress, final for botocore clients) rather than hold the
// request past the client's read timeout.
func (b *Backend) acceptRequested(ctx context.Context, uploadID string, space did.DID, requested []registry.MultipartPart) error {
	expires := time.Now().Add(acceptWaitBudget)
	err := b.acceptAndWait(ctx, uploadID, space, requested, expires)
	if errors.Is(err, taskrunner.ErrExpired) || errors.Is(err, errAcceptWaitExpired) ||
		(errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		b.multipartMetrics.completeDeferred.Add(ctx, 1)
		b.logger.Info("multipart completion deferred: the parts' accepts did not finish in time",
			zap.String("upload_id", uploadID))
		return errCompleteInProgress
	} else if err != nil {
		return fmt.Errorf("s3frontend: accept parts: %w", err)
	}
	current, err := b.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("s3frontend: list parts: %w", err)
	}
	byNum := make(map[int]registry.MultipartPart, len(current))
	for _, p := range current {
		byNum[p.PartNumber] = p
	}
	for _, rp := range requested {
		cur, ok := byNum[rp.PartNumber]
		if !ok || !sameDigests(cur.BlobDigests, rp.BlobDigests) {
			return s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if cur.State == registry.PartAccepted || len(cur.BlobDigests) == 0 {
			continue
		}
		parked, err := b.anyParked(ctx, space, cur.BlobDigests)
		if err != nil {
			return err
		}
		if parked {
			return fmt.Errorf("s3frontend: part %d is not accepted yet; retry", cur.PartNumber)
		}
		return s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	return nil
}

// acceptAndWait queues the accept of each blob of the parts Complete names
// that are still parked, committed at once, and waits for them, every step
// bounded by expires.
func (b *Backend) acceptAndWait(ctx context.Context, uploadID string, space did.DID, requested []registry.MultipartPart, expires time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	var parked []registry.MultipartPart
	for _, p := range requested {
		if p.State == registry.PartParked {
			parked = append(parked, p)
		}
	}
	if _, err := b.uploads.SubmitAll(ctx, blobAccepts(space, parked)); err != nil {
		return err
	}
	return b.awaitBlobAccepts(ctx, space, requested, expires)
}

// awaitBlobAccepts waits, until expires, for the pending accepts of the
// blobs of parts in space, making each due now first: a sync no-op on a
// blob's subject runs once the tasks due before it there have run. The
// space's accepts are brought forward by their subjects in one statement,
// which also stops their being replaced, so one another upload submits
// meanwhile is added behind the no-op and leaves the accepts ahead of it
// where they are. Other uploads' accepts in the space are due now already,
// so bringing them forward too changes nothing for them.
func (b *Backend) awaitBlobAccepts(ctx context.Context, space did.DID, parts []registry.MultipartPart, expires time.Time) error {
	seen := map[string]bool{}
	var subjects []string
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			subject := registry.ReleaseSubject(space, d)
			if !seen[subject] {
				seen[subject] = true
				subjects = append(subjects, subject)
			}
		}
	}
	due, err := b.blobs.ExpediteAccepts(ctx, space)
	if err != nil {
		return fmt.Errorf("bring the blobs' accepts forward: %w", err)
	}
	var waiting []string
	for _, subject := range subjects {
		if len(due[subject]) > 0 {
			waiting = append(waiting, subject)
		}
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		slots = make(chan struct{}, acceptWaits)
	)
	for _, subject := range waiting {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-slots; wg.Done() }()
			err := b.blobs.Flush(ctx, subject, expires)
			if errors.Is(err, taskrunner.ErrExpired) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
				err = errAcceptWaitExpired
			}
			if err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}

// anyParked reports whether any of digests is still parked in space, its
// accept yet to succeed.
func (b *Backend) anyParked(ctx context.Context, space did.DID, digests []mh.Multihash) (bool, error) {
	for _, d := range digests {
		_, err := b.parks.GetPark(ctx, space, d)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, registry.ErrNotFound) {
			return false, fmt.Errorf("s3frontend: lookup park: %w", err)
		}
	}
	return false, nil
}

// partSpanLengths returns the length of the span each of p's blobs covers
// in the object, as recorded with the part. A part recorded before the
// lengths were kept takes them from its blobs' intents: the stored byte
// count is the span itself for a session stored as received, else the
// envelope's, from which the span follows by the blob's FEE geometry.
func (b *Backend) partSpanLengths(ctx context.Context, space did.DID, plaintext bool, p registry.MultipartPart) ([]int64, error) {
	if p.SpanLengths != nil {
		return p.SpanLengths, nil
	}
	sizes := make([]int64, len(p.BlobDigests))
	for i, d := range p.BlobDigests {
		in, err := b.intents.GetIntent(ctx, space, d)
		if err != nil {
			return nil, fmt.Errorf("s3frontend: part blob %x: %w", d, err)
		}
		sizes[i] = in.Size
		if !plaintext {
			if sizes[i], err = b.blobPlaintextLen(ctx, space, d, in.Size); err != nil {
				return nil, fmt.Errorf("s3frontend: part blob %x: %w", d, err)
			}
		}
	}
	return sizes, nil
}

// sameDigests reports whether a and b list the same digests in order.
func sameDigests(a, b []mh.Multihash) bool {
	return slices.EqualFunc(a, b, func(x, y mh.Multihash) bool { return bytes.Equal(x, y) })
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

// etagsEqual compares two ETags ignoring surrounding quotes.
func etagsEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}

// spanLengths returns the length of the span each of blobs covers, in order.
func spanLengths(blobs msbucket.BlobRefs) []int64 {
	out := make([]int64, 0, len(blobs))
	for _, blob := range blobs {
		out = append(out, blob.Len())
	}
	return out
}
