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
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
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
	if !mst.IsValidKey(key) {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
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
	if err := b.multipart.CreateSession(ctx, registry.MultipartSession{
		UploadID:                uploadID,
		Bucket:                  bucket,
		ObjectKey:               key,
		State:                   registry.SessionOpen,
		ContentType:             ct,
		ContentEncoding:         backend.GetStringFromPtr(input.ContentEncoding),
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
	}); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("s3frontend: create session: %w", err)
	}
	return s3response.InitiateMultipartUploadResult{Bucket: bucket, Key: key, UploadId: uploadID}, nil
}

// openSession fetches uploadID's session and maps anything that is not an
// in-flight upload for (bucket, key) to NoSuchUpload: unknown id, a key that
// doesn't match the session's, or a session no longer open (completed uploads
// are retained for Complete idempotency but are gone as far as the other
// multipart operations are concerned).
func (b *Backend) openSession(ctx context.Context, uploadID string, key *string) (*registry.MultipartSession, error) {
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return nil, fmt.Errorf("s3frontend: get session: %w", err)
	}
	if key != nil && *key != sess.ObjectKey {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if sess.State != registry.SessionOpen {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	return sess, nil
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
// spools each to local disk (recording upload_intents), records the part
// (its ordered blob digests, md5, size, checksum), and uploads each blob to
// its provider — PARKED, not accepted: the /http/put conclude that triggers
// /blob/accept is deferred to Complete, so the bytes are durable but stay
// out of the PDP pipeline, and an Abort unwinds them with /blob/abort
// (§7.2). Re-uploading a part number supersedes the prior part; the
// superseded part's now-unreferenced blobs are dropped from the spool and
// rejected. The part ETag is the hex md5 of the part bytes.
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
	sess, err := b.openSession(ctx, uploadID, input.Key)
	if err != nil {
		return nil, err
	}

	// Checksum negotiation against the session declaration. A part checksum
	// for a different algorithm than the declared one is rejected, and a
	// COMPOSITE session requires one on every part (the composite final
	// checksum is derived from them).
	partAlgo, expected := partChecksumFromInput(input)
	sessAlgo := types.ChecksumAlgorithm(sess.ChecksumAlgorithm)
	if sessAlgo != "" && partAlgo != "" && partAlgo != sessAlgo {
		return nil, s3err.GetChecksumTypeMismatchErr(sessAlgo, partAlgo)
	}
	if sess.ChecksumType == string(types.ChecksumTypeComposite) && partAlgo == "" {
		return nil, s3err.GetChecksumTypeMismatchErr(sessAlgo, types.ChecksumAlgorithm("null"))
	}

	// Reader stack over the body: hr computes the persisted checksum (the
	// declared algorithm, or the internal CRC64NVME), clientRdr additionally
	// computes/validates a client-requested algorithm the session didn't
	// declare (echoed, never persisted). A client-supplied value mismatch
	// surfaces from the ingest read as a BadDigest API error.
	src := input.Body
	if src == nil {
		src = bytes.NewReader(nil)
	}
	var hr, clientRdr *utils.HashReader
	switch {
	case sessAlgo != "":
		ht, err := hashTypeForAlgo(sessAlgo)
		if err != nil {
			return nil, err
		}
		if hr, err = utils.NewHashReader(src, expected, ht); err != nil {
			return nil, err
		}
	case partAlgo == "" || partAlgo == types.ChecksumAlgorithmCrc64nvme:
		if hr, err = utils.NewHashReader(src, expected, utils.HashTypeCRC64NVME); err != nil {
			return nil, err
		}
	default:
		if hr, err = utils.NewHashReader(src, "", utils.HashTypeCRC64NVME); err != nil {
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

	// Capture the superseded part's blobs (if any) before overwriting, so
	// last-write-wins doesn't strand its spool files.
	var superseded [][]byte
	if prior, err := b.multipart.ListParts(ctx, uploadID); err == nil {
		for _, p := range prior {
			if p.PartNumber == int(*input.PartNumber) {
				superseded = p.BlobDigests
				break
			}
		}
	}

	space, err := b.bucketSpace(ctx, sess.Bucket)
	if err != nil {
		return nil, err
	}
	body, err := b.splitSpool(ctx, sess.Bucket, bodyReader)
	if err != nil {
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			return nil, apiErr
		}
		return nil, fmt.Errorf("s3frontend: upload part ingest: %w", err)
	}
	if err := b.multipart.PutPart(ctx, registry.MultipartPart{
		UploadID:    uploadID,
		PartNumber:  int(*input.PartNumber),
		ETagMD5:     body.MD5,
		Size:        body.Size,
		Checksum:    hr.Sum(),
		BlobDigests: bodyDigests(body),
		State:       registry.PartParked,
	}); err != nil {
		return nil, fmt.Errorf("s3frontend: record part: %w", err)
	}
	// Park the part's blobs on their providers before returning 200 — the
	// part is durable on the network as soon as the client sees success.
	// The part row is recorded first so a crash mid-park leaves re-drivable
	// spooled intents.
	if err := b.parkBlobs(ctx, space, body.Blobs); err != nil {
		return nil, fmt.Errorf("s3frontend: park part blobs: %w", err)
	}
	if len(superseded) > 0 {
		b.cleanupPartBlobs(ctx, space, uploadID, superseded)
	}
	etag := `"` + hex.EncodeToString(body.MD5) + `"`
	out := &s3.UploadPartOutput{ETag: &etag}
	switch {
	case sessAlgo != "":
		setUploadPartChecksum(out, sessAlgo, hr.Sum())
	case clientRdr != nil:
		setUploadPartChecksum(out, partAlgo, clientRdr.Sum())
	case partAlgo != "":
		setUploadPartChecksum(out, partAlgo, hr.Sum())
	}
	return out, nil
}

// CompleteMultipartUpload assembles the final object from the uploaded parts:
// it latches the session (single-winner vs Abort), validates the client's part
// list against the recorded parts, accepts every part's blobs on Forge, and
// commits a manifest whose Body is the ordered union of the parts' blobs. The
// object ETag is hex(md5(concat of part md5s)) + "-N".
//
// A successful Complete retains the session in state 'completed' (with its
// parts), so a duplicate Complete with an identical part list is idempotent
// per S3; the abandoned-session sweeper reaps the row later.
func (b *Backend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
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
	if ifMatch != nil || ifNoneMatch != nil {
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
	for i, sp := range requested {
		if i < len(requested)-1 && sp.Size < backend.MinPartSize {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrEntityTooSmall)
		}
		total += sp.Size
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

	// Idempotent re-Complete: the prior Complete committed the object; the
	// validation above already proved the client's part list matches the
	// retained parts, so return the same result without recommitting.
	if sess.State == registry.SessionCompleted {
		etagQ := `"` + etag + `"`
		res := s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etagQ}
		setCompleteResultChecksum(&res, ckAlgo, ckValue, ckType)
		return res, "", nil
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

	// Assemble the ordered body: each part's byte span (it may span several
	// blobs) is recorded so a later GET/HEAD ?partNumber=N can address it (§7.2).
	var blobs []msbucket.BlobRef
	var partSizes []int64
	var offset int64
	for _, sp := range requested {
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

	// Accept every part's blobs on Forge: parked blobs conclude (the deferred
	// /http/put receipt fires /blob/accept), stragglers that never parked
	// (crash between spool and park) fall back to the whole synchronous
	// upload. Then commit.
	if err := b.concludeBlobs(ctx, bucketState.Space, blobs); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("s3frontend: accept parts: %w", err)
	}

	mf := &msbucket.ObjectManifest{
		Key:                     key,
		ContentType:             sess.ContentType,
		Created:                 time.Now().Unix(),
		Body:                    msbucket.Body{Size: offset, Blobs: blobs, PartSizes: partSizes},
		ETag:                    etag,
		ContentEncoding:         sess.ContentEncoding,
		ContentDisposition:      sess.ContentDisposition,
		ContentLanguage:         sess.ContentLanguage,
		CacheControl:            sess.CacheControl,
		Expires:                 sess.Expires,
		WebsiteRedirectLocation: sess.WebsiteRedirectLocation,
		Metadata:                sess.Metadata,
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

	// Commit through the §5 write rule (docs/s3-versioning.md): seq allocation,
	// supersession per the bucket's versioning state, and the post-commit
	// reference-index reconcile. The conditional-write preconditions re-check
	// under the lock so a racing writer can't slip between the pre-check above
	// and the swap.
	node, effState, err := b.commitVersion(ctx, bucketState, key, mf, initState, func(superseded *msbucket.ObjectManifest) error {
		if ifMatch == nil && ifNoneMatch == nil {
			return nil
		}
		// A delete-marker current means "no object" for precondition purposes.
		oldETag, oldExists := "", false
		if superseded != nil && !superseded.DeleteMarker {
			oldETag, oldExists = etagOf(superseded), true
		}
		if ifMatch != nil {
			if !oldExists {
				return s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			if !etagsEqual(*ifMatch, oldETag) {
				return s3err.GetAPIError(s3err.ErrPreconditionFailed)
			}
		}
		if ifNoneMatch != nil && oldExists {
			return s3err.GetAPIError(s3err.ErrPreconditionFailed)
		}
		return nil
	})
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	committed = true
	// The object is durable. Retain the session (state 'completed') and its
	// parts so a duplicate Complete is idempotent; the sweeper reaps it later.
	// Best-effort: a failed latch leaves the row in 'completing', which the
	// sweeper also treats as terminal after the TTL.
	if _, err := b.multipart.LatchSession(ctx, uploadID, registry.SessionCompleting, registry.SessionCompleted); err != nil {
		b.logger.Warn("latch session to completed failed; sweeper reaps the completing row after the TTL",
			zap.String("uploadID", uploadID), zap.Error(err))
	}

	// The x-amz-version-id of the new version, omitted for unversioned buckets
	// (docs/s3-versioning.md §4.3).
	versionid := ""
	if effState.Configured() {
		versionid = node.VersionID
	}
	etagQ := `"` + etag + `"`
	res := s3response.CompleteMultipartUploadResult{Bucket: &bucket, Key: &key, ETag: &etagQ}
	setCompleteResultChecksum(&res, ckAlgo, ckValue, ckType)
	return res, versionid, nil
}

// AbortMultipartUpload cancels a multipart upload: it latches the session
// (single-winner vs Complete), drops it (cascading its parts), and removes the
// parts' now-unreferenced blobs from the spool — unallocating any that were
// parked on a provider (an upload ends in exactly one of accept or
// abort). No reference claims were taken (those happen only at
// Complete).
func (b *Backend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	if input.UploadId == nil {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	uploadID := *input.UploadId
	sess, err := b.openSession(ctx, uploadID, input.Key)
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
	won, err := b.multipart.LatchSession(ctx, uploadID, registry.SessionOpen, registry.SessionAborting)
	if err != nil {
		return fmt.Errorf("s3frontend: latch: %w", err)
	}
	if !won {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	// Snapshot the parts' blob digests before the cascade delete, then drop
	// the session and clean the spool.
	var digests [][]byte
	if parts, err := b.multipart.ListParts(ctx, uploadID); err == nil {
		for _, p := range parts {
			digests = append(digests, p.BlobDigests...)
		}
	}
	if err := b.multipart.DeleteSession(ctx, uploadID); err != nil {
		return fmt.Errorf("s3frontend: delete session: %w", err)
	}
	space, err := b.bucketSpace(ctx, sess.Bucket)
	if err != nil {
		return err
	}
	b.cleanupPartBlobs(ctx, space, uploadID, digests)
	return nil
}

// abortOpenSession force-aborts an open multipart session exactly like a
// client Abort: latch (losing gracefully to a concurrent Complete/Abort),
// drop the session, release its parts' now-unreferenced blobs. Used by
// DeleteBucket's implicit abort of in-flight uploads.
func (b *Backend) abortOpenSession(ctx context.Context, space did.DID, sess registry.MultipartSession) {
	// s3:DeleteBucket delegates no blob commands (hilt's s3perm maps it to
	// nil), so the surrounding request's proofs cannot authorize
	// /blob/abort; mask them so the uploader falls back to the blob
	// authority captured at UploadPart — the same resolution the
	// session-expiry sweeper uses. blob.Abort rides the write set as of
	// fil-forge/hilt#36.
	ctx = reqscope.WithoutProofStore(ctx)
	won, err := b.multipart.LatchSession(ctx, sess.UploadID, registry.SessionOpen, registry.SessionAborting)
	if err != nil || !won {
		return
	}
	var digests [][]byte
	if parts, err := b.multipart.ListParts(ctx, sess.UploadID); err == nil {
		for _, p := range parts {
			digests = append(digests, p.BlobDigests...)
		}
	}
	if err := b.multipart.DeleteSession(ctx, sess.UploadID); err != nil {
		return
	}
	b.cleanupPartBlobs(ctx, space, sess.UploadID, digests)
}

// cleanupPartBlobs removes spooled blobs that belonged to aborted, expired, or
// superseded parts of uploadID — unless the blob is still referenced: by a
// part of another in-flight session (content-addressed dedup), by a part still
// live in THIS session (a re-uploaded part may share blobs with its
// replacement or a sibling part), or by a committed object (reference claims /
// non-spooled intent state). Best-effort: cleanup failure never fails the S3
// operation; a stranded spool file is reapable later.
func (b *Backend) cleanupPartBlobs(ctx context.Context, space did.DID, uploadID string, digests [][]byte) {
	if len(digests) == 0 {
		return
	}
	// Digests still referenced by this session's live parts (after the
	// abort/supersede that triggered this cleanup).
	live := map[string]bool{}
	if parts, err := b.multipart.ListParts(ctx, uploadID); err == nil {
		for _, p := range parts {
			for _, d := range p.BlobDigests {
				live[string(d)] = true
			}
		}
	}
	seen := map[string]bool{}
	for _, d := range digests {
		k := string(d)
		if seen[k] || live[k] {
			continue
		}
		seen[k] = true
		if n, err := b.multipart.CountPartRefs(ctx, d, uploadID); err != nil || n > 0 {
			continue
		}
		if n, err := b.blobRefs.CountClaims(ctx, space, d); err != nil || n > 0 {
			continue
		}
		state := registry.IntentSpooled
		if in, err := b.intents.GetIntent(ctx, d); err == nil {
			state = in.State
		}
		if state != registry.IntentSpooled && state != registry.IntentParked {
			// Accepted/published blobs are the reference index's to manage.
			continue
		}
		// A parked blob is durable on its provider — release it there too
		// (best-effort; the reject on piri is idempotent, a straggler is
		// FIL-625's to reap). Cause is the /blob/add task link the
		// upload service needs to locate the provider. A BlobAccepted
		// refusal is benign — a concurrent session in this space accepted
		// the same content, so the reference index owns the blob now — and
		// the park row is obsolete either way.
		if state == registry.IntentParked {
			if park, err := b.parks.GetPark(ctx, d); err == nil {
				if cause, err := cid.Cast(park.AddTask); err == nil {
					if aerr := b.deferred.AbortBlob(ctx, space, mh.Multihash(d), cause); aerr != nil {
						b.logger.Warn("abort parked blob failed; provider-side release deferred",
							zap.String("digest", hex.EncodeToString(d)), zap.Error(aerr))
					}
				}
				if derr := b.parks.DeletePark(ctx, d); derr != nil {
					b.logger.Warn("delete park row failed",
						zap.String("digest", hex.EncodeToString(d)), zap.Error(derr))
				}
			}
		}
		if rerr := b.spool.Remove(mh.Multihash(d)); rerr != nil {
			b.logger.Warn("remove spooled blob failed",
				zap.String("digest", hex.EncodeToString(d)), zap.Error(rerr))
		}
		if derr := b.intents.DeleteIntent(ctx, d); derr != nil {
			b.logger.Warn("delete upload intent failed",
				zap.String("digest", hex.EncodeToString(d)), zap.Error(derr))
		}
	}
}

// parkBlobs makes each blob durable on its provider without accepting it:
// already-located blobs are marked accepted (dedup), already-parked blobs are
// skipped (another part or session parked the same content), the rest upload
// with the conclude deferred (WithConclude(false)) and persist their park
// state for Complete/Abort.
func (b *Backend) parkBlobs(ctx context.Context, space did.DID, blobs []msbucket.BlobRef) error {
	for _, blob := range blobs {
		digest := mh.Multihash(blob.Digest)
		if existing, err := b.locations.GetLocation(ctx, space, blob.Digest); err == nil && existing != nil {
			if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
				return fmt.Errorf("mark accepted (dedup): %w", err)
			}
			continue
		} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup location: %w", err)
		}
		if _, err := b.parks.GetPark(ctx, blob.Digest); err == nil {
			continue // already parked by a sibling part or session
		} else if !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup park: %w", err)
		}

		res, err := b.deferred.UploadBlob(ctx, space, digest, blob.Length, b.spool.Path(digest), uploader.WithConclude(false))
		if err != nil {
			return fmt.Errorf("park blob: %w", err)
		}
		if res.Location != nil {
			// The provider already held accepted bytes for this content —
			// accept ran despite the deferred conclude (dedup), record the
			// location like the synchronous path.
			if err := b.locations.PutLocation(ctx, registry.BlobLocation{
				Space:    space,
				Digest:   blob.Digest,
				Provider: res.Location.Provider,
				URL:      res.Location.URL,
				Size:     res.Location.Size,
			}); err != nil {
				return fmt.Errorf("record location: %w", err)
			}
			if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
				return fmt.Errorf("mark accepted: %w", err)
			}
			continue
		}
		// Location == nil ⇔ parked: durable on the provider with accept
		// deferred — persist the conclude state for Complete/Abort.
		if err := b.parks.PutPark(ctx, registry.BlobPark{
			Digest:        blob.Digest,
			AddTask:       res.AddTask.Bytes(),
			AcceptTask:    res.AcceptTask.Bytes(),
			PutInvocation: res.PutInvocation,
			Size:          blob.Length,
		}); err != nil {
			return fmt.Errorf("record park: %w", err)
		}
		if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentParked); err != nil {
			return fmt.Errorf("mark parked: %w", err)
		}
	}
	return nil
}

// concludeBlobs is Complete's park-aware counterpart to uploadBlobs: located
// blobs are already accepted (dedup); parked blobs conclude their deferred
// /http/put receipt — firing /blob/accept — and record their location;
// blobs that never parked (crash between spool and park) fall back to the
// whole synchronous upload.
func (b *Backend) concludeBlobs(ctx context.Context, space did.DID, blobs []msbucket.BlobRef) error {
	for _, blob := range blobs {
		digest := mh.Multihash(blob.Digest)
		if existing, err := b.locations.GetLocation(ctx, space, blob.Digest); err == nil && existing != nil {
			if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
				return fmt.Errorf("mark accepted (dedup): %w", err)
			}
			continue
		} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup location: %w", err)
		}

		park, err := b.parks.GetPark(ctx, blob.Digest)
		if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return fmt.Errorf("lookup park: %w", err)
		}
		var loc uploader.BlobLocation
		if park != nil {
			addTask, err := cid.Cast(park.AddTask)
			if err != nil {
				return fmt.Errorf("decode park add task: %w", err)
			}
			acceptTask, err := cid.Cast(park.AcceptTask)
			if err != nil {
				return fmt.Errorf("decode park accept task: %w", err)
			}
			loc, err = b.deferred.ConcludeBlob(ctx, space, uploader.UploadedBlob{
				Digest:        digest,
				Size:          park.Size,
				AddTask:       addTask,
				AcceptTask:    acceptTask,
				PutInvocation: park.PutInvocation,
			})
			if err != nil {
				return fmt.Errorf("conclude blob: %w", err)
			}
		} else {
			// Never parked (crash between spool and park): the spooled copy
			// drives the whole synchronous upload.
			res, uerr := b.uploader.UploadBlob(ctx, space, digest, blob.Length, b.spool.Path(digest))
			if uerr != nil {
				return fmt.Errorf("upload blob: %w", uerr)
			}
			if res.Location == nil {
				return fmt.Errorf("upload blob %x: concluding upload returned no location", blob.Digest)
			}
			loc = *res.Location
		}

		if err := b.locations.PutLocation(ctx, registry.BlobLocation{
			Space:    space,
			Digest:   blob.Digest,
			Provider: loc.Provider,
			URL:      loc.URL,
			Size:     loc.Size,
		}); err != nil {
			return fmt.Errorf("record location: %w", err)
		}
		if err := b.intents.SetIntentState(ctx, blob.Digest, registry.IntentAccepted); err != nil {
			return fmt.Errorf("mark accepted: %w", err)
		}
		if park != nil {
			// The sealed put invocation is spent — drop it promptly.
			if err := b.parks.DeletePark(ctx, blob.Digest); err != nil {
				return fmt.Errorf("drop park: %w", err)
			}
		}
	}
	return nil
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
	sess, err := b.openSession(ctx, uploadID, input.Key)
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
	if _, err := b.reg.Get(ctx, bucket); err != nil {
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

	all, err := b.multipart.ListSessions(ctx, bucket)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, fmt.Errorf("s3frontend: list sessions: %w", err)
	}
	// In-flight uploads only, honoring the prefix.
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

// SweepStaleMultipartSessions aborts in-flight multipart sessions older than
// ttl (dropping their spooled parts, exactly like a client Abort) and reaps
// completed/aborting leftovers past the same age. Returns how many sessions
// were cleaned. Called periodically by the daemon's sweeper loop.
func (b *Backend) SweepStaleMultipartSessions(ctx context.Context, ttl time.Duration) (int, error) {
	cutoff := time.Now().Add(-ttl)
	cleaned := 0
	// Stale open sessions: latch (losing gracefully to a concurrent
	// Complete/Abort) and clean up like an abort.
	stale, err := b.multipart.ListStaleSessions(ctx, registry.SessionOpen, cutoff)
	if err != nil {
		return 0, fmt.Errorf("s3frontend: sweep list: %w", err)
	}
	for _, s := range stale {
		won, err := b.multipart.LatchSession(ctx, s.UploadID, registry.SessionOpen, registry.SessionAborting)
		if err != nil || !won {
			continue
		}
		var digests [][]byte
		if parts, err := b.multipart.ListParts(ctx, s.UploadID); err == nil {
			for _, p := range parts {
				digests = append(digests, p.BlobDigests...)
			}
		}
		if err := b.multipart.DeleteSession(ctx, s.UploadID); err != nil {
			continue
		}
		space, serr := b.bucketSpace(ctx, s.Bucket)
		if serr != nil {
			continue // bucket gone; spool rows are reapable later
		}
		b.cleanupPartBlobs(ctx, space, s.UploadID, digests)
		cleaned++
	}
	// Terminal leftovers: completed sessions retained for Complete idempotency,
	// and any 'completing'/'aborting' rows stranded by a crash mid-transition.
	for _, state := range []string{registry.SessionCompleted, registry.SessionCompleting, registry.SessionAborting} {
		leftovers, err := b.multipart.ListStaleSessions(ctx, state, cutoff)
		if err != nil {
			continue
		}
		for _, s := range leftovers {
			if err := b.multipart.DeleteSession(ctx, s.UploadID); err == nil {
				cleaned++
			}
		}
	}
	return cleaned, nil
}

// etagsEqual compares two ETags ignoring surrounding quotes.
func etagsEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}
