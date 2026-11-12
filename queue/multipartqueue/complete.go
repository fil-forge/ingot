package multipartqueue

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// CompleteRequest is a CompleteMultipartUpload's request.
type CompleteRequest struct {
	// Bucket is the bucket the request names.
	Bucket string
	// Key is the object key the request names.
	Key string
	// UploadID is the upload the request completes.
	UploadID string
	// Parts are the parts the request lists, in its order.
	Parts []types.CompletedPart
	// ChecksumType is the checksum type the request names; empty for none.
	ChecksumType types.ChecksumType
	// Checksums are the final checksums the request carries, by algorithm.
	Checksums map[types.ChecksumAlgorithm]string
	// IfMatch is the request's If-Match; nil for none.
	IfMatch *string
	// IfNoneMatch is the request's If-None-Match; nil for none.
	IfNoneMatch *string
	// ObjectSize is the object size the request states; nil for none.
	ObjectSize *int64
	// Report is called once, before the parts are accepted, with the
	// version id the response will carry ("" for none) and whether the
	// object is stored as received: the response's early headers.
	Report func(versionID string, plaintext bool)
}

// CompleteResult is a Complete's outcome.
type CompleteResult struct {
	// ETag is the object's ETag, unquoted.
	ETag string
	// VersionID is the version id the response carries; "" for none.
	VersionID string
	// Plaintext is set when the object is stored as received.
	Plaintext bool
	// ChecksumAlgorithm, Checksum and ChecksumType are the object's final
	// checksum; empty for none, and for a repeated Complete, which reports
	// none.
	ChecksumAlgorithm types.ChecksumAlgorithm
	// Checksum is the final checksum's value.
	Checksum string
	// ChecksumType is the final checksum's type.
	ChecksumType types.ChecksumType
	// Space is the upload's space.
	Space did.DID
	// Orphans are the blobs of the parts the Complete left out, whose
	// releases it queued, due now.
	Orphans []mh.Multihash
}

// completeCall is a Complete's request, for its
// queue-multipart-upload.complete, and what the Complete tells it back.
type completeCall struct {
	req CompleteRequest
	res CompleteResult
	// reported is whether the Complete has reported its headers. It outlives
	// a run of the task's tree, because the first report is the one that
	// goes out and a tree whose commit fails runs again from scratch.
	reported bool
	// reserved is the version the Complete reserved and reported, kept for
	// the same reason: a run again installs the id the client already has.
	reserved *bucketqueue.ReservedVersion
}

// Complete runs the upload's queue-multipart-upload.complete. It waits for
// the upload's other changes, a Complete already running included, within
// the complete wait: a Complete that lost to another then replays its
// result. Past it the answer is ErrInProgress, never ErrNoSuchUpload. The
// wait is not time for the other Complete to finish: one that waits silently
// longer than the client's read timeout is cut off as the other was, so
// raising it cannot help.
func (q *Queue) Complete(ctx context.Context, req CompleteRequest) (CompleteResult, error) {
	call := &completeCall{req: req}
	spec := uploadSpec(req.UploadID, CompleteAction, nil)
	expires := time.Now().Add(q.limits.CompleteWait)
	spec.Expires = &expires
	_, err := q.runner.Run(context.WithValue(ctx, completeKey{}, call), spec)
	if errors.Is(err, taskrunner.ErrExpired) {
		return CompleteResult{}, ErrInProgress
	}
	if err != nil {
		return CompleteResult{}, err
	}
	return call.res, nil
}

// runComplete is queue-multipart-upload.complete (complete).
func (q *Queue) runComplete(ctx context.Context, ex *taskrunner.Exec, _ taskrunner.Task) ([]byte, error) {
	call, _ := ctx.Value(completeKey{}).(*completeCall)
	if call == nil {
		return nil, taskrunner.Permanent(errors.New("complete: its request has gone"))
	}
	res, err := q.complete(ctx, ex, call)
	if err != nil {
		return nil, err
	}
	call.res = res
	return nil, nil
}

// complete assembles the object from the parts the request lists and
// commits it, on the upload's subject: no part lands and no Abort runs
// meanwhile. A completed upload replays the Complete that committed it.
func (q *Queue) complete(ctx context.Context, ex *taskrunner.Exec, call *completeCall) (CompleteResult, error) {
	req := call.req
	// Only the first report counts, and a report is binding, so the
	// reservation below needs to know whether "" went out first. Every report
	// comes after the session has loaded, and carries its encryption.
	var plaintext bool
	report := func(versionID string) {
		if !call.reported {
			call.reported = true
			req.Report(versionID, plaintext)
		}
	}
	bucket, key, uploadID := req.Bucket, req.Key, req.UploadID
	bucketState, err := q.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return CompleteResult{}, bucketop.ErrBucketNotFound
		}
		return CompleteResult{}, fmt.Errorf("multipartqueue: complete: %w", err)
	}
	sess, err := q.multipart.GetSession(ctx, uploadID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return CompleteResult{}, ErrNoSuchUpload
		}
		return CompleteResult{}, fmt.Errorf("multipartqueue: complete: %w", err)
	}
	// Upload ids are global: the session must be this bucket and key's, or the
	// assembly below would commit another upload's parts under the request's
	// target. Unlike the other multipart operations this admits a completed
	// session, which the idempotent re-Complete replays.
	if sess.Bucket != bucket || sess.ObjectKey != key {
		return CompleteResult{}, ErrNoSuchUpload
	}
	// Nor a session of an earlier bucket by this name: its parts live in
	// that bucket's space, and this bucket's is another.
	if sess.Space.Defined() && sess.Space != bucketState.Space {
		return CompleteResult{}, ErrNoSuchUpload
	}
	plaintext = sess.Plaintext

	// A Complete naming a checksum type must match the CreateMultipartUpload
	// declaration.
	if req.ChecksumType != "" && types.ChecksumType(sess.ChecksumType) != req.ChecksumType {
		return CompleteResult{}, &ChecksumTypeError{Type: types.ChecksumType(sess.ChecksumType)}
	}

	// Conditional writes: S3 documents only If-None-Match: * (don't overwrite)
	// and If-Match (require current ETag) for Complete; a concrete If-None-Match
	// value — or combining If-None-Match with If-Match — is unsupported.
	ifMatch, ifNoneMatch := req.IfMatch, req.IfNoneMatch
	if ifNoneMatch != nil && (*ifNoneMatch != "*" || ifMatch != nil) {
		return CompleteResult{}, ErrConditionUnsupported
	}
	// Not for a completed upload, which replays its result: after its
	// commit, the precheck would newly fail an If-None-Match repeat.
	if sess.State != registry.SessionCompleted && (ifMatch != nil || ifNoneMatch != nil) {
		curETag, exists, lerr := q.currentETag(ctx, bucket, key)
		if lerr != nil {
			return CompleteResult{}, lerr
		}
		if ifMatch != nil {
			if !exists {
				return CompleteResult{}, ErrNoSuchKey
			}
			if !etagsEqual(*ifMatch, curETag) {
				return CompleteResult{}, ErrPreconditionFailed
			}
		}
		if ifNoneMatch != nil && exists {
			return CompleteResult{}, ErrPreconditionFailed
		}
	}

	if len(req.Parts) == 0 {
		return CompleteResult{}, ErrInvalidPart
	}
	// Without the trees: validation and the replay of a completed upload
	// need part numbers, sizes, ETags and checksums only, and the trees are
	// gone once an upload completes. They load once the parts are accepted.
	stored, err := q.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return CompleteResult{}, fmt.Errorf("multipartqueue: list parts: %w", err)
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
			return CompleteResult{}, fmt.Errorf("multipartqueue: composite checksum reader: %w", err)
		}
	}
	var composable string
	missingStored := false

	// Validate the requested parts (in-range and ascending, each matching a
	// recorded part by number + ETag + checksum) and compute the multipart
	// ETag while accumulating the final checksum.
	requested := make([]registry.MultipartPart, 0, len(req.Parts))
	etagHasher := md5.New()
	prev := 0
	for _, rp := range req.Parts {
		// A part entry missing either field is malformed; a part number
		// below 1 is invalid (both per the upstream posix backend).
		// Out-of-range numbers fall through to the membership check (no
		// stored part can match) and fail with ErrInvalidPart.
		if rp.PartNumber == nil || rp.ETag == nil {
			return CompleteResult{}, ErrMalformedPartList
		}
		num := int(*rp.PartNumber)
		if num < 1 {
			return CompleteResult{}, &PartError{Kind: PartNumberInvalid, UploadID: uploadID, PartNumber: *rp.PartNumber, ETag: *rp.ETag, Arg: strconv.Itoa(num)}
		}
		if num <= prev {
			return CompleteResult{}, ErrInvalidPartOrder
		}
		prev = num
		sp, ok := byNum[num]
		if !ok {
			return CompleteResult{}, ErrInvalidPart
		}
		if !etagsEqual(*rp.ETag, hex.EncodeToString(sp.ETagMD5)) {
			return CompleteResult{}, ErrInvalidPart
		}
		stored := ""
		if mpHadChecksum {
			stored = sp.Checksum
		}
		if err := checkPartChecksum(sessAlgo, stored, rp, uploadID); err != nil {
			return CompleteResult{}, err
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
				return CompleteResult{}, fmt.Errorf("multipartqueue: process part %d checksum: %w", num, err)
			}
		case len(requested) == 1:
			composable = sp.Checksum
		default:
			composable, err = utils.AddCRCChecksum(ckAlgo, composable, sp.Checksum, sp.Size)
			if err != nil {
				return CompleteResult{}, fmt.Errorf("multipartqueue: combine part %d checksum: %w", num, err)
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
			return CompleteResult{}, ErrPartTooSmall
		}
		trueOffsets = append(trueOffsets, total)
		total += sp.Size
		blobCount += len(sp.BlobDigests)
	}
	// A manifest holds at most MaxBodyBlobs blobs. The default blob size never
	// reaches it; a small max_blob_size can, and the completion is refused
	// here. The parts' blobs may be accepted already; an abort releases them
	// as such.
	if blobCount > q.limits.MaxBodyBlobs {
		q.logger.Info("multipart completion refused: too many blobs for one object",
			zap.String("key", key), zap.Int("blobs", blobCount), zap.Int("max", q.limits.MaxBodyBlobs))
		return CompleteResult{}, ErrTooManyBlobs
	}
	if req.ObjectSize != nil && (*req.ObjectSize < 0 || *req.ObjectSize != total) {
		return CompleteResult{}, &ObjectSizeError{Stated: *req.ObjectSize, Total: total}
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
			if got := req.Checksums[ckAlgo]; got != "" {
				if ckType == types.ChecksumTypeComposite && !strings.Contains(got, "-") {
					got = fmt.Sprintf("%s-%d", got, len(requested))
				}
				if got != ckValue {
					return CompleteResult{}, &ChecksumError{Algorithm: ckAlgo}
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
			return CompleteResult{}, ErrInvalidPart
		}
		// A re-Complete of an already-completed upload returns the ETag
		// but no checksum: AWS omits it on the idempotent replay (for both
		// COMPOSITE and FULL_OBJECT), unlike the first Complete. A null
		// version is one the first Complete committed outside an Enabled
		// bucket, and sent no id for.
		versionid := sess.CommittedVersionID
		if versionid == registry.NullVersionID {
			versionid = ""
		}
		report(versionid)
		return CompleteResult{ETag: sess.CommittedETag, VersionID: versionid, Plaintext: sess.Plaintext}, nil
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
	trees, err := q.multipart.ListPartTrees(ctx, uploadID)
	if err != nil {
		return CompleteResult{}, fmt.Errorf("multipartqueue: list part trees: %w", err)
	}
	if need := rehashNeeded(requested, trees, trueOffsets); need > q.limits.RehashBudget {
		q.logger.Info("multipart completion refused: re-hash over budget",
			zap.String("key", key), zap.Int("parts", len(requested)), zap.Int64("size", total), zap.Int64("rehash", need), zap.Int64("budget", q.limits.RehashBudget))
		return CompleteResult{}, &PartLayoutError{Need: need, Budget: q.limits.RehashBudget}
	}

	// An Enabled bucket's new version id goes out now, before the parts are
	// accepted: the response headers are committed at the report, as S3
	// commits its early 200 with x-amz-version-id, and an error after it can
	// only go in the 200's body. The re-hash refusal above comes first so
	// that it keeps its 400. The commit then installs exactly this id, or
	// fails (bucketqueue.ErrReservationSuperseded). Every other Complete
	// reports no id, and the commit then refuses to create a numbered
	// version, which the client could never learn.
	if call.reserved == nil {
		call.reserved = &bucketqueue.ReservedVersion{}
		if bucketState.Versioning == registry.VersioningEnabled && !call.reported {
			r, err := q.buckets.Reserve(ctx, bucket)
			if err != nil {
				call.reserved = nil
				return CompleteResult{}, err
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
		lengths, err := q.partSpanLengths(ctx, bucketState.Space, sess.Plaintext, sp)
		if err != nil {
			return CompleteResult{}, err
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
	if err := q.acceptRequested(ctx, uploadID, sess.Space, requested); err != nil {
		return CompleteResult{}, err
	}

	// The object's BLAKE3 digest and blocks, assembled from the parts' trees,
	// re-reading a part only where its record cannot serve (bounded above,
	// before the parts were accepted).
	body := msbucket.Body{Size: blobs.Size(), PartSizes: partSizes, PartChecksums: partChecksums}
	tree, err := q.multipartTree(ctx, bucketState.Space, sess.Plaintext, requested, trees, partOffsets, blobs)
	if err != nil {
		return CompleteResult{}, fmt.Errorf("multipartqueue: object tree: %w", err)
	}
	if err := body.SetTree(tree); err != nil {
		return CompleteResult{}, fmt.Errorf("multipartqueue: object tree: %w", err)
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
	initState, err := bucketqueue.LockState(bucketState, types.ObjectLockMode(sess.LockMode), sess.LockRetainUntil, types.ObjectLockLegalHoldStatus(sess.LockLegalHold))
	if err != nil {
		return CompleteResult{}, err
	}
	tags, err := backend.ParseObjectTags(sess.Tagging)
	if err != nil {
		// Validated at CreateMultipartUpload; a parse failure here is a bug.
		return CompleteResult{}, fmt.Errorf("multipartqueue: parse session tagging: %w", err)
	}
	if len(tags) > 0 {
		if initState == nil {
			initState = &msbucket.VersionState{}
		}
		initState.Tags = tags
	}

	// Commit through the §5 write rule (docs/s3-versioning.md), as a child:
	// seq allocation, supersession per the bucket's versioning state, and
	// the reference-index claims and drops with the root's advance, all
	// committed with this task. The commit re-checks the conditional-write
	// preconditions against the version it supersedes.
	put := &bucketqueue.PutChange{Manifest: mf, HasBlobs: true, Blobs: blobs, InitState: initState, Reserved: reserved, Completes: true}
	if ifMatch != nil {
		put.HasIfMatch, put.IfMatch = true, *ifMatch
	}
	if ifNoneMatch != nil {
		put.HasIfNoneMatch, put.IfNoneMatch = true, *ifNoneMatch
	}
	auth := q.capture(ctx, bucketState.Space)
	out, err := q.buckets.CommitChild(ctx, ex, bucketState, auth, bucketqueue.KeyChange{Key: key, Put: put})
	if err != nil {
		return CompleteResult{}, err
	}
	node := out.Results[0]
	if node.Err != nil {
		if errors.Is(node.Err, bucketqueue.ErrReservationSuperseded) {
			q.logger.Info("multipart completion refused: the key or the bucket's versioning changed since it reported its version",
				zap.String("key", key), zap.String("uploadID", uploadID), zap.String("versionID", reserved.VersionID))
		}
		return CompleteResult{}, node.Err
	}

	// With the object: the session, recorded completed with the committed
	// ETag and version id, is retained with its parts so a repeated Complete
	// replays this result, until its reap. The parts' tree material is dead
	// now that the manifest holds the object's tree, and it is the bulk of a
	// large upload's rows (up to about 320 KiB per 5 GiB part).
	done := registry.CompletedSession(uploadID, etag, node.VersionID)
	if err := ex.WriteConditional(ctx, done.SQL, done.Args...); err != nil {
		return CompleteResult{}, err
	}
	dropTrees := registry.DeletedPartTrees(uploadID)
	if err := ex.Write(ctx, dropTrees.SQL, dropTrees.Args...); err != nil {
		return CompleteResult{}, err
	}
	if q.sessionTTL >= 0 {
		spec := uploadSpec(uploadID, ReapAction, nil)
		at := time.Now().Add(q.sessionTTL)
		spec.NotBefore = &at
		if _, err := ex.Submit(ctx, spec); err != nil {
			return CompleteResult{}, err
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
	if err := spawnReleases(ctx, ex, bucketState.Space, orphans, auth.Release, registry.DueNow()); err != nil {
		return CompleteResult{}, err
	}

	// The x-amz-version-id of the new version. Only an enabled bucket echoes it;
	// a suspended bucket stores the object under the "null" version id but omits
	// it from the CompleteMultipartUpload response, as does an unversioned bucket
	// (docs/s3-versioning.md §4.3).
	versionid := ""
	if out.Versioning == registry.VersioningEnabled {
		versionid = node.VersionID
	}
	res := CompleteResult{ETag: etag, VersionID: versionid, Plaintext: mf.Plaintext, Space: bucketState.Space, Orphans: orphans}
	if ckValue != "" {
		res.ChecksumAlgorithm, res.Checksum, res.ChecksumType = ckAlgo, ckValue, ckType
	}
	return res, nil
}

// currentETag resolves the committed ETag of (bucket, key) for a conditional
// Complete's precheck. It distinguishes "no such key" (exists = false, no
// error) from real errors; a delete-marker current counts as "no object".
func (q *Queue) currentETag(ctx context.Context, bucket, key string) (_ string, _ bool, err error) {
	st, err := q.reg.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return "", false, bucketop.ErrBucketNotFound
		}
		return "", false, err
	}
	ctx, span := tracing.Start(ctx, "tree.lookup")
	defer func() { tracing.End(span, err) }()
	if !st.Root.Defined() {
		return "", false, nil
	}
	valCid, err := mst.LoadMST(q.read, st.Space, st.Root).Get(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("multipartqueue: mst get: %w", err)
	}
	var val msbucket.ObjectValue
	if err := q.read.Get(ctx, st.Space, valCid, &val); err != nil {
		return "", false, fmt.Errorf("multipartqueue: value get: %w", err)
	}
	mf := val.Manifest
	if mf == nil {
		var em msbucket.EnvelopedManifest
		if err := q.read.Get(ctx, st.Space, val.Leaf.Current.Manifest, &em); err != nil {
			return "", false, fmt.Errorf("multipartqueue: manifest get: %w", err)
		}
		mf = em.Manifest
	}
	if mf.DeleteMarker {
		return "", false, nil
	}
	return mf.QuotedETag(), true, nil
}

// etagsEqual compares two ETags ignoring surrounding quotes.
func etagsEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}
