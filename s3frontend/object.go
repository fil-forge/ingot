package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	"go.opentelemetry.io/otel/attribute"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

const defaultMaxKeys = 1000

// PutObject writes an object. Tagging and ACLs are dropped on the floor for
// now (see bucket-metadata.rfc §"Canonical state vs service state"); lock
// headers stamp the new version's state (docs/s3-object-lock.md §7). ETag is
// the hex md5 of the body, quoted per S3 wire format.
// bodyMD5Source supplies a body's MD5 in place of ingest's own pass. Ingest
// calls it after the body has been read, so it may be a digest the request
// carried or the request's MD5 checksum reader, whose Sum is final only once
// the stream is consumed. It returns the raw 16-byte digest.
type bodyMD5Source func() []byte

// knownMD5 is the bodyMD5Source for a digest already in hand.
func knownMD5(digest []byte) bodyMD5Source { return func() []byte { return digest } }

// checksumReaderMD5 is the bodyMD5Source for a HashReader computing MD5 over
// the body: its Sum is the base64 digest once the read completes.
func checksumReaderMD5(hr *utils.HashReader) bodyMD5Source {
	return func() []byte { return decodeContentMD5(hr.Sum()) }
}

// clientProvidedBodyMD5 returns where the object body's MD5 comes from when
// the request already establishes it, so ingest can skip its own MD5 pass: a
// Content-MD5 header (the checksum middleware verifies it against the stream
// before the backend commits), or an MD5 checksum spec, explicit
// (x-amz-checksum-md5, validated by hr on the same read) or algorithm-only
// (x-amz-checksum-algorithm: MD5, computed by hr on the same read). It returns
// nil when the request supplies no usable MD5, in which case ingest computes
// it as before. A malformed Content-MD5 yields nil; the middleware still
// rejects it during the body read, so ingest never commits a wrong ETag.
func clientProvidedBodyMD5(ctx context.Context, spec *checksumSpec, hr *utils.HashReader) bodyMD5Source {
	if digest := decodeContentMD5(contentMD5Header(ctx)); digest != nil {
		return knownMD5(digest)
	}
	if hr != nil && spec != nil && spec.algo == types.ChecksumAlgorithmMd5 {
		return checksumReaderMD5(hr)
	}
	return nil
}

// contentMD5Header returns the request's Content-MD5 value, matched
// case-insensitively (fasthttp canonicalizes the key to "Content-Md5"), or ""
// when the request set none or no request is in the context.
func contentMD5Header(ctx context.Context) string {
	req, ok := reqscope.Request(ctx)
	if !ok {
		return ""
	}
	for k, v := range req.Headers {
		if strings.EqualFold(k, "Content-Md5") {
			return v
		}
	}
	return ""
}

// decodeContentMD5 decodes a base64 Content-MD5 / x-amz-checksum-md5 value to
// its raw 16 bytes, or nil when it is empty or not a 16-byte digest.
func decodeContentMD5(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(b) != md5.Size {
		return nil
	}
	return b
}

// unsupportedObjectACL reports whether a PutObject / CreateMultipartUpload
// request sets any object ACL. ingot does not model ACLs, so — unlike a bucket,
// which still accepts the default "private" — it rejects an object request that
// names any canned ACL or grantee outright. The controller forwards these from
// the x-amz-acl / x-amz-grant-* headers.
func unsupportedObjectACL(acl types.ObjectCannedACL, grants ...*string) bool {
	if acl != "" {
		return true
	}
	for _, g := range grants {
		if g != nil && *g != "" {
			return true
		}
	}
	return false
}

func (b *Backend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	bucketName := *input.Bucket
	key := *input.Key
	if err := objectKeyError(key); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if unsupportedObjectACL(input.ACL, input.GrantFullControl, input.GrantRead, input.GrantReadACP, input.GrantWriteACP) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	}
	sse, err := requestedEncryption(ctx)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	contentType := backend.GetStringFromPtr(input.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Bucket must exist before we upload, so a PUT to a missing bucket
	// doesn't waste an upload. The commit finds it again on the bucket's
	// subject.
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
	// Stored as received only under a "none" bucket the request did not ask
	// AES256 of (sse.go).
	plaintext := storesPlaintext(bucketState, sse)

	// PRECONDITIONS: If-Match / If-None-Match. Evaluated here to fail fast
	// before ingest, then RE-CHECKED by the commit on the bucket's subject so
	// a concurrent writer can't slip between this read and the commit (§3).
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

	// INGEST (no lock): stream the body → coarse-split into blobs → stream
	// each to its provider (allocate→PUT→accept). A 200 means every body blob is durable and accepted before the manifest
	// that references it is committed (docs/architecture.md §7.1).
	bodyRec, blobs, refs, err := b.ingestBody(ctx, bucketState, bodyReader, declaredLength(input.ContentLength), clientProvidedBodyMD5(ctx, spec, hr), plaintext)
	if err != nil {
		// A checksum/digest mismatch surfaces from the HashReader. BadDigestError,
		// InvalidDigestError, and ContentSHA256MismatchError embed APIError but are
		// distinct concrete types, so errors.As against APIError won't match them;
		// surface each verbatim so its 400 status and XML body reach the versitygw
		// renderer (a generic %w wrap degrades it to InternalError).
		var bd s3err.BadDigestError
		if errors.As(err, &bd) {
			return s3response.PutObjectOutput{}, bd
		}
		var idg s3err.InvalidDigestError
		if errors.As(err, &idg) {
			return s3response.PutObjectOutput{}, idg
		}
		var csm s3err.ContentSHA256MismatchError
		if errors.As(err, &csm) {
			return s3response.PutObjectOutput{}, csm
		}
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

	// COMMIT, on the bucket's subject: the §5 write rule — seq allocation,
	// manifest + rebuilt leaf + MST splice + the root's advance carrying the
	// reference-index claims and drops (docs/s3-versioning.md). If-Match and
	// If-None-Match are checked there, against the version the put would
	// supersede.
	mf := &msbucket.ObjectManifest{
		Key:                     key,
		ContentType:             contentType,
		Created:                 time.Now().Unix(),
		Body:                    bodyRec,
		ETag:                    hex.EncodeToString(bodyRec.MD5),
		ChecksumAlgorithm:       ckAlgo,
		Checksum:                ckVal,
		ChecksumType:            string(types.ChecksumTypeFullObject),
		ContentEncoding:         normalizeContentEncoding(backend.GetStringFromPtr(input.ContentEncoding)),
		ContentDisposition:      backend.GetStringFromPtr(input.ContentDisposition),
		ContentLanguage:         backend.GetStringFromPtr(input.ContentLanguage),
		CacheControl:            backend.GetStringFromPtr(input.CacheControl),
		Expires:                 backend.GetStringFromPtr(input.Expires),
		WebsiteRedirectLocation: backend.GetStringFromPtr(input.WebsiteRedirectLocation),
		Metadata:                input.Metadata,
		Plaintext:               plaintext,
	}
	put := &PutChange{Manifest: mf, HasBlobs: blobs != nil, Blobs: blobs, Refs: blobRefTasks(refs), InitState: initState}
	if input.IfMatch != nil {
		put.HasIfMatch, put.IfMatch = true, *input.IfMatch
	}
	if input.IfNoneMatch != nil {
		put.HasIfNoneMatch, put.IfNoneMatch = true, *input.IfNoneMatch
	}
	node, effState, err := b.commitOne(ctx, bucketState, KeyChange{Key: key, Put: put}, "commit version")
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	size := mf.Body.Size
	out := s3response.PutObjectOutput{
		ETag:                 etagOf(mf),
		Size:                 &size,
		ServerSideEncryption: encryptionOf(mf),
	}
	// Only an enabled bucket echoes a version id in the response. A suspended
	// bucket stores the object under the "null" version id but omits it from the
	// PUT response, matching AWS (docs/s3-versioning.md §4.3).
	if effState == registry.VersioningEnabled {
		out.VersionID = node.VersionID
	}
	out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(ckAlgo, ckVal, mf.ChecksumType)
	return out, nil
}

// ingestBody streams an object body off-lock: it coarse-splits into blobs,
// streams each to its provider (SplitSizedBody), and runs each blob's
// queue-space-shard.add, which concludes it, records its location and intent
// and takes the write's pending reference to it. It returns the Body the
// manifest will pin, its blobs, which the commit writes into the manifest,
// and the references, by string(digest), which the commit's claims take
// over. A zero-byte body has no blobs and uploads nothing. A non-nil md5Src
// is where the body's MD5 comes from instead of ingest's own pass (see
// bodyMD5Source); ingest reads it after the body and stamps it onto Body.MD5.
//
// On any error the blobs already streamed are left for the provider's
// expiry, and the references already taken for their expiry; no manifest is
// written, so no catalog entry ever references a non-durable blob.
//
// size is the body's declared length (see splitBody); plaintext stores the
// body as received (see storesPlaintext).
func (b *Backend) ingestBody(ctx context.Context, bucket *registry.State, r io.Reader, size int64, md5Src bodyMD5Source, plaintext bool) (msbucket.Body, msbucket.BlobRefs, map[string]taskrunner.TaskID, error) {
	body, err := b.splitBody(ctx, bucket.Space, r, size, md5Src, plaintext, !b.dropAcceptedBodies)
	if err != nil {
		return msbucket.Body{}, nil, nil, err
	}
	defer body.done()
	refs, err := b.concludeBody(ctx, bucket.Name, bucket.Space, body)
	if err != nil {
		return msbucket.Body{}, nil, nil, err
	}
	return body.Body, body.Blobs, refs, nil
}

// writtenBody is a body split and streamed, with the uploads its blobs went
// to and their stored sizes, keyed by string(digest). The blobs are parked
// on their providers and need only their conclude.
type writtenBody struct {
	msbucket.Body
	// Blobs is the body's blob list as split, which the commit writes into
	// Body.Blobs (see msbucket.BuildBlobList). It shadows that field here.
	Blobs    msbucket.BlobRefs
	streamed map[string]uploader.StreamedBlob
	sizes    map[string]int64
	// locals are the blobs' copies in the cache, not yet adopted, by
	// string(digest).
	locals map[string]*blockstore.TempBlob
}

// done discards the copies not adopted.
func (wb writtenBody) done() {
	for _, l := range wb.locals {
		l.Discard()
	}
}

// declaredLength is a request's body length. S3 requires one on every
// PutObject and UploadPart (versitygw's auth middleware refuses a request
// without it, and takes an aws-chunked body's from its decoded length), so
// only a direct caller of the backend passes none, meaning an empty body.
func declaredLength(n *int64) int64 {
	if n == nil || *n < 0 {
		return 0
	}
	return *n
}

// splitBody coarse-splits a body into blobs, encrypts each into a FEE
// envelope (recipient: the tenant's wrap key) streamed to its provider under
// its ciphertext digest, and records a blob_encryption_params row per blob —
// WITHOUT concluding. It is the shared first half of ingest: a single-shot
// PUT follows it with concludeBody immediately; a multipart UploadPart parks
// the blobs and defers the conclude to Complete. Each blob's add records its
// upload intent.
//
// The Body it returns is entirely plaintext-coordinate (Size, spans,
// SHA256/MD5 — all computed before encryption); the intents record the
// stored (ciphertext) byte count, which is what the provider holds (see
// writtenBody.sizes). With
// plaintext set the body is stored as received, so the two coincide, and no
// key material is minted or recorded for its blobs. A non-nil md5Src
// supplies the body's MD5 (a verified Content-MD5, the request's MD5
// checksum reader, or a single-part copy source's digest): the MD5 pass is
// skipped and Body.MD5 is read from md5Src once the body is consumed.
//
// size is the body's declared length; a body that turns out a different
// length fails. cache tees each blob into the cache, for the write to adopt
// once the provider holds it. opts are passed through to the split (a part
// skips the whole-body tree).
func (b *Backend) splitBody(ctx context.Context, space did.DID, r io.Reader, size int64, md5Src bodyMD5Source, plaintext, cache bool, opts ...msbucket.SplitOption) (_ writtenBody, err error) {
	// The span covers receiving the body (it streams in from the client as
	// SplitSizedBody reads it), encrypting it and sending it to its
	// providers; the body.received event marks where the client finished
	// sending.
	ctx, span := tracing.Start(ctx, "body.write")
	defer func() { tracing.End(span, err) }()
	if r == nil {
		r = bytes.NewReader(nil)
	}
	r = &receivedReader{r: r, span: span}
	// One tenant recipient per request, resolved before anything is stored:
	// a body that cannot be wrapped to its tenant is not stored at all. A
	// plaintext body wraps nothing and needs none.
	var enc *bodyWriter
	if plaintext {
		enc = newPlainBlobWriter(b.streaming, space)
	} else {
		recipient, err := b.tenantRecipient(ctx)
		if err != nil {
			return writtenBody{}, err
		}
		enc = newEncryptingBlobWriter(b.streaming, b.regionKeys, space, []fee.Recipient{recipient})
	}
	enc.logger = b.logger
	if cache {
		enc.cache = b.cache
	}
	// A failed split leaves no caller to discard the local copies, or to
	// conclude the blobs it streamed before the failure.
	defer func() {
		if err != nil {
			enc.drop()
			enc.abandonStreamed(ctx)
		}
	}()
	splitOpts := append([]msbucket.SplitOption(nil), opts...)
	if md5Src != nil {
		splitOpts = append(splitOpts, msbucket.WithoutMD5())
	}
	body, blobs, err := msbucket.SplitSizedBody(ctx, enc, r, size, b.maxBlobSize, splitOpts...)
	switch {
	case errors.Is(err, msbucket.ErrBodyShort):
		return writtenBody{}, s3err.GetAPIError(s3err.ErrIncompleteBody)
	case errors.Is(err, msbucket.ErrBodyLong):
		return writtenBody{}, s3err.GetAPIError(s3err.ErrContentLengthMismatch)
	case err != nil:
		return writtenBody{}, fmt.Errorf("split body: %w", err)
	}
	if md5Src != nil {
		// The body read completed without error, so a carried digest was
		// verified against the stream and a checksum reader's digest is
		// final; either is the body's MD5.
		body.MD5 = md5Src()
		if len(body.MD5) != md5.Size {
			return writtenBody{}, fmt.Errorf("split body: MD5 source yielded %d bytes, want %d", len(body.MD5), md5.Size)
		}
	}
	span.SetAttributes(
		attribute.Int64("ingot.body.bytes", body.Size),
		attribute.Int("ingot.body.blobs", len(blobs)),
	)
	sizes := make(map[string]int64, len(blobs))
	for _, blob := range blobs {
		storedSize, err := enc.storedSize(blob.Digest)
		if err != nil {
			return writtenBody{}, err
		}
		sizes[string(blob.Digest)] = storedSize
		if plaintext {
			continue
		}
		// The read path decrypts from this row; it must exist before any
		// manifest referencing the blob can commit.
		params, err := enc.params(space, blob.Digest)
		if err != nil {
			return writtenBody{}, err
		}
		if err := b.encParams.PutEncryptionParams(ctx, params); err != nil {
			return writtenBody{}, fmt.Errorf("record encryption params: %w", err)
		}
	}
	return writtenBody{Body: body, Blobs: blobs, streamed: enc.streamed(), sizes: sizes, locals: enc.locals()}, nil
}

// concludeBody runs the queue-space-shard.add of each blob of a body written
// to bucket in space, which concludes it, and returns the references the
// adds took, by string(digest). A blob the space already holds (the same
// bytes stored as received, written again) is reused, and its stream's
// allocation given up: concluding it would make the upload service return an
// accept receipt with no fresh location commitment, which the edge client
// (correctly) rejects. Each blob's local copy moves into the cache once the
// provider holds it.
func (b *Backend) concludeBody(ctx context.Context, bucket string, space did.DID, body writtenBody) (map[string]taskrunner.TaskID, error) {
	release := b.releaseAuthority(ctx, space)
	refs := make(map[string]taskrunner.TaskID, len(body.Blobs))
	// Each digest once: a body stored as received can hold the same bytes
	// twice, and the provider needs them once.
	for _, blob := range body.Blobs {
		k := string(blob.Digest)
		if _, ok := refs[k]; ok {
			continue
		}
		sb := body.streamed[k]
		ref, err := b.addBlob(ctx, space, bucket, blob.Digest, body.sizes[k], addStreamed, &sb, release)
		if err != nil {
			return nil, err
		}
		refs[k] = ref
		b.shards.Cache(ctx, blob.Digest, body.locals[k])
	}
	return refs, nil
}

// The reference index (blob_refs) is written by the commit: a version's
// claims on its body digests land, and the discarded versions' claims drop,
// with the root's advance (registry.Registry.RootCommitWrites).
// The diff is the crux of safe dedup + delete:
//
//   - a digest in new but not old gains a claim (newly referenced);
//   - each generation's claim rows are keyed by a per-generation id
//     (claimVersionID), so racing writers never touch one shared row;
//   - the new generation's claims and the superseded generation's drops
//     commit with the root or not at all: a failed commit leaves no claim
//     behind and drops none, and a crash can neither leave a version
//     unclaimed nor a discarded one claimed;
//   - a drop of the space's last claim on a digest queues the blob's
//     release in the same transaction (queue-space-shard.release); the
//     release re-checks the claim count when it runs, so every remaining
//     interleave converges.

// claimVersionID names a generation's blob_refs rows: the version's ULID
// token for versioned buckets, or "null#<seq>" for a null version — unique
// per commit, so unversioned generations never share a claim row.
func claimVersionID(versionID string, seq uint64) string {
	if versionID == registry.NullVersionID {
		return fmt.Sprintf("null#%d", seq)
	}
	return versionID
}

// addClaims records one claim per DEDUPLICATED digest for a new generation,
// for the commit to carry with the root's advance. Each claim takes over the write's pending reference to its
// blob, from refs by string(digest), so a release of the blob, which counts
// the reference or the claim, never finds neither. A digest with no
// reference is a Complete's, whose session's parts reference it until the
// teardown that follows the commit.
func (b *Backend) addClaims(tx *bucketop.Tx, st *registry.State, key, claimID string, digests []multihash.Multihash, refs map[string]taskrunner.TaskID) {
	var claims []registry.BlobClaim
	for k, d := range digestSet(digests) {
		claims = append(claims, registry.BlobClaim{Digest: d, Bucket: st.Name, ObjectKey: key, VersionID: claimID, Space: st.Space, Ref: refs[k]})
	}
	tx.ClaimBlobs(claims)
}

// dropClaims records a superseded/deleted generation's claims for the commit
// to drop with the root's advance; the drop of a space's last claim
// on a digest queues the blob's release, due the release grace after the
// commit.
func (b *Backend) dropClaims(sc *commitScope, st *registry.State, key, claimID string, digests []multihash.Multihash) {
	if len(digests) == 0 {
		return
	}
	var drops []registry.ClaimDrop
	for _, d := range digestSet(digests) {
		drops = append(drops, registry.ClaimDrop{Digest: d, Bucket: st.Name, ObjectKey: key, VersionID: claimID, Space: st.Space})
	}
	sc.tx.DropClaims(drops, b.releaseGrace, sc.args.Release)
}

// bodyDigests returns the digests of a stored body's blobs in order, loading
// its extents through store.
func bodyDigests(ctx context.Context, store blockstore.Reader, space did.DID, body msbucket.Body) ([]multihash.Multihash, error) {
	digests, err := body.Index(store, space).Digests(ctx)
	if err != nil {
		return nil, fmt.Errorf("body digests: %w", err)
	}
	return digests, nil
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
	// Non-multipart object: a single logical part covering the whole body. A
	// partNumber past that single part is ErrInvalidPartNumberRange (HTTP 416),
	// matching AWS (verified: GET ?partNumber=2 on a single-part object returns
	// 416 InvalidPartNumber).
	if partNumber != 1 {
		return 0, 0, false, nil, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)
	}
	if body.Size == 0 {
		// A zero-byte object has no range; S3 omits Content-Range and returns 200.
		return 0, 0, false, nil, nil
	}
	return 0, body.Size, true, nil, nil
}

// headObject is HeadObject's metadata lookup, which GetObjectAttributes
// shares: the output, plus the version it describes so a caller derives
// everything else from the same manifest. It writes nothing to the
// response. On the delete-marker errors the marker's version comes back
// with the populated output; other errors return neither.
func (b *Backend) headObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, *resolvedVersion, error) {
	if input.Bucket == nil {
		return nil, nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err := rejectEncryptionHeaders(ctx); err != nil {
		return nil, nil, err
	}
	versionID := backend.GetStringFromPtr(input.VersionId)
	rv, err := b.resolveVersion(ctx, *input.Bucket, *input.Key, versionID)
	if err != nil {
		return nil, nil, err
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
			return mout, rv, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return mout, rv, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	lastModified := time.Unix(mf.Created, 0)
	if err := backend.EvaluatePreconditions(etagOf(mf), lastModified, backend.PreConditions{
		IfMatch:       input.IfMatch,
		IfNoneMatch:   input.IfNoneMatch,
		IfModSince:    input.IfModifiedSince,
		IfUnmodeSince: input.IfUnmodifiedSince,
	}); err != nil {
		// A 304 Not Modified must carry the object's ETag (RFC 7232 §4.1):
		// return the metadata alongside the not-modified error so the S3 API
		// layer emits the ETag header. Other precondition failures (e.g. 412
		// PreconditionFailed) return no metadata.
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) && apiErr.HTTPStatusCode == http.StatusNotModified {
			ifEtag := etagOf(mf)
			return &s3.HeadObjectOutput{ETag: &ifEtag, LastModified: &lastModified}, nil, err
		}
		return nil, nil, err
	}
	etag := etagOf(mf)
	objSize := mf.Body.Size

	// A request selects bytes either by ?partNumber=N or by Range (versitygw
	// rejects both together upstream). Both yield a 206 with Content-Range; a
	// partNumber also carries x-amz-mp-parts-count for a multipart object.
	startOffset, length, isRange, partsCount, err := selectBytes(mf.Body, input.PartNumber, backend.GetStringFromPtr(input.Range))
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	contentType := mf.ContentType
	out := &s3.HeadObjectOutput{
		ServerSideEncryption:      encryptionOf(mf),
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
	if input.ChecksumMode == types.ChecksumModeEnabled {
		switch {
		case !isRange:
			// Whole-object checksum (a ranged read's would not match the object).
			out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum, mf.ChecksumType)
		case input.PartNumber != nil:
			// A ?partNumber read returns that part's own checksum, when the
			// object recorded per-part checksums (a checksummed multipart upload).
			if pn := int(*input.PartNumber); pn >= 1 && pn <= len(mf.Body.PartChecksums) {
				out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Body.PartChecksums[pn-1], mf.ChecksumType)
			}
		}
	}
	return out, rv, nil
}

// HeadObject returns an object's metadata, honoring `?versionId`, the
// conditional-request preconditions, and the same byte-selection as GetObject
// (?partNumber=N or a Range header → a 206 with Content-Range, plus
// x-amz-mp-parts-count for a multipart part). Tagging is not implemented.
func (b *Backend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	out, rv, err := b.headObject(ctx, input)
	if err != nil {
		return out, err
	}
	// The object's CID rides on every successful read, ranged or not: it
	// names the whole object, which a ranged reader verifies against through
	// the tree (see cidHeader).
	setCIDHeader(ctx, rv.mf.Body)
	return out, nil
}

// GetObjectAttributes reports an object's size, ETag, storage class and
// checksum. It reads the same metadata as HeadObject; the controller filters
// the result down to the attributes the client requested. ObjectParts is left
// nil: the manifest stores per-part sizes but no per-part checksums/etags, so
// there is nothing faithful to report, and the shipped posix/azure backends
// likewise omit it.
func (b *Backend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	// headObject rather than HeadObject: the metadata without the x-cid
	// header, which belongs to GET and HEAD responses only, and the manifest
	// it read, so the part list and the Blake3 attribute below describe the
	// same version as the ETag.
	data, rv, err := b.headObject(ctx, &s3.HeadObjectInput{
		Bucket:       input.Bucket,
		Key:          input.Key,
		VersionId:    input.VersionId,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		// A version-scoped delete marker surfaces as ErrMethodNotAllowed with a
		// populated head output; GetObjectAttributes on such a version returns
		// 405 MethodNotAllowed (matching AWS), carrying the marker and version so
		// the controller still emits those headers. The current-marker case
		// (no versionId) surfaces as ErrNoSuchKey from HeadObject and falls
		// through below as a 404.
		if errors.Is(err, s3err.GetAPIError(s3err.ErrMethodNotAllowed)) && data != nil {
			return s3response.GetObjectAttributesResponse{
				DeleteMarker: data.DeleteMarker,
				VersionId:    data.VersionId,
			}, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		return s3response.GetObjectAttributesResponse{}, err
	}

	// A completed multipart object reports its part count. HeadObject only
	// surfaces the count for a ?partNumber request, so read it from the
	// manifest here. The per-part list is left unpopulated: the manifest stores
	// per-part sizes but no per-part checksums, and AWS only returns the Parts
	// list for checksummed multipart uploads — so TotalPartsCount is the
	// faithful subset, matching AWS for a non-checksummed multipart object.
	var objectParts *s3response.ObjectParts
	// The Blake3 attribute (an Ingot extension, see blake3Attribute) is
	// built only when asked for: its outboard is 64 bytes per block, 2 MiB at
	// the 32768-block cap and about 2.7 MiB once base64-encoded. The
	// controller passes the requested names through; a caller that passes
	// none (an older controller, a direct caller) gets it.
	var blake3 *s3response.Blake3Tree
	if wantsBlake3(input.ObjectAttributes) {
		blake3 = blake3Attribute(rv.mf.Body)
	}
	{
		sizes := rv.mf.Body.PartSizes
		sums := rv.mf.Body.PartChecksums
		if n := len(sizes); n > 0 {
			op := &s3response.ObjectParts{PartsCount: n}
			// The per-part list (and its pagination) is populated only for a
			// checksummed multipart object; AWS returns just the count otherwise.
			if len(sums) == n {
				marker := 0
				if input.PartNumberMarker != nil {
					if m, err := strconv.Atoi(*input.PartNumberMarker); err == nil {
						marker = m
					}
				}
				maxParts := 1000
				if input.MaxParts != nil {
					maxParts = int(*input.MaxParts)
				}
				algo := types.ChecksumAlgorithm(rv.mf.ChecksumAlgorithm)
				next := 0
				truncated := false
				for i := 0; i < n; i++ {
					pn := i + 1
					if pn <= marker {
						continue
					}
					if len(op.Parts) >= maxParts {
						truncated = true
						break
					}
					sz := sizes[i]
					part := types.ObjectPart{PartNumber: aws.Int32(int32(pn)), Size: aws.Int64(sz)}
					setObjectPartChecksum(&part, algo, sums[i])
					op.Parts = append(op.Parts, part)
					next = pn
				}
				op.PartNumberMarker = &marker
				op.MaxParts = &maxParts
				op.NextPartNumberMarker = &next
				op.IsTruncated = &truncated
			}
			objectParts = op
		}
	}

	return s3response.GetObjectAttributesResponse{
		Blake3:       blake3,
		ETag:         backend.TrimEtag(data.ETag),
		ObjectSize:   data.ContentLength,
		StorageClass: data.StorageClass,
		LastModified: data.LastModified,
		VersionId:    data.VersionId,
		DeleteMarker: data.DeleteMarker,
		ObjectParts:  objectParts,
		// GetObjectAttributes reports a composite (multipart) checksum without
		// the "-N" part-count suffix that the CompleteMPU response and the
		// checksum headers carry; strip it here to match AWS.
		Checksum: &types.Checksum{
			ChecksumCRC32:     stripCompositeChecksumSuffix(data.ChecksumCRC32),
			ChecksumCRC32C:    stripCompositeChecksumSuffix(data.ChecksumCRC32C),
			ChecksumSHA1:      stripCompositeChecksumSuffix(data.ChecksumSHA1),
			ChecksumSHA256:    stripCompositeChecksumSuffix(data.ChecksumSHA256),
			ChecksumCRC64NVME: stripCompositeChecksumSuffix(data.ChecksumCRC64NVME),
			ChecksumSHA512:    stripCompositeChecksumSuffix(data.ChecksumSHA512),
			ChecksumMD5:       data.ChecksumMD5,
			ChecksumXXHASH64:  stripCompositeChecksumSuffix(data.ChecksumXXHASH64),
			ChecksumXXHASH3:   stripCompositeChecksumSuffix(data.ChecksumXXHASH3),
			ChecksumXXHASH128: stripCompositeChecksumSuffix(data.ChecksumXXHASH128),
			ChecksumType:      data.ChecksumType,
		},
	}, nil
}

// stripCompositeChecksumSuffix removes the "-N" multipart part-count suffix from
// a composite checksum value. GetObjectAttributes returns the bare digest while
// the CompleteMPU response and x-amz-checksum-* headers keep the suffix. A plain
// (single-part) checksum has no such suffix and is returned unchanged.
// setObjectPartChecksum sets the checksum field matching algo on a per-part
// GetObjectAttributes entry.
func setObjectPartChecksum(p *types.ObjectPart, algo types.ChecksumAlgorithm, value string) {
	v := value
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		p.ChecksumCRC32 = &v
	case types.ChecksumAlgorithmCrc32c:
		p.ChecksumCRC32C = &v
	case types.ChecksumAlgorithmSha1:
		p.ChecksumSHA1 = &v
	case types.ChecksumAlgorithmSha256:
		p.ChecksumSHA256 = &v
	case types.ChecksumAlgorithmCrc64nvme:
		p.ChecksumCRC64NVME = &v
	}
}

func stripCompositeChecksumSuffix(v *string) *string {
	if v == nil {
		return nil
	}
	s := *v
	i := strings.LastIndexByte(s, '-')
	if i < 0 || i == len(s)-1 {
		return v
	}
	for _, r := range s[i+1:] {
		if r < '0' || r > '9' {
			return v
		}
	}
	t := s[:i]
	return &t
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
	if err := rejectEncryptionHeaders(ctx); err != nil {
		return nil, err
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
	getLastModified := time.Unix(mf.Created, 0)
	if err := backend.EvaluatePreconditions(etagOf(mf), getLastModified, backend.PreConditions{
		IfMatch:       input.IfMatch,
		IfNoneMatch:   input.IfNoneMatch,
		IfModSince:    input.IfModifiedSince,
		IfUnmodeSince: input.IfUnmodifiedSince,
	}); err != nil {
		// A 304 Not Modified must carry the object's ETag (RFC 7232 §4.1):
		// return the metadata alongside the not-modified error so the S3 API
		// layer emits the ETag header. Other precondition failures (e.g. 412
		// PreconditionFailed) return no metadata.
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) && apiErr.HTTPStatusCode == http.StatusNotModified {
			ifEtag := etagOf(mf)
			return &s3.GetObjectOutput{ETag: &ifEtag, LastModified: &getLastModified}, err
		}
		return nil, err
	}

	objSize := mf.Body.Size
	// Select bytes by ?partNumber=N or by Range (versitygw rejects both together);
	// partsCount is non-nil only for a multipart object addressed by part number.
	startOffset, length, isRange, partsCount, err := selectBytes(mf.Body, input.PartNumber, backend.GetStringFromPtr(input.Range))
	if err != nil {
		return nil, err
	}

	// Resolve how each blob of the bytes read becomes plaintext — the plain
	// opener for unencrypted blobs, the decrypting opener where an
	// encryption row exists. Doing it here (not at first Read) fails a broken
	// encrypted object as a request error, before response headers are
	// written.
	readStart, readEnd := int64(0), objSize-1
	if isRange {
		readStart, readEnd = startOffset, startOffset+length-1
	}
	blobs := mf.Body.Index(b.read, st.Space)
	opener, err := b.bodyOpener(ctx, st.Space, blobs, mf.Plaintext, readStart, readEnd)
	if err != nil {
		return nil, err
	}

	var contentRange *string
	var body = msbucket.OpenBody(ctx, opener, st.Space, blobs)
	if isRange {
		body = msbucket.OpenBodyRange(ctx, opener, st.Space, blobs, readStart, readEnd)
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
		ServerSideEncryption:      encryptionOf(mf),
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
	if input.ChecksumMode == types.ChecksumModeEnabled {
		switch {
		case !isRange:
			// Whole-object checksum (a ranged read's would not match the object).
			out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Checksum, mf.ChecksumType)
		case input.PartNumber != nil:
			// A ?partNumber read returns that part's own checksum, when the
			// object recorded per-part checksums (a checksummed multipart upload).
			if pn := int(*input.PartNumber); pn >= 1 && pn <= len(mf.Body.PartChecksums) {
				out.ChecksumCRC32, out.ChecksumCRC32C, out.ChecksumSHA1, out.ChecksumSHA256, out.ChecksumCRC64NVME, out.ChecksumSHA512, out.ChecksumMD5, out.ChecksumXXHASH64, out.ChecksumXXHASH3, out.ChecksumXXHASH128, out.ChecksumType = checksumFields(mf.ChecksumAlgorithm, mf.Body.PartChecksums[pn-1], mf.ChecksumType)
			}
		}
	}
	// The object's CID rides on every successful read, ranged or not: it
	// names the whole object, which a ranged reader verifies against through
	// the tree (see cidHeader).
	setCIDHeader(ctx, mf.Body)
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
	pre := deletePreconditions(&backend.ObjectDeletePreconditions{
		IfMatch:            input.IfMatch,
		IfMatchLastModTime: input.IfMatchLastModifiedTime,
		IfMatchSize:        input.IfMatchSize,
	})
	versionID := backend.GetStringFromPtr(input.VersionId)
	res, versioning, err := b.commitOne(ctx, bucketState, KeyChange{Key: key, Delete: &DeleteChange{VersionID: versionID, Pre: pre}}, "delete")
	if err != nil {
		return nil, err
	}
	out := &s3.DeleteObjectOutput{}
	switch {
	case versionID != "":
		// Version-scoped: that one version is gone. The response gates on
		// the versioning state the commit ran under.
		if versioning.Configured() {
			vid := versionID
			out.VersionId = &vid
			if res.DeleteMarker {
				marker := true
				out.DeleteMarker = &marker
			}
		}
	case versioning.Configured():
		// Versioned: the delete added a marker.
		marker := true
		out.DeleteMarker, out.VersionId = &marker, &res.VersionID
	}
	return out, nil
}

// applyDeleteKey permanently removes one key — the unversioned bucket's
// delete: it drops the key's value (its manifest; a leaf never occurs
// on a purely-unversioned bucket, but is handled for completeness) and
// releases the (single, null) version's body blobs through the reference
// index. Missing keys (and an empty bucket) are idempotent no-ops. preconds,
// when non-nil, gates the delete on If-Match / size / mod-time under the
// lock. Shared by DeleteObject and DeleteObjects.
func (b *Backend) applyDeleteKey(ctx context.Context, sc *commitScope, key string, pre *DeletePreconditions) (cid.Cid, error) {
	var preconds *backend.ObjectDeletePreconditions
	if pre != nil {
		p := pre.preconditions()
		preconds = &p
	}
	tx := sc.tx
	return func() (cid.Cid, error) {
		// Empty bucket: nothing to delete. Returning cid.Undef leaves the
		// root as it is — the equivalent of "no-op success."
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
		// A manifest-valued key's block is the manifest itself; a leaf key
		// names it. Either way this is the root the space counted.
		oldManifest := valCid
		if val.Leaf != nil {
			var em msbucket.EnvelopedManifest
			if err := tx.Get(ctx, tx.State().Space, val.Leaf.Current.Manifest, &em); err != nil {
				return cid.Undef, fmt.Errorf("load manifest: %w", err)
			}
			oldMf = em.Manifest
			oldManifest = val.Leaf.Current.Manifest
		}

		// Preconditions (If-Match / size / mod-time) against the version
		// being removed.
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
			tx.AddGCCandidate(val.Leaf.Current.Manifest)
		}
		tx.AddGCCandidate(valCid)
		oldDigests, err := bodyDigests(ctx, tx, tx.State().Space, oldMf.Body)
		if err != nil {
			return cid.Undef, err
		}
		// The removed version's claims drop with the commit; each last-claim
		// drop queues a release.
		oldVersionID := oldMf.VersionID
		if oldVersionID == "" {
			oldVersionID = registry.NullVersionID
		}
		b.dropClaims(sc, tx.State(), key, claimVersionID(oldVersionID, oldMf.Seq), oldDigests)
		// The key is gone, so the space stops counting it.
		b.register(sc, tx.State(), key, oldManifest, spaceuploadqueue.Remove)
		return t2.GetPointer(ctx, tx)
	}()
}

// DeleteObjects deletes up to 1000 keys in one request, best-effort (not
// atomic): each key is deleted independently and reported in the per-key result
// (Deleted, or Error on failure). Quiet mode omits the successful entries.
//
// Hilt authorizes the request on s3:DeleteObject. An entry naming a version
// permanently removes it, which S3 gives its own permission; the entries are
// in the body, out of Hilt's sight, so each such entry is checked here against
// the key's action set and fails with AccessDenied without it.
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

	quiet := input.Delete.Quiet != nil && *input.Delete.Quiet
	// Every key the request may delete goes into one commit, which decides
	// each against the bucket's state there: up to 1,000 deletes cost one
	// fsync and one root advance. A key refused here, or by the commit on its
	// own terms, fails alone.
	type entryOf struct {
		key, versionID string
		err            error
		change         int // index into changes, or -1
	}
	var entries []entryOf
	var changes []KeyChange
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil {
			continue
		}
		e := entryOf{key: *obj.Key, versionID: backend.GetStringFromPtr(obj.VersionId), change: -1}
		if e.versionID != "" && !reqscope.Permits(ctx, bucketState.Space, deleteObjectVersionAction) {
			e.err = s3err.GetAPIError(s3err.ErrAccessDenied)
		} else {
			d := &DeleteChange{VersionID: e.versionID}
			// Per-key ETag precondition (conditional delete): the object is
			// removed only if its current version's ETag matches; otherwise
			// the entry fails with PreconditionFailed, matching AWS.
			if obj.ETag != nil && e.versionID == "" {
				d.HasETag, d.ETag = true, *obj.ETag
			}
			e.change = len(changes)
			changes = append(changes, KeyChange{Key: e.key, Delete: d})
		}
		entries = append(entries, e)
	}
	var outcome commitOutcome
	if len(changes) > 0 {
		if outcome, err = b.commit(ctx, bucketState, changes...); err != nil {
			return s3response.DeleteResult{}, err
		}
	}
	var res s3response.DeleteResult
	for _, e := range entries {
		key, versionID := e.key, e.versionID
		var cr ChangeResult
		if e.change >= 0 {
			cr = outcome.results[e.change]
			if cr.Err != nil {
				e.err = mapCommitError(cr.Err.err(), "delete")
			}
		}
		if e.err != nil {
			code, msg := deleteErrorFields(e.err)
			er := types.Error{Key: &key, Code: &code, Message: &msg}
			if versionID != "" && e.change >= 0 {
				vid := versionID
				er.VersionId = &vid
			}
			res.Error = append(res.Error, er)
			continue
		}
		entry := types.DeletedObject{Key: &key}
		switch {
		case versionID != "":
			// Version-scoped: that one version is gone for good. DeleteMarker
			// is set explicitly either way — upstream conformance expects an
			// explicit false for a non-marker version delete.
			if outcome.versioning.Configured() {
				vid := versionID
				entry.VersionId = &vid
				dm := cr.DeleteMarker
				entry.DeleteMarker = &dm
				if cr.DeleteMarker {
					entry.DeleteMarkerVersionId = &vid
				}
			}
		case outcome.versioning.Configured():
			marker := true
			vid := cr.VersionID
			entry.DeleteMarker = &marker
			entry.DeleteMarkerVersionId = &vid
		}
		if !quiet {
			res.Deleted = append(res.Deleted, entry)
		}
	}
	return res, nil
}

// deleteErrorFields maps a per-key delete failure to an S3 error code +
// message for the DeleteObjects per-entry result.
// deleteObjectVersionAction is the S3 permission a DeleteObjects entry naming
// a version needs, beyond the request's own s3:DeleteObject.
const deleteObjectVersionAction = "s3:DeleteObjectVersion"

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
	// Default only when max-keys is unset; an explicit max-keys=0 must return an
	// empty page (handled in listWalk), not be promoted to the default.
	if input.MaxKeys == nil {
		limit = defaultMaxKeys
	}

	from := prefix
	if marker != "" && marker > from {
		// V1 Marker: list strictly after this key.
		from = marker + "\x01"
	}

	res, err := b.listWalk(ctx, bucketName, prefix, delimiter, from, marker, limit)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	out := s3response.ListObjectsResult{
		Name:           &bucketName,
		Prefix:         &prefix,
		Delimiter:      strPtrOrNil(delimiter),
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
	// Default only when max-keys is unset; an explicit max-keys=0 must return an
	// empty page (handled in listWalk), not be promoted to the default.
	if input.MaxKeys == nil {
		limit = defaultMaxKeys
	}

	from := prefix
	if startAfter != "" && startAfter > from {
		// Walk strictly past startAfter by appending a low byte.
		from = startAfter + "\x01"
	}

	res, err := b.listWalk(ctx, bucketName, prefix, delimiter, from, startAfter, limit)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	// FetchOwner attaches the bucket owner to each listed object. Ingot models
	// ownership by the Forge space DID rather than an S3 canonical user id, so
	// that DID is reported as the owner id.
	if input.FetchOwner != nil && *input.FetchOwner && len(res.contents) > 0 {
		if st, gerr := b.reg.Get(ctx, bucketName); gerr == nil {
			ownerID := st.Space.String()
			owner := &types.Owner{ID: &ownerID}
			for i := range res.contents {
				res.contents[i].Owner = owner
			}
		}
	}

	keyCount := int32(len(res.contents) + len(res.commonPrefixes))
	out := s3response.ListObjectsV2Result{
		Name:           &bucketName,
		Prefix:         &prefix,
		Delimiter:      strPtrOrNil(delimiter),
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
func (b *Backend) listWalk(ctx context.Context, bucketName, prefix, delimiter, from, marker string, limit int) (listWalkResult, error) {
	out := listWalkResult{
		contents:       []s3response.Object{},
		commonPrefixes: []types.CommonPrefix{},
	}
	// An explicit max-keys=0 lists nothing and is never truncated.
	if limit <= 0 {
		return out, nil
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

	// Listing cost follows the keys the walk visits, which can be far more
	// than it returns (delete markers, keys rolled into common prefixes).
	var walkErr error
	scanned := 0
	ctx, span := tracing.Start(ctx, "tree.list")
	defer func() {
		span.SetAttributes(
			attribute.Int("ingot.tree.keys_scanned", scanned),
			attribute.Int("ingot.tree.keys_returned", len(out.contents)+len(out.commonPrefixes)),
		)
		tracing.End(span, walkErr)
	}()

	t := mst.LoadMST(b.read, st.Space, st.Root)
	seenPrefix := map[string]struct{}{}
	walkErr = t.WalkLeavesFromNocache(ctx, from, func(k string, valCid cid.Cid) error {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			return mst.ErrStopWalk
		}
		scanned++

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
				// A common prefix rolled up on an earlier page (its value <= the
				// resume marker) is already returned: skip its keys entirely so
				// the marker advances past the whole group, not just past the
				// prefix string. Mirrors ListObjectVersions' keyMarker handling.
				if marker != "" && cp <= marker {
					return nil
				}
				if _, dup := seenPrefix[cp]; !dup {
					// Look-ahead: a new, non-dup element found while the page is
					// already full proves there is more — truncate here and stop,
					// leaving nextKey at the already-emitted limit-th element.
					if len(out.contents)+len(out.commonPrefixes) >= limit {
						out.truncated = true
						return mst.ErrStopWalk
					}
					seenPrefix[cp] = struct{}{}
					cpCopy := cp
					out.commonPrefixes = append(out.commonPrefixes, types.CommonPrefix{Prefix: &cpCopy})
					out.nextKey = cp
				}
				return nil
			}
		}

		if len(out.contents)+len(out.commonPrefixes) >= limit {
			out.truncated = true
			return mst.ErrStopWalk
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
		out.nextKey = k
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

// objectKeyError validates an object key for storage and returns the S3 error
// to surface, or nil when the key is acceptable. A key over the size limit is
// reported as KeyTooLongError (matching AWS); other invalid keys (empty,
// non-UTF-8, NUL) are InvalidRequest.
func objectKeyError(key string) error {
	if len(key) > mst.MaxKeyBytes {
		return s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if !mst.IsValidKey(key) {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	return nil
}

// normalizeContentEncoding drops the aws-chunked transfer token(s) from a
// Content-Encoding value. aws-chunked marks a SigV4 streaming payload, not a
// real content encoding, so S3 does not persist it; a value consisting only of
// aws-chunked tokens normalizes to "" and is stored as no encoding.
func normalizeContentEncoding(enc string) string {
	if enc == "" {
		return ""
	}
	var kept []string
	for _, tok := range strings.Split(enc, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" || strings.EqualFold(tok, "aws-chunked") {
			continue
		}
		kept = append(kept, tok)
	}
	return strings.Join(kept, ", ")
}
