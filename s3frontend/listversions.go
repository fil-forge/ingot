package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// ListObjectVersions walks every version of every key (docs/s3-versioning.md
// §9.2): keys in lexicographic order from the top MST, versions newest-first
// within each key (Current, then the prev tree's inverted-seq walk). Non-marker
// versions land in Versions, markers in DeleteMarkers; MaxKeys counts both.
// Pagination resumes strictly after the (KeyMarker, VersionIdMarker) pair; on
// truncation the Next markers name the last emitted entry.
func (b *Backend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	if input.Bucket == nil {
		return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucketName := *input.Bucket
	prefix := backend.GetStringFromPtr(input.Prefix)
	delimiter := backend.GetStringFromPtr(input.Delimiter)
	keyMarker := backend.GetStringFromPtr(input.KeyMarker)
	versionIDMarker := backend.GetStringFromPtr(input.VersionIdMarker)

	maxKeys := int32(0)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	limit := int(maxKeys)
	if limit <= 0 {
		limit = defaultMaxKeys
	}

	// A version-id marker must accompany a key marker and be well-formed.
	if versionIDMarker != "" {
		if kind, _ := classifyVersionID(versionIDMarker); keyMarker == "" || kind == versionKindInvalid {
			return s3response.ListVersionsResult{}, errInvalidVersionID(versionIDMarker)
		}
	}

	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return s3response.ListVersionsResult{}, err
	}

	res := s3response.ListVersionsResult{
		Name:      &bucketName,
		Prefix:    &prefix,
		Delimiter: &delimiter,
		MaxKeys:   &maxKeys,
	}
	if input.KeyMarker != nil {
		res.KeyMarker = input.KeyMarker
	}
	if input.VersionIdMarker != nil {
		res.VersionIdMarker = input.VersionIdMarker
	}
	truncated := false
	res.IsTruncated = &truncated
	if !st.Root.Defined() {
		return res, nil
	}

	// Resumption (§9.2): with both markers, restart AT the marker key and skip
	// within its version sequence; with only a key marker, start strictly after
	// it.
	from := prefix
	if keyMarker != "" && keyMarker >= from {
		if versionIDMarker == "" {
			from = keyMarker + "\x01"
		} else {
			from = keyMarker
		}
	}

	count := 0
	var lastKey, lastVersionID string
	full := func() bool { return count >= limit }

	t := mst.LoadMST(b.read, st.Space, st.Root)
	seenPrefix := map[string]struct{}{}
	walkErr := t.WalkLeavesFromNocache(ctx, from, func(k string, valCid cid.Cid) error {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			return mst.ErrStopWalk
		}

		// Delimiter grouping subsumes every version of the rolled-up keys. A
		// group emitted on an earlier page (its prefix ≤ the key marker) is
		// skipped whole.
		if delimiter != "" {
			tail := k[len(prefix):]
			if i := strings.Index(tail, delimiter); i >= 0 {
				cp := prefix + tail[:i+len(delimiter)]
				if keyMarker != "" && cp <= keyMarker {
					return nil
				}
				if _, dup := seenPrefix[cp]; !dup {
					seenPrefix[cp] = struct{}{}
					cpCopy := cp
					res.CommonPrefixes = append(res.CommonPrefixes, types.CommonPrefix{Prefix: &cpCopy})
					count++
					if full() {
						truncated = true
						lastKey, lastVersionID = cp, ""
						return mst.ErrStopWalk
					}
				}
				return nil
			}
		}

		var val msbucket.ObjectValue
		if err := b.read.Get(ctx, st.Space, valCid, &val); err != nil {
			return fmt.Errorf("value get %s: %w", valCid, err)
		}
		var leaf *msbucket.ObjectLeaf
		var current msbucket.VersionNode
		if val.Leaf != nil {
			leaf = val.Leaf
			current = leaf.Current
		} else {
			// Manifest-valued key (§2.1): its single version is the current one, and the
			// value block is already the manifest.
			current = msbucket.VersionNode{Seq: val.Manifest.Seq, VersionID: manifestVersionID(val.Manifest), Manifest: valCid}
		}

		// Versions of the marker key resume strictly after the marker version:
		// only entries with seq below the marker's are emitted. A token marker
		// needs no existence check: its seq is a *position* in the newest-first
		// order, and a position survives its version's deletion — everything at
		// or above it was emitted on earlier pages, so seeking below it is
		// exact continuation, never data loss. Only a null marker, whose id
		// encodes no position, falls back when it no longer resolves (its
		// version was deleted between pages): seqBelow stays MaxUint64 and the
		// key's versions are re-emitted — a duplicate page is possible, data
		// loss is not, and pagination still advances.
		seqBelow := uint64(math.MaxUint64)
		if k == keyMarker && versionIDMarker != "" {
			switch kind, seq := classifyVersionID(versionIDMarker); kind {
			case versionKindNull:
				if current.VersionID == registry.NullVersionID {
					seqBelow = current.Seq
				} else if leaf != nil && leaf.NullSeq != 0 {
					seqBelow = leaf.NullSeq
				}
			case versionKindToken:
				seqBelow = seq
			}
		}

		emit := func(node msbucket.VersionNode, mf *msbucket.ObjectManifest, isLatest bool) {
			key := k
			vid := node.VersionID
			lm := time.Unix(mf.Created, 0)
			latest := isLatest
			if mf.DeleteMarker {
				res.DeleteMarkers = append(res.DeleteMarkers, types.DeleteMarkerEntry{
					Key:          &key,
					VersionId:    &vid,
					IsLatest:     &latest,
					LastModified: &lm,
				})
			} else {
				etag := etagOf(mf)
				size := mf.Body.Size
				ov := s3response.ObjectVersion{
					Key:          &key,
					VersionId:    &vid,
					IsLatest:     &latest,
					ETag:         &etag,
					Size:         &size,
					LastModified: &lm,
					StorageClass: types.ObjectVersionStorageClassStandard,
				}
				if mf.ChecksumAlgorithm != "" && mf.Checksum != "" {
					ov.ChecksumAlgorithm = []types.ChecksumAlgorithm{types.ChecksumAlgorithm(mf.ChecksumAlgorithm)}
					ov.ChecksumType = types.ChecksumTypeFullObject
				}
				res.Versions = append(res.Versions, ov)
			}
			count++
			lastKey, lastVersionID = k, vid
		}

		if current.Seq < seqBelow {
			mf := val.Manifest
			if mf == nil {
				var em msbucket.EnvelopedManifest
				if err := b.read.Get(ctx, st.Space, current.Manifest, &em); err != nil {
					return fmt.Errorf("manifest get %s: %w", current.Manifest, err)
				}
				mf = em.Manifest
			}
			emit(current, mf, true)
			if full() {
				truncated = true
				return mst.ErrStopWalk
			}
		}

		if leaf == nil || leaf.Prev == nil {
			return nil
		}
		pt := mst.LoadMST(b.read, st.Space, *leaf.Prev)
		// The prev walk is newest-first (inverted-seq keys); resume strictly
		// after the marker version by starting just past its key. revSeqKey is
		// fixed-width, so appending "\x01" forms a bound that sorts after the
		// marker's key and before every other key.
		prevFrom := ""
		if seqBelow != math.MaxUint64 {
			prevFrom = revSeqKey(seqBelow) + "\x01"
		}
		stopped := false
		perr := pt.WalkLeavesFrom(ctx, prevFrom, func(_ string, mfCid cid.Cid) error {
			var em msbucket.EnvelopedManifest
			if err := b.read.Get(ctx, st.Space, mfCid, &em); err != nil {
				return fmt.Errorf("manifest get %s: %w", mfCid, err)
			}
			mf := em.Manifest
			emit(msbucket.VersionNode{Seq: mf.Seq, VersionID: mf.VersionID, Manifest: mfCid}, mf, false)
			if full() {
				stopped = true
				return mst.ErrStopWalk
			}
			return nil
		})
		if perr != nil {
			return perr
		}
		if stopped {
			truncated = true
			return mst.ErrStopWalk
		}
		return nil
	})
	if walkErr != nil {
		var apiErr s3err.APIError
		if errors.As(walkErr, &apiErr) {
			return s3response.ListVersionsResult{}, apiErr
		}
		return s3response.ListVersionsResult{}, fmt.Errorf("s3frontend: list versions walk: %w", walkErr)
	}

	if truncated {
		res.NextKeyMarker = &lastKey
		if lastVersionID != "" {
			res.NextVersionIdMarker = &lastVersionID
		}
	}
	return res, nil
}
