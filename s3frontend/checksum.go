package s3frontend

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// checksumSpec describes the additional S3 checksum to compute over an object
// body on PUT: the algorithm, the HashReader type that computes it, and the
// client-supplied expected value ("" when the client only named an algorithm
// and wants the server to compute it).
type checksumSpec struct {
	algo     types.ChecksumAlgorithm
	hashType utils.HashType
	expected string
}

// checksumFromInput derives the checksum to compute from a PutObject request.
// An explicit x-amz-checksum-<alg> value takes precedence (and is validated);
// otherwise x-amz-checksum-algorithm selects the algorithm to compute and echo.
// When the request names no checksum at all, the server falls back to a
// full-object CRC64NVME — S3's default-checksum-on-store behavior, which clients
// (and the SDK) expect to read back even when they sent nothing. That fallback
// is computed, not validated, since there is no client value. So this never
// returns a nil spec: every stored object carries at least a CRC64NVME.
func checksumFromInput(in s3response.PutObjectInput) (*checksumSpec, error) {
	switch {
	case in.ChecksumSHA256 != nil:
		return &checksumSpec{types.ChecksumAlgorithmSha256, utils.HashTypeSha256, *in.ChecksumSHA256}, nil
	case in.ChecksumCRC32 != nil:
		return &checksumSpec{types.ChecksumAlgorithmCrc32, utils.HashTypeCRC32, *in.ChecksumCRC32}, nil
	case in.ChecksumCRC32C != nil:
		return &checksumSpec{types.ChecksumAlgorithmCrc32c, utils.HashTypeCRC32C, *in.ChecksumCRC32C}, nil
	case in.ChecksumSHA1 != nil:
		return &checksumSpec{types.ChecksumAlgorithmSha1, utils.HashTypeSha1, *in.ChecksumSHA1}, nil
	case in.ChecksumCRC64NVME != nil:
		return &checksumSpec{types.ChecksumAlgorithmCrc64nvme, utils.HashTypeCRC64NVME, *in.ChecksumCRC64NVME}, nil
	case in.ChecksumSHA512 != nil:
		return &checksumSpec{types.ChecksumAlgorithmSha512, utils.HashTypeSha512, *in.ChecksumSHA512}, nil
	case in.ChecksumMD5 != nil:
		return &checksumSpec{types.ChecksumAlgorithmMd5, utils.HashTypeMd5, *in.ChecksumMD5}, nil
	case in.ChecksumXXHASH64 != nil:
		return &checksumSpec{types.ChecksumAlgorithmXxhash64, utils.HashTypeXXHASH64, *in.ChecksumXXHASH64}, nil
	case in.ChecksumXXHASH3 != nil:
		return &checksumSpec{types.ChecksumAlgorithmXxhash3, utils.HashTypeXXHASH3, *in.ChecksumXXHASH3}, nil
	case in.ChecksumXXHASH128 != nil:
		return &checksumSpec{types.ChecksumAlgorithmXxhash128, utils.HashTypeXXHASH128, *in.ChecksumXXHASH128}, nil
	case in.ChecksumAlgorithm != "":
		ht, err := hashTypeForAlgo(in.ChecksumAlgorithm)
		if err != nil {
			return nil, err
		}
		return &checksumSpec{in.ChecksumAlgorithm, ht, ""}, nil
	default:
		// No client checksum named → server-computed full-object CRC64NVME.
		return &checksumSpec{types.ChecksumAlgorithmCrc64nvme, utils.HashTypeCRC64NVME, ""}, nil
	}
}

func hashTypeForAlgo(algo types.ChecksumAlgorithm) (utils.HashType, error) {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return utils.HashTypeCRC32, nil
	case types.ChecksumAlgorithmCrc32c:
		return utils.HashTypeCRC32C, nil
	case types.ChecksumAlgorithmSha1:
		return utils.HashTypeSha1, nil
	case types.ChecksumAlgorithmSha256:
		return utils.HashTypeSha256, nil
	case types.ChecksumAlgorithmCrc64nvme:
		return utils.HashTypeCRC64NVME, nil
	case types.ChecksumAlgorithmSha512:
		return utils.HashTypeSha512, nil
	case types.ChecksumAlgorithmMd5:
		return utils.HashTypeMd5, nil
	case types.ChecksumAlgorithmXxhash64:
		return utils.HashTypeXXHASH64, nil
	case types.ChecksumAlgorithmXxhash3:
		return utils.HashTypeXXHASH3, nil
	case types.ChecksumAlgorithmXxhash128:
		return utils.HashTypeXXHASH128, nil
	default:
		return utils.HashTypeNone, utils.IsChecksumAlgorithmValid(algo)
	}
}

// checksumFields maps a stored (algorithm, base64 value, checksum type) to the
// per-algorithm response pointers + the checksum type. All nil / empty type
// when there is no checksum, so callers can assign the result unconditionally.
// An empty stored type means the block predates type recording, when every
// checksum was full-object.
func checksumFields(algo, val, ctypeStored string) (crc32, crc32c, sha1, sha256, crc64, sha512, md5, xxh64, xxh3, xxh128 *string, ctype types.ChecksumType) {
	if algo == "" || val == "" {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, ""
	}
	ctype = types.ChecksumType(ctypeStored)
	if ctype == "" {
		ctype = types.ChecksumTypeFullObject
	}
	v := val
	switch types.ChecksumAlgorithm(algo) {
	case types.ChecksumAlgorithmCrc32:
		crc32 = &v
	case types.ChecksumAlgorithmCrc32c:
		crc32c = &v
	case types.ChecksumAlgorithmSha1:
		sha1 = &v
	case types.ChecksumAlgorithmSha256:
		sha256 = &v
	case types.ChecksumAlgorithmCrc64nvme:
		crc64 = &v
	case types.ChecksumAlgorithmSha512:
		sha512 = &v
	case types.ChecksumAlgorithmMd5:
		md5 = &v
	case types.ChecksumAlgorithmXxhash64:
		xxh64 = &v
	case types.ChecksumAlgorithmXxhash3:
		xxh3 = &v
	case types.ChecksumAlgorithmXxhash128:
		xxh128 = &v
	}
	return crc32, crc32c, sha1, sha256, crc64, sha512, md5, xxh64, xxh3, xxh128, ctype
}

// partChecksumFromInput derives the checksum an UploadPart request carries: an
// explicit x-amz-checksum-<alg> value wins; otherwise x-amz-checksum-algorithm
// names the algorithm with no precomputed value ("") — the trailing-checksum
// (aws-chunked) form, where the chunk reader has already validated the trailer
// and the backend recomputes the digest itself. Both empty when the request
// names no checksum.
func partChecksumFromInput(input *s3.UploadPartInput) (types.ChecksumAlgorithm, string) {
	for _, c := range []struct {
		algo  types.ChecksumAlgorithm
		value *string
	}{
		{types.ChecksumAlgorithmCrc32, input.ChecksumCRC32},
		{types.ChecksumAlgorithmCrc32c, input.ChecksumCRC32C},
		{types.ChecksumAlgorithmSha1, input.ChecksumSHA1},
		{types.ChecksumAlgorithmSha256, input.ChecksumSHA256},
		{types.ChecksumAlgorithmCrc64nvme, input.ChecksumCRC64NVME},
		{types.ChecksumAlgorithmSha512, input.ChecksumSHA512},
		{types.ChecksumAlgorithmMd5, input.ChecksumMD5},
		{types.ChecksumAlgorithmXxhash64, input.ChecksumXXHASH64},
		{types.ChecksumAlgorithmXxhash3, input.ChecksumXXHASH3},
		{types.ChecksumAlgorithmXxhash128, input.ChecksumXXHASH128},
	} {
		if c.value != nil {
			return c.algo, *c.value
		}
	}
	return input.ChecksumAlgorithm, ""
}

// setUploadPartChecksum sets the response pointer matching algo on an
// UploadPart output. No-op for an empty algo or sum.
func setUploadPartChecksum(out *s3.UploadPartOutput, algo types.ChecksumAlgorithm, sum string) {
	if algo == "" || sum == "" {
		return
	}
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		out.ChecksumCRC32 = &sum
	case types.ChecksumAlgorithmCrc32c:
		out.ChecksumCRC32C = &sum
	case types.ChecksumAlgorithmSha1:
		out.ChecksumSHA1 = &sum
	case types.ChecksumAlgorithmSha256:
		out.ChecksumSHA256 = &sum
	case types.ChecksumAlgorithmCrc64nvme:
		out.ChecksumCRC64NVME = &sum
	case types.ChecksumAlgorithmSha512:
		out.ChecksumSHA512 = &sum
	case types.ChecksumAlgorithmMd5:
		out.ChecksumMD5 = &sum
	case types.ChecksumAlgorithmXxhash64:
		out.ChecksumXXHASH64 = &sum
	case types.ChecksumAlgorithmXxhash3:
		out.ChecksumXXHASH3 = &sum
	case types.ChecksumAlgorithmXxhash128:
		out.ChecksumXXHASH128 = &sum
	}
}

// setResponsePartChecksum sets the per-part pointer matching algo on a
// ListParts response entry. No-op for an empty algo or sum.
func setResponsePartChecksum(p *s3response.Part, algo types.ChecksumAlgorithm, sum string) {
	if algo == "" || sum == "" {
		return
	}
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		p.ChecksumCRC32 = &sum
	case types.ChecksumAlgorithmCrc32c:
		p.ChecksumCRC32C = &sum
	case types.ChecksumAlgorithmSha1:
		p.ChecksumSHA1 = &sum
	case types.ChecksumAlgorithmSha256:
		p.ChecksumSHA256 = &sum
	case types.ChecksumAlgorithmCrc64nvme:
		p.ChecksumCRC64NVME = &sum
	case types.ChecksumAlgorithmSha512:
		p.ChecksumSHA512 = &sum
	case types.ChecksumAlgorithmMd5:
		p.ChecksumMD5 = &sum
	case types.ChecksumAlgorithmXxhash64:
		p.ChecksumXXHASH64 = &sum
	case types.ChecksumAlgorithmXxhash3:
		p.ChecksumXXHASH3 = &sum
	case types.ChecksumAlgorithmXxhash128:
		p.ChecksumXXHASH128 = &sum
	}
}

// setCompleteResultChecksum sets the final checksum + type on a
// CompleteMultipartUpload result. No-op for an empty algo or sum.
func setCompleteResultChecksum(res *s3response.CompleteMultipartUploadResult, algo types.ChecksumAlgorithm, sum string, ctype types.ChecksumType) {
	if algo == "" || sum == "" {
		return
	}
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		res.ChecksumCRC32 = &sum
	case types.ChecksumAlgorithmCrc32c:
		res.ChecksumCRC32C = &sum
	case types.ChecksumAlgorithmSha1:
		res.ChecksumSHA1 = &sum
	case types.ChecksumAlgorithmSha256:
		res.ChecksumSHA256 = &sum
	case types.ChecksumAlgorithmCrc64nvme:
		res.ChecksumCRC64NVME = &sum
	case types.ChecksumAlgorithmSha512:
		res.ChecksumSHA512 = &sum
	case types.ChecksumAlgorithmMd5:
		res.ChecksumMD5 = &sum
	case types.ChecksumAlgorithmXxhash64:
		res.ChecksumXXHASH64 = &sum
	case types.ChecksumAlgorithmXxhash3:
		res.ChecksumXXHASH3 = &sum
	case types.ChecksumAlgorithmXxhash128:
		res.ChecksumXXHASH128 = &sum
	default:
		return
	}
	res.ChecksumType = &ctype
}

// finalChecksumFromInput returns the algo's whole-upload checksum from a
// Complete request ("" when the request doesn't carry one).
func finalChecksumFromInput(algo types.ChecksumAlgorithm, input *s3.CompleteMultipartUploadInput) string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return backend.GetStringFromPtr(input.ChecksumCRC32)
	case types.ChecksumAlgorithmCrc32c:
		return backend.GetStringFromPtr(input.ChecksumCRC32C)
	case types.ChecksumAlgorithmSha1:
		return backend.GetStringFromPtr(input.ChecksumSHA1)
	case types.ChecksumAlgorithmSha256:
		return backend.GetStringFromPtr(input.ChecksumSHA256)
	case types.ChecksumAlgorithmCrc64nvme:
		return backend.GetStringFromPtr(input.ChecksumCRC64NVME)
	case types.ChecksumAlgorithmSha512:
		return backend.GetStringFromPtr(input.ChecksumSHA512)
	case types.ChecksumAlgorithmMd5:
		return backend.GetStringFromPtr(input.ChecksumMD5)
	case types.ChecksumAlgorithmXxhash64:
		return backend.GetStringFromPtr(input.ChecksumXXHASH64)
	case types.ChecksumAlgorithmXxhash3:
		return backend.GetStringFromPtr(input.ChecksumXXHASH3)
	case types.ChecksumAlgorithmXxhash128:
		return backend.GetStringFromPtr(input.ChecksumXXHASH128)
	default:
		return ""
	}
}

// validatePartChecksum checks one CompletedPart entry of a Complete request
// against the part's stored checksum (mirroring the upstream posix backend).
// sessAlgo/stored are the session's declared algorithm and the part's persisted
// value; both empty for a session that declared no checksum. The caller has
// already verified part.PartNumber and part.ETag are non-nil.
func validatePartChecksum(sessAlgo types.ChecksumAlgorithm, stored string, part types.CompletedPart, uploadID string) error {
	n, argValue := numberOfChecksums(part)
	if n > 1 {
		return s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, argValue)
	}
	if sessAlgo == "" {
		if n != 0 {
			return s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		return nil
	}
	if n == 0 {
		return s3err.APIError{
			Code:           "InvalidRequest",
			Description:    fmt.Sprintf("The upload was created using a %v checksum. The complete request must include the checksum for each part. It was missing for part %v in the request.", strings.ToLower(string(sessAlgo)), *part.PartNumber),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	for _, cs := range []struct {
		value *string
		algo  types.ChecksumAlgorithm
	}{
		{part.ChecksumCRC32, types.ChecksumAlgorithmCrc32},
		{part.ChecksumCRC32C, types.ChecksumAlgorithmCrc32c},
		{part.ChecksumSHA1, types.ChecksumAlgorithmSha1},
		{part.ChecksumSHA256, types.ChecksumAlgorithmSha256},
		{part.ChecksumCRC64NVME, types.ChecksumAlgorithmCrc64nvme},
		{part.ChecksumSHA512, types.ChecksumAlgorithmSha512},
		{part.ChecksumMD5, types.ChecksumAlgorithmMd5},
		{part.ChecksumXXHASH64, types.ChecksumAlgorithmXxhash64},
		{part.ChecksumXXHASH3, types.ChecksumAlgorithmXxhash3},
		{part.ChecksumXXHASH128, types.ChecksumAlgorithmXxhash128},
	} {
		if cs.value == nil {
			continue
		}
		if !utils.IsValidChecksum(*cs.value, cs.algo) {
			return s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, *cs.value)
		}
		// The expected value for the session's algorithm is the stored part
		// checksum; any other algorithm has no stored counterpart, so a
		// supplied value can only match if it is empty (it isn't — nil was
		// skipped above).
		expected := ""
		if cs.algo == sessAlgo {
			expected = stored
		}
		if *cs.value != expected {
			if cs.algo == sessAlgo {
				return s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
			}
			return s3err.APIError{
				Code:           "BadDigest",
				Description:    fmt.Sprintf("The %v you specified for part %v did not match what we received.", strings.ToLower(string(cs.algo)), *part.PartNumber),
				HTTPStatusCode: http.StatusBadRequest,
			}
		}
	}
	return nil
}

// numberOfChecksums counts the checksums a CompletedPart entry carries and
// renders the "ALGO:value;…" argument string the multiple-checksums
// InvalidArgument reports (the upstream error lists only the five core
// algorithms; the rest count toward the total without appearing).
func numberOfChecksums(part types.CompletedPart) (int, string) {
	count := 0
	b := &strings.Builder{}
	for _, c := range []struct {
		algo  types.ChecksumAlgorithm
		value *string
	}{
		{types.ChecksumAlgorithmCrc32, part.ChecksumCRC32},
		{types.ChecksumAlgorithmCrc32c, part.ChecksumCRC32C},
		{types.ChecksumAlgorithmCrc64nvme, part.ChecksumCRC64NVME},
		{types.ChecksumAlgorithmSha1, part.ChecksumSHA1},
		{types.ChecksumAlgorithmSha256, part.ChecksumSHA256},
	} {
		if backend.GetStringFromPtr(c.value) != "" {
			count++
			fmt.Fprintf(b, "%s:%s;", string(c.algo), *c.value)
		}
	}
	for _, v := range []*string{part.ChecksumSHA512, part.ChecksumMD5, part.ChecksumXXHASH64, part.ChecksumXXHASH3, part.ChecksumXXHASH128} {
		if backend.GetStringFromPtr(v) != "" {
			count++
		}
	}
	return count, b.String()
}
