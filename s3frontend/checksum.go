package s3frontend

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3api/utils"
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

// setCopyPartChecksum sets the response pointer matching algo on an
// UploadPartCopy result. No-op for an empty algo or sum.
func setCopyPartChecksum(res *s3response.CopyPartResult, algo types.ChecksumAlgorithm, sum string) {
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

// completeChecksums returns the whole-upload checksums a Complete request
// carries, by algorithm.
func completeChecksums(input *s3.CompleteMultipartUploadInput) map[types.ChecksumAlgorithm]string {
	out := map[types.ChecksumAlgorithm]string{}
	for algo, v := range map[types.ChecksumAlgorithm]*string{
		types.ChecksumAlgorithmCrc32:     input.ChecksumCRC32,
		types.ChecksumAlgorithmCrc32c:    input.ChecksumCRC32C,
		types.ChecksumAlgorithmSha1:      input.ChecksumSHA1,
		types.ChecksumAlgorithmSha256:    input.ChecksumSHA256,
		types.ChecksumAlgorithmCrc64nvme: input.ChecksumCRC64NVME,
		types.ChecksumAlgorithmSha512:    input.ChecksumSHA512,
		types.ChecksumAlgorithmMd5:       input.ChecksumMD5,
		types.ChecksumAlgorithmXxhash64:  input.ChecksumXXHASH64,
		types.ChecksumAlgorithmXxhash3:   input.ChecksumXXHASH3,
		types.ChecksumAlgorithmXxhash128: input.ChecksumXXHASH128,
	} {
		if v != nil && *v != "" {
			out[algo] = *v
		}
	}
	return out
}
