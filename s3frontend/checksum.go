package s3frontend

import (
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

// checksumFields maps a stored (algorithm, base64 value) to the per-algorithm
// response pointers + the checksum type. All nil / empty type when there is no
// checksum, so callers can assign the result unconditionally. Single-part
// objects are always full-object checksums.
func checksumFields(algo, val string) (crc32, crc32c, sha1, sha256, crc64, sha512, md5, xxh64, xxh3, xxh128 *string, ctype types.ChecksumType) {
	if algo == "" || val == "" {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, ""
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
	return crc32, crc32c, sha1, sha256, crc64, sha512, md5, xxh64, xxh3, xxh128, types.ChecksumTypeFullObject
}
