package s3frontend

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3response"
)

// TestChecksumFromInput_DefaultsToCRC64NVME locks S3's default-checksum-on-store
// behavior: a PutObject that names no checksum still computes (without
// validating) a full-object CRC64NVME, while an explicitly named algorithm or
// value takes precedence.
func TestChecksumFromInput_DefaultsToCRC64NVME(t *testing.T) {
	sha256 := "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
	algoOnly := types.ChecksumAlgorithmCrc32

	cases := []struct {
		name         string
		in           s3response.PutObjectInput
		wantAlgo     types.ChecksumAlgorithm
		wantHash     utils.HashType
		wantExpected string
	}{
		{
			name:         "no checksum → default crc64nvme (compute, no validation)",
			in:           s3response.PutObjectInput{},
			wantAlgo:     types.ChecksumAlgorithmCrc64nvme,
			wantHash:     utils.HashTypeCRC64NVME,
			wantExpected: "",
		},
		{
			name:         "explicit value wins over the default",
			in:           s3response.PutObjectInput{ChecksumSHA256: &sha256},
			wantAlgo:     types.ChecksumAlgorithmSha256,
			wantHash:     utils.HashTypeSha256,
			wantExpected: sha256,
		},
		{
			name:         "named algorithm (no value) wins over the default",
			in:           s3response.PutObjectInput{ChecksumAlgorithm: algoOnly},
			wantAlgo:     types.ChecksumAlgorithmCrc32,
			wantHash:     utils.HashTypeCRC32,
			wantExpected: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := checksumFromInput(tc.in)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if spec == nil {
				t.Fatal("spec = nil, want non-nil (every object carries at least a CRC64NVME)")
			}
			if spec.algo != tc.wantAlgo || spec.hashType != tc.wantHash || spec.expected != tc.wantExpected {
				t.Fatalf("got {algo=%s hash=%v expected=%q}, want {algo=%s hash=%v expected=%q}",
					spec.algo, spec.hashType, spec.expected, tc.wantAlgo, tc.wantHash, tc.wantExpected)
			}
		})
	}
}
