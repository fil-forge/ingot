package multipartqueue

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3api/utils"
)

// checkPartChecksum checks one part entry of a Complete request against the
// part's stored checksum (mirroring the upstream posix backend). sessAlgo and
// stored are the session's declared algorithm and the part's persisted value;
// both empty for a session that declared no checksum. The caller has already
// checked that part.PartNumber and part.ETag are set.
func checkPartChecksum(sessAlgo types.ChecksumAlgorithm, stored string, part types.CompletedPart, uploadID string) error {
	perr := func(kind string) *PartError {
		return &PartError{Kind: kind, UploadID: uploadID, PartNumber: *part.PartNumber, ETag: *part.ETag}
	}
	n, argValue := numberOfChecksums(part)
	if n > 1 {
		e := perr(PartChecksumInvalid)
		e.Arg = argValue
		return e
	}
	if sessAlgo == "" {
		if n != 0 {
			return perr(PartNotMatched)
		}
		return nil
	}
	if n == 0 {
		e := perr(PartChecksumMissing)
		e.Algorithm = sessAlgo
		return e
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
			e := perr(PartChecksumInvalid)
			e.Arg = *cs.value
			return e
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
				return perr(PartNotMatched)
			}
			e := perr(PartChecksumMismatch)
			e.Algorithm = cs.algo
			return e
		}
	}
	return nil
}

// numberOfChecksums counts the checksums a part entry carries and renders
// the "ALGO:value;…" argument string the multiple-checksums error reports
// (the upstream error lists only the five core algorithms; the rest count
// toward the total without appearing).
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
		if c.value != nil && *c.value != "" {
			count++
			fmt.Fprintf(b, "%s:%s;", string(c.algo), *c.value)
		}
	}
	for _, v := range []*string{part.ChecksumSHA512, part.ChecksumMD5, part.ChecksumXXHASH64, part.ChecksumXXHASH3, part.ChecksumXXHASH128} {
		if v != nil && *v != "" {
			count++
		}
	}
	return count, b.String()
}
