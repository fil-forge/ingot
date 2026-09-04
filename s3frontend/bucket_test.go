package s3frontend

import (
	"strings"
	"testing"
)

// TestValidBucketName pins the AWS general-purpose bucket naming rules,
// including every name the s3vectors bucket-0038 vector expects rejected.
func TestValidBucketName(t *testing.T) {
	reject := []struct{ name, why string }{
		{"ABmsstbucket", "uppercase"},
		{"my_bucket_msst", "underscore"},
		{"my-bucket-msst-", "trailing hyphen"},
		{"-my-bucket-msst", "leading hyphen"},
		{"my..bucket-msst", "consecutive dots"},
		{"my.-bucket-msst", "dot then hyphen"},
		{"my-.bucket-msst", "hyphen then dot"},
		{"192.168.1.1", "IPv4 form"},
		{"xn--bucket-msst", "reserved xn-- prefix"},
		{"bucket-s3alias", "reserved -s3alias suffix"},
		{"bucket--ol-s3", "reserved --ol-s3 suffix"},
		{"aa", "too short (2)"},
		{strings.Repeat("a", 64), "too long (64)"},
		{"my.bucket.", "trailing dot"},
		{".my.bucket", "leading dot"},
		{"sthree-bucket", "reserved sthree- prefix"},
	}
	for _, tc := range reject {
		if validBucketName(tc.name) {
			t.Errorf("validBucketName(%q) = true, want false (%s)", tc.name, tc.why)
		}
	}

	accept := []string{
		"abc",
		"my-bucket",
		"my-bucket-msst",
		"my.bucket.1",
		"my.bucket-1",
		"1.2.3",                    // three labels: not an IPv4 address
		"bucket-with-s3alias-word", // "s3alias" only as a suffix is reserved
		strings.Repeat("a", 63),
	}
	for _, name := range accept {
		if !validBucketName(name) {
			t.Errorf("validBucketName(%q) = false, want true", name)
		}
	}
}
