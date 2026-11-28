package s3frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNormalizeContentEncoding locks S3's aws-chunked handling: the streaming
// transfer marker is stripped from the stored Content-Encoding, and a value
// consisting only of aws-chunked tokens is stored as no encoding at all.
func TestNormalizeContentEncoding(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"gzip, aws-chunked", "gzip"},
		{"aws-chunked, gzip", "gzip"},
		{"aws-chunked", ""},
		{"aws-chunked, aws-chunked", ""},
		{"AWS-Chunked", ""},
		{"gzip, AWS-CHUNKED", "gzip"},
		{"gzip, deflate, aws-chunked", "gzip, deflate"},
		{"gzip", "gzip"},
		{"", ""},
	}
	for _, c := range cases {
		got := normalizeContentEncoding(c.in)
		assert.Equal(t, c.want, got, "normalizeContentEncoding(%q)", c.in)
	}
}
