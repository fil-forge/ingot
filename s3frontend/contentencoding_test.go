package s3frontend

import "testing"

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
		if got := normalizeContentEncoding(c.in); got != c.want {
			t.Errorf("normalizeContentEncoding(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
