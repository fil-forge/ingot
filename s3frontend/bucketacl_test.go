package s3frontend

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestUnsupportedBucketACL(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name string
		in   *s3.CreateBucketInput
		want bool
	}{
		{"no acl (default)", &s3.CreateBucketInput{}, false},
		{"explicit private", &s3.CreateBucketInput{ACL: types.BucketCannedACLPrivate}, false},
		{"public-read canned", &s3.CreateBucketInput{ACL: types.BucketCannedACLPublicRead}, true},
		{"authenticated-read canned", &s3.CreateBucketInput{ACL: types.BucketCannedACLAuthenticatedRead}, true},
		{"grant-read", &s3.CreateBucketInput{GrantRead: str("id=abc")}, true},
		{"grant-full-control", &s3.CreateBucketInput{GrantFullControl: str("id=abc")}, true},
		{"grant-write-acp", &s3.CreateBucketInput{GrantWriteACP: str("id=abc")}, true},
		{"private plus a grant is still unsupported", &s3.CreateBucketInput{ACL: types.BucketCannedACLPrivate, GrantWrite: str("id=abc")}, true},
		{"empty grant pointer is ignored", &s3.CreateBucketInput{GrantRead: str("")}, false},
	}
	for _, c := range cases {
		if got := unsupportedBucketACL(c.in); got != c.want {
			t.Errorf("%s: unsupportedBucketACL = %v, want %v", c.name, got, c.want)
		}
	}
}


func TestUnsupportedObjectACL(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name  string
		acl   types.ObjectCannedACL
		grant []*string
		want  bool
	}{
		{"no acl, no grants", "", nil, false},
		{"empty grant pointers", "", []*string{str(""), nil}, false},
		{"explicit private is rejected too", types.ObjectCannedACLPrivate, nil, true},
		{"public-read canned", types.ObjectCannedACLPublicRead, nil, true},
		{"bucket-owner-full-control canned", types.ObjectCannedACLBucketOwnerFullControl, nil, true},
		{"grant-read", "", []*string{str("id=abc")}, true},
		{"grant plus no acl", "", []*string{nil, str("id=abc")}, true},
	}
	for _, c := range cases {
		if got := unsupportedObjectACL(c.acl, c.grant...); got != c.want {
			t.Errorf("%s: unsupportedObjectACL = %v, want %v", c.name, got, c.want)
		}
	}
}


func TestRequestsServerSideEncryption(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{"no sse headers", map[string]string{"Content-Type": "text/plain", "X-Amz-Acl": "private"}, false},
		{"sse-s3/kms selector", map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms"}, true},
		{"sse kms key id", map[string]string{"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "key"}, true},
		{"sse context", map[string]string{"X-Amz-Server-Side-Encryption-Context": "ctx"}, true},
		{"sse bucket-key-enabled", map[string]string{"X-Amz-Server-Side-Encryption-Bucket-Key-Enabled": "true"}, true},
		{"sse-c algorithm", map[string]string{"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256"}, true},
		{"sse-c key", map[string]string{"X-Amz-Server-Side-Encryption-Customer-Key": "base64key"}, true},
		{"copy-source sse-c", map[string]string{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": "AES256"}, true},
		{"case-insensitive", map[string]string{"x-amz-server-side-encryption": "AES256"}, true},
	}
	for _, c := range cases {
		if got := requestsServerSideEncryption(c.headers); got != c.want {
			t.Errorf("%s: requestsServerSideEncryption = %v, want %v", c.name, got, c.want)
		}
	}
}
