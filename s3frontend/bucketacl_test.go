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
