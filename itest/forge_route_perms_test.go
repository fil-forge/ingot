//go:build itest

package itest

import (
	"bytes"
	"net/http"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestForgeRoutePermissions proves, through the real hilt, that a request
// with a subresource is authorized as the operation the gateway performs:
//
//   - a key holding s3:CreateBucket but none of the configuration
//     permissions is refused PutBucketVersioning and DeleteObjects on an
//     existing bucket, since each is authorized on its own permission and
//     against that bucket's scope;
//   - DeleteObjects is authorized on s3:DeleteObject, and a batch with an
//     entry naming a version is refused as a whole, nothing in it deleted,
//     unless the key also holds s3:DeleteObjectVersion: hilt decides the
//     request on its body, where S3 would refuse the entry alone.
//
// Each denied key first makes an allowed request, so the second attempt at a
// denial runs against ingot's caches and the local fast path must refuse
// what hilt refuses.
//
//	go test -tags itest ./itest -run TestForgeRoutePermissions -v -timeout 900s
func TestForgeRoutePermissions(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	const tenant, bucket = "route-perms", "versioned"
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, tenant)
	full := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))
	if _, err := full.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := full.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("PutBucketVersioning with the permission: %v", err)
	}
	put := func(key string) string {
		t.Helper()
		out, err := full.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(patternBytes(1 << 10))})
		if err != nil {
			t.Fatalf("PutObject %q: %v", key, err)
		}
		return aws.ToString(out.VersionId)
	}
	client := func(name string, permissions []string) *s3.Client {
		ak, sk := hiltCreateAccessKey(t, ctx, s, tenant, name, permissions, nil)
		return sdkClient(forgeS3Conf(endpoint, ak, sk))
	}
	denied := func(t *testing.T, what string, do func() error) {
		t.Helper()
		for attempt := 1; attempt <= 2; attempt++ {
			if code, status := apiErrorOf(t, do()); code != "AccessDenied" || status != http.StatusForbidden {
				t.Fatalf("%s (attempt %d): %s/%d, want AccessDenied/403", what, attempt, code, status)
			}
		}
	}
	deleteObjects := func(c *s3.Client, ids ...types.ObjectIdentifier) (*s3.DeleteObjectsOutput, error) {
		return c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: ids}})
	}

	t.Run("bucket create does not reach an existing bucket", func(t *testing.T) {
		creator := client("creator", []string{"s3:CreateBucket", "s3:ListAllMyBuckets"})
		if _, err := creator.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		denied(t, "create-only key suspending versioning", func() error {
			_, err := creator.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
				Bucket:                  aws.String(bucket),
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			return err
		})
		denied(t, "create-only key deleting objects", func() error {
			_, err := deleteObjects(creator, types.ObjectIdentifier{Key: aws.String("any")})
			return err
		})
	})

	t.Run("an entry naming a version needs the version permission", func(t *testing.T) {
		withoutVersion := slices.DeleteFunc(slices.Clone(hiltAllPermissions), func(p string) bool { return p == "s3:DeleteObjectVersion" })
		deleter := client("deleter", withoutVersion)
		v1 := put("kept")
		put("marked")
		// An allowed request first, so the key's action set and chains are
		// cached and the second denial below runs against them.
		if _, err := deleteObjects(deleter, types.ObjectIdentifier{Key: aws.String("probe")}); err != nil {
			t.Fatalf("priming DeleteObjects with s3:DeleteObject: %v", err)
		}

		// The mixed batch is refused as a whole, once through hilt and once
		// against the caches, with nothing in it deleted.
		denied(t, "delete-only key naming a version", func() error {
			_, err := deleteObjects(deleter,
				types.ObjectIdentifier{Key: aws.String("kept"), VersionId: aws.String(v1)},
				types.ObjectIdentifier{Key: aws.String("marked")},
			)
			return err
		})
		if _, err := full.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("kept"), VersionId: aws.String(v1)}); err != nil {
			t.Fatalf("refused version is gone: %v", err)
		}
		if _, err := full.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("marked")}); err != nil {
			t.Fatalf("\"marked\" was deleted by a refused batch: %v", err)
		}

		// The plain batch is authorized on s3:DeleteObject alone.
		out, err := deleteObjects(deleter, types.ObjectIdentifier{Key: aws.String("marked")})
		if err != nil {
			t.Fatalf("DeleteObjects with s3:DeleteObject: %v", err)
		}
		if len(out.Errors) != 0 || len(out.Deleted) != 1 || aws.ToString(out.Deleted[0].Key) != "marked" || !aws.ToBool(out.Deleted[0].DeleteMarker) {
			t.Fatalf("result = deleted %+v, errors %+v; want a delete marker for \"marked\"", out.Deleted, out.Errors)
		}

		versionDeleter := client("version-deleter", []string{"s3:DeleteObject", "s3:DeleteObjectVersion"})
		out, err = deleteObjects(versionDeleter, types.ObjectIdentifier{Key: aws.String("kept"), VersionId: aws.String(v1)})
		if err != nil {
			t.Fatalf("DeleteObjects with s3:DeleteObjectVersion: %v", err)
		}
		if len(out.Errors) != 0 || len(out.Deleted) != 1 || aws.ToString(out.Deleted[0].VersionId) != v1 {
			t.Fatalf("result = deleted %+v, errors %+v; want version %s deleted", out.Deleted, out.Errors, v1)
		}
	})
}
