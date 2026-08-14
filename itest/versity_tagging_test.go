//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Object-tagging groups of the S3 conformance partition (upstream
// TestPutObjectTagging, TestGetObjectTagging, TestDeleteObjectTagging),
// curated per docs/s3-object-tagging.md §6. These categories run with the
// plain conf: their buckets are neither lock-enabled nor versioned, so the
// plain teardown applies. The versioned tagging behaviors are the
// Versioning_* rows in versity_versioning_test.go.

var putObjectTaggingPass = []forgeCase{
	{name: "non_existing_object", fn: integration.PutObjectTagging_non_existing_object},
	{name: "long_tags", fn: integration.PutObjectTagging_long_tags},
	{name: "duplicate_keys", fn: integration.PutObjectTagging_duplicate_keys},
	{name: "tag_count_limit", fn: integration.PutObjectTagging_tag_count_limit},
	{name: "invalid_tags", fn: integration.PutObjectTagging_invalid_tags},
	{name: "success", fn: integration.PutObjectTagging_success},
}

var getObjectTaggingPass = []forgeCase{
	{name: "non_existing_object", fn: integration.GetObjectTagging_non_existing_object},
	{name: "unset_tags", fn: integration.GetObjectTagging_unset_tags},
	{name: "invalid_parent", fn: integration.GetObjectTagging_invalid_parent},
	{name: "success", fn: integration.GetObjectTagging_success},
}

var deleteObjectTaggingPass = []forgeCase{
	{name: "non_existing_object", fn: integration.DeleteObjectTagging_non_existing_object},
	{name: "success_status", fn: integration.DeleteObjectTagging_success_status},
	{name: "success", fn: integration.DeleteObjectTagging_success},
}

var deleteObjectTaggingXFail = []forgeCase{
	// Sends x-amz-expected-bucket-owner with the tenant's own access key,
	// but the hilt flow substitutes the configured root access key as the
	// ACL owner (s3frontend/bucket.go GetBucketAcl), so the correct-owner
	// request 403s — the same owner-substitution surface as the
	// *_expected_owner xfails in the object tables.
	{name: "expected_bucket_owner", fn: integration.DeleteObjectTagging_expected_bucket_owner},
}
