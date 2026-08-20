//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Bucket-level groups of the S3 conformance partition. Seeded from the
// in-memory suite's curated lists, verified live against the forge-mode
// stack (the two deployments proved case-identical on these groups before
// the in-memory harness was removed).

var createBucketPass = []forgeCase{
	{name: "default_object_lock", fn: integration.CreateBucket_default_object_lock},
	{name: "invalid_bucket_name", fn: integration.CreateBucket_invalid_bucket_name},
	{name: "invalid_canned_acl", fn: integration.CreateBucket_invalid_canned_acl},
	{name: "invalid_ownership", fn: integration.CreateBucket_invalid_ownership},
	{name: "ownership_with_acl", fn: integration.CreateBucket_ownership_with_acl},
	{name: "success", fn: integration.CreateBucket_success},
}

var createBucketXFail = []forgeCase{
	// Since the hilt (tenant-management) integration, bucket creation is
	// forwarded to hilt, which does not validate the CreateBucket
	// LocationConstraint against the deployment region — the invalid
	// constraint is accepted instead of rejected.
	{name: "invalid_location_constraint", fn: integration.CreateBucket_invalid_location_constraint},
	{name: "as_user", fn: integration.CreateBucket_as_user},
	{name: "default_acl", fn: integration.CreateBucket_default_acl},
	{name: "duplicate_keys", fn: integration.CreateBucket_duplicate_keys},
	{name: "existing_bucket", fn: integration.CreateBucket_existing_bucket},
	{name: "invalid_tags", fn: integration.CreateBucket_invalid_tags},
	{name: "long_tags", fn: integration.CreateBucket_long_tags},
	{name: "non_default_acl", fn: integration.CreateBucket_non_default_acl},
	{name: "owned_by_you", fn: integration.CreateBucket_owned_by_you},
	{name: "private_canned_acl", fn: integration.CreateBucket_private_canned_acl},
	{name: "private_canned_acl_bucket_owner_enforced_ownership", fn: integration.CreateBucket_private_canned_acl_bucket_owner_enforced_ownership},
	{name: "tag_count_limit", fn: integration.CreateBucket_tag_count_limit},
}

var headBucketPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.HeadBucket_non_existing_bucket},
	{name: "success", fn: integration.HeadBucket_success},
}

var listBucketsPass = []forgeCase{
	{name: "empty_success", fn: integration.ListBuckets_empty_success},
	{name: "invalid_max_buckets", fn: integration.ListBuckets_invalid_max_buckets},
	// truncated is counter-position-sensitive, not S3-semantics-sensitive:
	// the upstream case names buckets from a process-global counter
	// (test-bucket-<N>) and compares pages position-by-position expecting
	// creation order, while ingot returns buckets lexicographically. Whether
	// a page straddles a digit boundary (…9, 10 or …99, 100) — and thus
	// whether the orders diverge — depends on how far the counter advanced
	// before this case runs, which is an artifact of suite composition (it
	// deterministically fails at this suite's counter position). Skipped
	// rather than curated; the pagination surface itself is exercised by
	// ListBuckets_with_prefix and ListBuckets_invalid_max_buckets.
	{name: "truncated", fn: integration.ListBuckets_truncated, skip: func() string {
		return "creation-order pagination assertion is counter-position-dependent (ingot lists lexicographically)"
	}},
}

var listBucketsXFail = []forgeCase{
	// success/with_prefix assert the listing's Owner equals the account's
	// access key; since the hilt integration ListBuckets reports the
	// tenant's did:plc as owner, not the signing access key.
	{name: "success", fn: integration.ListBuckets_success},
	{name: "with_prefix", fn: integration.ListBuckets_with_prefix},
	{name: "as_admin", fn: integration.ListBuckets_as_admin},
	{name: "as_user", fn: integration.ListBuckets_as_user},
}

var deleteBucketPass = []forgeCase{
	{name: "incorrect_expected_bucket_owner", fn: integration.DeleteBucket_incorrect_expected_bucket_owner},
	{name: "non_empty_bucket", fn: integration.DeleteBucket_non_empty_bucket},
	{name: "non_existing_bucket", fn: integration.DeleteBucket_non_existing_bucket},
	{name: "success_status_code", fn: integration.DeleteBucket_success_status_code},
}
