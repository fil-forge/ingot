//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Single-object groups of the S3 conformance partition.
//
// Historical note: before DeleteObject released network blobs (FIL-588),
// a bucket that ever held a non-empty object body could not be deleted —
// hilt's /s3/bucket/delete refuses non-empty spaces (409 BucketNotEmpty) —
// so every such case failed its bucket-delete teardown and sat in the
// XFail tables as "teardown-blocked". The unexpected-pass ratchet flagged
// them all when the release path landed on this branch; they now live in
// the pass tables.

var putObjectPass = []forgeCase{
	{name: "checksum_algorithm_and_header_mismatch", fn: integration.PutObject_checksum_algorithm_and_header_mismatch},
	// Overwrites superseding checksummed bodies release their blobs: needs
	// hilt ≥ #37 (blob.Remove in the write set) and smelt ≥ #19 (the piri
	// blob/release delegation).
	{name: "checksums_success", fn: integration.PutObject_checksums_success},
	{name: "dir_object_default_checksum", fn: integration.PutObject_dir_object_default_checksum},
	{name: "dir_object_checksums_success", fn: integration.PutObject_dir_object_checksums_success},
	{name: "incorrect_checksums", fn: integration.PutObject_incorrect_checksums},
	{name: "invalid_credentials", fn: integration.PutObject_invalid_credentials},
	{name: "false_negative_object_names", fn: integration.PutObject_false_negative_object_names},
	{name: "invalid_checksum_header", fn: integration.PutObject_invalid_checksum_header},
	{name: "invalid_legal_hold", fn: integration.PutObject_invalid_legal_hold},
	{name: "invalid_object_lock_mode", fn: integration.PutObject_invalid_object_lock_mode},
	{name: "invalid_object_names", fn: integration.PutObject_invalid_object_names},
	{name: "invalid_retain_until_date", fn: integration.PutObject_invalid_retain_until_date},
	{name: "invalid_website_redirect_location", fn: integration.PutObject_invalid_website_redirect_location},
	{name: "long_metadata", fn: integration.PutObject_long_metadata},
	{name: "missing_bucket_lock", fn: integration.PutObject_missing_bucket_lock},
	{name: "missing_object_lock_retention_config", fn: integration.PutObject_missing_object_lock_retention_config},
	{name: "multiple_checksum_headers", fn: integration.PutObject_multiple_checksum_headers},
	{name: "non_existing_bucket", fn: integration.PutObject_non_existing_bucket},
	{name: "past_retain_until_date", fn: integration.PutObject_past_retain_until_date},
	// racey_success creates its bucket lock-enabled by hand and needs the
	// versioned teardown: it runs as LockCreation/PutObject_racey_success.
	{name: "special_chars", fn: integration.PutObject_special_chars},
	{name: "conditional_writes", fn: integration.PutObject_conditional_writes},
	{name: "default_checksum", fn: integration.PutObject_default_checksum},
	{name: "default_content_type", fn: integration.PutObject_default_content_type},
	{name: "success", fn: integration.PutObject_success},
	{name: "with_metadata", fn: integration.PutObject_with_metadata},
}

var putObjectXFail = []forgeCase{
	// The incorrect_md5 subcheck expects 400 InvalidDigest; ingot 500s.
	{name: "md5", fn: integration.PutObject_md5},
	// A metadata-combining re-PUT is denied (403) under the hilt authorize
	// flow.
	{name: "should_combine_metadata", fn: integration.PutObject_should_combine_metadata},
	{name: "object_acl_not_supported", fn: integration.PutObject_object_acl_not_supported},
	{name: "tagging", fn: integration.PutObject_tagging},
	// with_object_lock passes but needs the versioned teardown: it runs as
	// LockCreation/PutObject_with_object_lock (versity_lock_test.go).
}

var getObjectPass = []forgeCase{
	{name: "dir_object_checksum", fn: integration.GetObject_dir_object_checksum},
	{name: "dir_with_range", fn: integration.GetObject_dir_with_range},
	{name: "directory_object_noslash", fn: integration.GetObject_directory_object_noslash},
	{name: "empty_object_part_number_1", fn: integration.GetObject_empty_object_part_number_1},
	{name: "invalid_part_number", fn: integration.GetObject_invalid_part_number},
	{name: "non_existing_key", fn: integration.GetObject_non_existing_key},
	{name: "zero_len_with_range", fn: integration.GetObject_zero_len_with_range},
	{name: "by_range_resp_status", fn: integration.GetObject_by_range_resp_status},
	{name: "checksums", fn: integration.GetObject_checksums},
	{name: "conditional_reads", fn: integration.GetObject_conditional_reads},
	{name: "incidental_dir_object", fn: integration.GetObject_incidental_dir_object},
	{name: "invalid_parent", fn: integration.GetObject_invalid_parent},
	{name: "mp_part_number_exceeds_parts_count", fn: integration.GetObject_mp_part_number_exceeds_parts_count},
	{name: "mp_part_number_resp_status", fn: integration.GetObject_mp_part_number_resp_status},
	{name: "mp_part_number_success", fn: integration.GetObject_mp_part_number_success},
	{name: "non_mp_part_number_1_success", fn: integration.GetObject_non_mp_part_number_1_success},
	{name: "large_object", fn: integration.GetObject_large_object},
	{name: "non_existing_dir_object", fn: integration.GetObject_non_existing_dir_object},
	{name: "not_enabled_checksum_mode", fn: integration.GetObject_not_enabled_checksum_mode},
	{name: "overrides_presign_success", fn: integration.GetObject_overrides_presign_success},
	{name: "overrides_success", fn: integration.GetObject_overrides_success},
	{name: "range_and_part_number", fn: integration.GetObject_range_and_part_number},
	{name: "ranged_with_checksum_mode", fn: integration.GetObject_ranged_with_checksum_mode},
	{name: "with_range", fn: integration.GetObject_with_range},
}

var getObjectXFail = []forgeCase{
	// Directory objects are served with binary/octet-stream instead of
	// application/x-directory.
	{name: "directory_success", fn: integration.GetObject_directory_success},
	// Requires PutBucketPolicy, which ingot 501s (NotImplemented).
	{name: "overrides_fail_public", fn: integration.GetObject_overrides_fail_public},
	// Asserts object tagging (TagCount), which is unimplemented.
	{name: "success", fn: integration.GetObject_success},
}

var headObjectPass = []forgeCase{
	{name: "directory_object_noslash", fn: integration.HeadObject_directory_object_noslash},
	{name: "empty_object_part_number_1", fn: integration.HeadObject_empty_object_part_number_1},
	{name: "invalid_part_number", fn: integration.HeadObject_invalid_part_number},
	{name: "non_existing_object", fn: integration.HeadObject_non_existing_object},
	{name: "overrides_presign_success", fn: integration.HeadObject_overrides_presign_success},
	{name: "overrides_success", fn: integration.HeadObject_overrides_success},
	{name: "dir_with_range", fn: integration.HeadObject_dir_with_range},
	{name: "zero_len_with_range", fn: integration.HeadObject_zero_len_with_range},
	{name: "checksums", fn: integration.HeadObject_checksums},
	{name: "conditional_reads", fn: integration.HeadObject_conditional_reads},
	{name: "incidental_dir_object", fn: integration.HeadObject_incidental_dir_object},
	{name: "invalid_parent_dir", fn: integration.HeadObject_invalid_parent_dir},
	{name: "mp_part_number_exceeds_parts_count", fn: integration.HeadObject_mp_part_number_exceeds_parts_count},
	{name: "mp_part_number_resp_status", fn: integration.HeadObject_mp_part_number_resp_status},
	{name: "mp_part_number_success", fn: integration.HeadObject_mp_part_number_success},
	{name: "non_mp_part_number_1_success", fn: integration.HeadObject_non_mp_part_number_1_success},
	{name: "non_existing_dir_object", fn: integration.HeadObject_non_existing_dir_object},
	{name: "not_enabled_checksum_mode", fn: integration.HeadObject_not_enabled_checksum_mode},
	{name: "range_and_part_number", fn: integration.HeadObject_range_and_part_number},
	{name: "by_range_resp_status", fn: integration.HeadObject_by_range_resp_status},
	{name: "ranged_with_checksum_mode", fn: integration.HeadObject_ranged_with_checksum_mode},
	{name: "with_range", fn: integration.HeadObject_with_range},
}

var headObjectXFail = []forgeCase{
	// Requires PutBucketPolicy, which ingot 501s (NotImplemented).
	{name: "overrides_fail_public", fn: integration.HeadObject_overrides_fail_public},
	// Asserts object tagging (TagCount), which is unimplemented.
	{name: "success", fn: integration.HeadObject_success},
}

var deleteObjectPass = []forgeCase{
	{name: "directory_object", fn: integration.DeleteObject_directory_object},
	{name: "directory_object_noslash", fn: integration.DeleteObject_directory_object_noslash},
	{name: "incorrect_expected_bucket_owner", fn: integration.DeleteObject_incorrect_expected_bucket_owner},
	{name: "non_empty_dir_obj", fn: integration.DeleteObject_non_empty_dir_obj},
	{name: "non_existing_dir_object", fn: integration.DeleteObject_non_existing_dir_object},
	{name: "non_existing_object", fn: integration.DeleteObject_non_existing_object},
	{name: "success", fn: integration.DeleteObject_success},
	{name: "success_status_code", fn: integration.DeleteObject_success_status_code},
	{name: "conditional_writes", fn: integration.DeleteObject_conditional_writes},
}

var deleteObjectXFail = []forgeCase{
	// The ExpectedBucketOwner-matching delete is denied (403) under the
	// hilt authorize flow (ownership is the tenant's did:plc, not the
	// account the case expects).
	{name: "expected_bucket_owner", fn: integration.DeleteObject_expected_bucket_owner},
}

// CopyObject covers the full upstream group (31 cases). The in-memory suite
// only partitioned 16 of them; the remainder were partitioned empirically
// against the forge stack when this table was created. The posix-only
// CopyObject_overwrite_same_file_object (not part of the upstream CopyObject
// group dispatch) is deliberately absent.
var copyObjectPass = []forgeCase{
	{name: "copy_to_itself", fn: integration.CopyObject_copy_to_itself},
	{name: "copy_to_itself_invalid_directive", fn: integration.CopyObject_copy_to_itself_invalid_directive},
	{name: "invalid_copy_source", fn: integration.CopyObject_invalid_copy_source},
	{name: "non_existing_dst_bucket", fn: integration.CopyObject_non_existing_dst_bucket},
	{name: "to_itself_with_new_metadata", fn: integration.CopyObject_to_itself_with_new_metadata},
	{name: "invalid_tagging_directive", fn: integration.CopyObject_invalid_tagging_directive},
	{name: "invalid_checksum_algorithm", fn: integration.CopyObject_invalid_checksum_algorithm},
	{name: "success", fn: integration.CopyObject_success},
	{name: "copy_source_starting_with_slash", fn: integration.CopyObject_copy_source_starting_with_slash},
	{name: "default_content_type_with_replace_metadata", fn: integration.CopyObject_default_content_type_with_replace_metadata},
	{name: "non_existing_dir_object", fn: integration.CopyObject_non_existing_dir_object},
	{name: "with_metadata", fn: integration.CopyObject_with_metadata},
	{name: "conditional_reads", fn: integration.CopyObject_conditional_reads},
	{name: "with_special_characters", fn: integration.CopyObject_with_special_characters},
	{name: "long_metadata", fn: integration.CopyObject_long_metadata},
	{name: "should_copy_meta_props", fn: integration.CopyObject_should_copy_meta_props},
	{name: "should_replace_meta_props", fn: integration.CopyObject_should_replace_meta_props},
	{name: "missing_bucket_lock", fn: integration.CopyObject_missing_bucket_lock},
	// invalid_legal_hold / invalid_object_lock_mode put a source object into
	// a withLock() bucket and need the versioned teardown: they run under
	// LockCreation (versity_lock_test.go).
	{name: "invalid_website_redirect_location", fn: integration.CopyObject_invalid_website_redirect_location},
	{name: "create_checksum_on_copy", fn: integration.CopyObject_create_checksum_on_copy},
	{name: "should_copy_the_existing_checksum", fn: integration.CopyObject_should_copy_the_existing_checksum},
	{name: "should_replace_the_existing_checksum", fn: integration.CopyObject_should_replace_the_existing_checksum},
	{name: "to_itself_by_replacing_the_checksum", fn: integration.CopyObject_to_itself_by_replacing_the_checksum},
}

// Observed failing against the forge stack: multi-account semantics, tagging,
// and ACLs are unimplemented surface.
var copyObjectXFail = []forgeCase{
	{name: "not_owned_source_bucket", fn: integration.CopyObject_not_owned_source_bucket},
	{name: "should_replace_tagging", fn: integration.CopyObject_should_replace_tagging},
	{name: "should_copy_tagging", fn: integration.CopyObject_should_copy_tagging},
	{name: "object_acl_not_supported", fn: integration.CopyObject_object_acl_not_supported},
	{name: "incorrect_source_bucket_expected_owner", fn: integration.CopyObject_incorrect_source_bucket_expected_owner},
	// with_legal_hold / with_retention_lock pass but need the versioned
	// teardown: they run under LockCreation (versity_lock_test.go).
}

var deleteObjectsPass = []forgeCase{
	{name: "success", fn: integration.DeleteObjects_success},
	{name: "empty_input", fn: integration.DeleteObjects_empty_input},
	{name: "non_existing_objects", fn: integration.DeleteObjects_non_existing_objects},
}
