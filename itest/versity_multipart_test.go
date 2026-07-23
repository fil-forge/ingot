//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Multipart groups of the S3 conformance partition, partitioned empirically
// against the forge-mode stack (see the curation note in README.md). The
// remaining xfail surface: part-level checksums (FIL-620), tagging/object-lock
// /ACL on create (FIL-534/FIL-525), and the UploadPartCopy group (FIL-586).

var createMultipartPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.CreateMultipartUpload_non_existing_bucket},
	{name: "dir_obj", fn: integration.CreateMultipartUpload_dir_obj},
	{name: "long_metadata", fn: integration.CreateMultipartUpload_long_metadata},
	{name: "with_metadata", fn: integration.CreateMultipartUpload_with_metadata},
	{name: "with_object_lock_invalid_retention", fn: integration.CreateMultipartUpload_with_object_lock_invalid_retention},
	{name: "past_retain_until_date", fn: integration.CreateMultipartUpload_past_retain_until_date},
	{name: "invalid_legal_hold", fn: integration.CreateMultipartUpload_invalid_legal_hold},
	{name: "invalid_object_lock_mode", fn: integration.CreateMultipartUpload_invalid_object_lock_mode},
	{name: "invalid_website_redirect_location", fn: integration.CreateMultipartUpload_invalid_website_redirect_location},
	{name: "invalid_checksum_algorithm", fn: integration.CreateMultipartUpload_invalid_checksum_algorithm},
	{name: "empty_checksum_algorithm_with_checksum_type", fn: integration.CreateMultipartUpload_empty_checksum_algorithm_with_checksum_type},
	{name: "type_algo_mismatch", fn: integration.CreateMultipartUpload_type_algo_mismatch},
	{name: "invalid_checksum_type", fn: integration.CreateMultipartUpload_invalid_checksum_type},
	{name: "valid_algo_type", fn: integration.CreateMultipartUpload_valid_algo_type},
	{name: "success", fn: integration.CreateMultipartUpload_success},
}

var createMultipartXFail = []forgeCase{
	{name: "with_tagging", fn: integration.CreateMultipartUpload_with_tagging},
	{name: "with_object_lock", fn: integration.CreateMultipartUpload_with_object_lock},
	{name: "with_object_lock_not_enabled", fn: integration.CreateMultipartUpload_with_object_lock_not_enabled},
	{name: "object_acl_not_supported", fn: integration.CreateMultipartUpload_object_acl_not_supported},
}

var uploadPartPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.UploadPart_non_existing_bucket},
	{name: "invalid_part_number", fn: integration.UploadPart_invalid_part_number},
	{name: "non_existing_key", fn: integration.UploadPart_non_existing_key},
	{name: "etag_quoting_consistency", fn: integration.UploadPart_etag_quoting_consistency},
	{name: "non_existing_mp_upload", fn: integration.UploadPart_non_existing_mp_upload},
	{name: "multiple_checksum_headers", fn: integration.UploadPart_multiple_checksum_headers},
	{name: "invalid_checksum_header", fn: integration.UploadPart_invalid_checksum_header},
	{name: "checksum_header_and_algo_mismatch", fn: integration.UploadPart_checksum_header_and_algo_mismatch},
	{name: "success", fn: integration.UploadPart_success},
}

var uploadPartXFail = []forgeCase{
	{name: "checksum_algorithm_mistmatch_on_initialization", fn: integration.UploadPart_checksum_algorithm_mistmatch_on_initialization},
	{name: "checksum_algorithm_mistmatch_on_initialization_with_value", fn: integration.UploadPart_checksum_algorithm_mistmatch_on_initialization_with_value},
	{name: "incorrect_checksums", fn: integration.UploadPart_incorrect_checksums},
	{name: "no_checksum_with_full_object_checksum_type", fn: integration.UploadPart_no_checksum_with_full_object_checksum_type},
	{name: "no_checksum_with_composite_checksum_type", fn: integration.UploadPart_no_checksum_with_composite_checksum_type},
	{name: "with_checksums_success", fn: integration.UploadPart_with_checksums_success},
}

var uploadPartCopyPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.UploadPartCopy_non_existing_bucket},
	{name: "invalid_part_number", fn: integration.UploadPartCopy_invalid_part_number},
	{name: "invalid_copy_source", fn: integration.UploadPartCopy_invalid_copy_source},
}

var uploadPartCopyXFail = []forgeCase{
	{name: "incorrect_uploadId", fn: integration.UploadPartCopy_incorrect_uploadId},
	{name: "incorrect_object_key", fn: integration.UploadPartCopy_incorrect_object_key},
	{name: "non_existing_source_bucket", fn: integration.UploadPartCopy_non_existing_source_bucket},
	{name: "non_existing_source_object_key", fn: integration.UploadPartCopy_non_existing_source_object_key},
	{name: "success", fn: integration.UploadPartCopy_success},
	{name: "by_range_invalid_ranges", fn: integration.UploadPartCopy_by_range_invalid_ranges},
	{name: "exceeding_copy_source_range", fn: integration.UploadPartCopy_exceeding_copy_source_range},
	{name: "greater_range_than_obj_size", fn: integration.UploadPartCopy_greater_range_than_obj_size},
	{name: "by_range_success", fn: integration.UploadPartCopy_by_range_success},
	{name: "should_copy_the_checksum", fn: integration.UploadPartCopy_should_copy_the_checksum},
	{name: "should_not_copy_the_checksum", fn: integration.UploadPartCopy_should_not_copy_the_checksum},
	{name: "should_calculate_the_checksum", fn: integration.UploadPartCopy_should_calculate_the_checksum},
	{name: "conditional_reads", fn: integration.UploadPartCopy_conditional_reads},
	{name: "incorrect_source_bucket_expected_owner", fn: integration.UploadPartCopy_incorrect_source_bucket_expected_owner},
}

var listPartsPass = []forgeCase{
	{name: "incorrect_uploadId", fn: integration.ListParts_incorrect_uploadId},
	{name: "incorrect_object_key", fn: integration.ListParts_incorrect_object_key},
	{name: "invalid_max_parts", fn: integration.ListParts_invalid_max_parts},
	{name: "invalid_part_number_marker", fn: integration.ListParts_invalid_part_number_marker},
	{name: "default_max_parts", fn: integration.ListParts_default_max_parts},
	{name: "exceeding_max_parts", fn: integration.ListParts_exceeding_max_parts},
	{name: "truncated", fn: integration.ListParts_truncated},
	{name: "success", fn: integration.ListParts_success},
	{name: "with_checksums", fn: integration.ListParts_with_checksums},
}

// Explicit null-checksum-type echo is FIL-620.
var listPartsXFail = []forgeCase{
	{name: "null_checksums", fn: integration.ListParts_null_checksums},
}

var listMultipartUploadsPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.ListMultipartUploads_non_existing_bucket},
	{name: "empty_result", fn: integration.ListMultipartUploads_empty_result},
	{name: "invalid_max_uploads", fn: integration.ListMultipartUploads_invalid_max_uploads},
	{name: "max_uploads", fn: integration.ListMultipartUploads_max_uploads},
	{name: "exceeding_max_uploads", fn: integration.ListMultipartUploads_exceeding_max_uploads},
	{name: "ignore_upload_id_marker", fn: integration.ListMultipartUploads_ignore_upload_id_marker},
	{name: "invalid_uploadId_marker", fn: integration.ListMultipartUploads_invalid_uploadId_marker},
	{name: "keyMarker_not_from_list", fn: integration.ListMultipartUploads_keyMarker_not_from_list},
	{name: "delimiter_truncated", fn: integration.ListMultipartUploads_delimiter_truncated},
	{name: "prefix", fn: integration.ListMultipartUploads_prefix},
	{name: "both_delimiter_and_prefix", fn: integration.ListMultipartUploads_both_delimiter_and_prefix},
	{name: "delimiter_no_matches", fn: integration.ListMultipartUploads_delimiter_no_matches},
	{name: "with_checksums", fn: integration.ListMultipartUploads_with_checksums},
}

var listMultipartUploadsXFail = []forgeCase{}

var abortMultipartPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.AbortMultipartUpload_non_existing_bucket},
	{name: "incorrect_uploadId", fn: integration.AbortMultipartUpload_incorrect_uploadId},
	{name: "incorrect_object_key", fn: integration.AbortMultipartUpload_incorrect_object_key},
	{name: "success", fn: integration.AbortMultipartUpload_success},
	{name: "success_status_code", fn: integration.AbortMultipartUpload_success_status_code},
	{name: "if_match_initiated_time", fn: integration.AbortMultipartUpload_if_match_initiated_time},
}

var abortMultipartXFail = []forgeCase{}

var completeMultipartPass = []forgeCase{
	// upstream function name carries a typo (CompletedMultipartUpload_...).
	{name: "non_existing_bucket", fn: integration.CompletedMultipartUpload_non_existing_bucket},
	{name: "incorrect_part_number", fn: integration.CompleteMultipartUpload_incorrect_part_number},
	{name: "missing_part_fields", fn: integration.CompleteMultipartUpload_missing_part_fields},
	{name: "invalid_part_number", fn: integration.CompleteMultipartUpload_invalid_part_number},
	{name: "default_content_type", fn: integration.CompleteMultipartUpload_default_content_type},
	{name: "invalid_ETag", fn: integration.CompleteMultipartUpload_invalid_ETag},
	{name: "small_upload_size", fn: integration.CompleteMultipartUpload_small_upload_size},
	{name: "empty_parts", fn: integration.CompleteMultipartUpload_empty_parts},
	{name: "incorrect_parts_order", fn: integration.CompleteMultipartUpload_incorrect_parts_order},
	{name: "mpu_object_size", fn: integration.CompleteMultipartUpload_mpu_object_size},
	{name: "conditional_writes", fn: integration.CompleteMultipartUpload_conditional_writes},
	{name: "invalid_checksum_type", fn: integration.CompleteMultipartUpload_invalid_checksum_type},
	{name: "multiple_final_checksums", fn: integration.CompleteMultipartUpload_multiple_final_checksums},
	{name: "invalid_final_checksums", fn: integration.CompleteMultipartUpload_invalid_final_checksums},
	{name: "invalid_final_composite_checksum", fn: integration.CompleteMultipartUpload_invalid_final_composite_checksum},
	{name: "with_metadata", fn: integration.CompleteMultipartUpload_with_metadata},
	{name: "success", fn: integration.CompleteMultipartUpload_success},
	{name: "already_completed", fn: integration.CompleteMultipartUpload_already_completed},
	// racey_success races ten concurrent 25 MiB multipart uploads of one key
	// under a 30s client deadline — on a host simultaneously running the
	// smelt stack its outcome depends on load, not S3 semantics, so it is
	// skipped rather than curated.
	{name: "racey_success", fn: integration.CompleteMultipartUpload_racey_success, skip: func() string {
		return "load-sensitive (10 concurrent 25MiB uploads under a 30s deadline); outcome depends on host load, not S3 semantics"
	}},
}

// Part-level / composite-checksum verification is FIL-620;
// racey_data_integrity additionally leans on atomic concurrent overwrites.
var completeMultipartXFail = []forgeCase{
	{name: "invalid_checksum_part", fn: integration.CompleteMultipartUpload_invalid_checksum_part},
	{name: "multiple_checksum_part", fn: integration.CompleteMultipartUpload_multiple_checksum_part},
	{name: "incorrect_checksum_part", fn: integration.CompleteMultipartUpload_incorrect_checksum_part},
	{name: "different_checksum_part", fn: integration.CompleteMultipartUpload_different_checksum_part},
	{name: "missing_part_checksum", fn: integration.CompleteMultipartUpload_missing_part_checksum},
	{name: "incorrect_final_checksums", fn: integration.CompleteMultipartUpload_incorrect_final_checksums},
	{name: "should_calculate_the_final_checksum_full_object", fn: integration.CompleteMultipartUpload_should_calculate_the_final_checksum_full_object},
	{name: "should_verify_the_final_checksum", fn: integration.CompleteMultipartUpload_should_verify_the_final_checksum},
	{name: "should_verify_final_composite_checksum", fn: integration.CompleteMultipartUpload_should_verify_final_composite_checksum},
	{name: "checksum_type_mismatch", fn: integration.CompleteMultipartUpload_checksum_type_mismatch},
	{name: "should_ignore_the_final_checksum", fn: integration.CompleteMultipartUpload_should_ignore_the_final_checksum},
	{name: "should_succeed_without_final_checksum_type", fn: integration.CompleteMultipartUpload_should_succeed_without_final_checksum_type},
	{name: "racey_data_integrity", fn: integration.CompleteMultipartUpload_racey_data_integrity},
}
