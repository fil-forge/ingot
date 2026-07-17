package testing

import (
	"context"
	"testing"

	"github.com/fil-forge/versitygw/tests/integration"
	"go.uber.org/zap/zaptest"
)

// smokeCase pairs an upstream versitygw integration case with its
// subtest name. Each TestSmoke_* / TestSmokeXFail_* function below
// declares its cases inline as a []smokeCase, so GoLand (and any
// other IDE that parses table-driven Go tests) renders one
// play-icon per row in the gutter.
type smokeCase struct {
	name string
	fn   integration.IntTest
}

// Layout: one top-level test per S3 group, in two flavors:
//
//	TestSmoke_<Group>      — known-passing cases (every case must pass)
//	TestSmokeXFail_<Group> — cases ingot fails today; each one is
//	                        expected to fail and reported as SKIP.
//	                        An unexpected pass errors so the case
//	                        can be promoted.
//
// Adding a case: when a fix lands, run the matching TestSmokeXFail_*.
// Cases that flip green will report "case unexpectedly passed" — move
// the line from the XFail function to the matching TestSmoke_* one.
//
// Each top-level test boots its own Harness (via smokeHarness), so
// failures in one group can't leak buckets / segments / op-roots
// into another. Cases within a group share one harness because the
// upstream cases create + tear down their own buckets internally.

// smokeHarness boots a Harness scoped to t and registers cleanup.
func smokeHarness(t *testing.T) *Harness {
	t.Helper()
	h, err := StartHarness(t.Context(), WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })
	return h
}

// =============================================================
// Known-passing cases
// =============================================================

func TestSmoke_CreateBucket(t *testing.T) {
	tests := []smokeCase{
		{"invalid_bucket_name", integration.CreateBucket_invalid_bucket_name},
		{"invalid_canned_acl", integration.CreateBucket_invalid_canned_acl},
		{"invalid_location_constraint", integration.CreateBucket_invalid_location_constraint},
		{"invalid_ownership", integration.CreateBucket_invalid_ownership},
		{"ownership_with_acl", integration.CreateBucket_ownership_with_acl},
		{"success", integration.CreateBucket_success},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_HeadBucket(t *testing.T) {
	tests := []smokeCase{
		{"non_existing_bucket", integration.HeadBucket_non_existing_bucket},
		{"success", integration.HeadBucket_success},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_ListBuckets(t *testing.T) {
	tests := []smokeCase{
		{"empty_success", integration.ListBuckets_empty_success},
		{"invalid_max_buckets", integration.ListBuckets_invalid_max_buckets},
		{"success", integration.ListBuckets_success},
		{"truncated", integration.ListBuckets_truncated},
		{"with_prefix", integration.ListBuckets_with_prefix},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_DeleteBucket(t *testing.T) {
	tests := []smokeCase{
		{"incorrect_expected_bucket_owner", integration.DeleteBucket_incorrect_expected_bucket_owner},
		{"non_empty_bucket", integration.DeleteBucket_non_empty_bucket},
		{"non_existing_bucket", integration.DeleteBucket_non_existing_bucket},
		{"success_status_code", integration.DeleteBucket_success_status_code},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_PutObject(t *testing.T) {
	tests := []smokeCase{
		{"checksum_algorithm_and_header_mismatch", integration.PutObject_checksum_algorithm_and_header_mismatch},
		{"checksums_success", integration.PutObject_checksums_success},
		{"conditional_writes", integration.PutObject_conditional_writes},
		{"default_checksum", integration.PutObject_default_checksum},
		{"default_content_type", integration.PutObject_default_content_type},
		{"dir_object_default_checksum", integration.PutObject_dir_object_default_checksum},
		{"dir_object_checksums_success", integration.PutObject_dir_object_checksums_success},
		{"incorrect_checksums", integration.PutObject_incorrect_checksums},
		{"invalid_credentials", integration.PutObject_invalid_credentials},
		{"false_negative_object_names", integration.PutObject_false_negative_object_names},
		{"invalid_checksum_header", integration.PutObject_invalid_checksum_header},
		{"invalid_legal_hold", integration.PutObject_invalid_legal_hold},
		{"invalid_object_lock_mode", integration.PutObject_invalid_object_lock_mode},
		{"invalid_object_names", integration.PutObject_invalid_object_names},
		{"invalid_retain_until_date", integration.PutObject_invalid_retain_until_date},
		{"long_metadata", integration.PutObject_long_metadata},
		{"missing_object_lock_retention_config", integration.PutObject_missing_object_lock_retention_config},
		{"multiple_checksum_headers", integration.PutObject_multiple_checksum_headers},
		{"non_existing_bucket", integration.PutObject_non_existing_bucket},
		{"past_retain_until_date", integration.PutObject_past_retain_until_date},
		{"racey_success", integration.PutObject_racey_success},
		{"special_chars", integration.PutObject_special_chars},
		{"success", integration.PutObject_success},
		{"should_combine_metadata", integration.PutObject_should_combine_metadata},
		{"with_metadata", integration.PutObject_with_metadata},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_GetObject(t *testing.T) {
	tests := []smokeCase{
		{"by_range_resp_status", integration.GetObject_by_range_resp_status},
		{"checksums", integration.GetObject_checksums},
		{"conditional_reads", integration.GetObject_conditional_reads},
		{"dir_object_checksum", integration.GetObject_dir_object_checksum},
		{"dir_with_range", integration.GetObject_dir_with_range},
		{"directory_object_noslash", integration.GetObject_directory_object_noslash},
		{"empty_object_part_number_1", integration.GetObject_empty_object_part_number_1},
		{"invalid_parent", integration.GetObject_invalid_parent},
		{"invalid_part_number", integration.GetObject_invalid_part_number},
		{"mp_part_number_exceeds_parts_count", integration.GetObject_mp_part_number_exceeds_parts_count},
		{"mp_part_number_resp_status", integration.GetObject_mp_part_number_resp_status},
		{"mp_part_number_success", integration.GetObject_mp_part_number_success},
		{"non_mp_part_number_1_success", integration.GetObject_non_mp_part_number_1_success},
		{"large_object", integration.GetObject_large_object},
		{"non_existing_dir_object", integration.GetObject_non_existing_dir_object},
		{"non_existing_key", integration.GetObject_non_existing_key},
		{"not_enabled_checksum_mode", integration.GetObject_not_enabled_checksum_mode},
		{"overrides_presign_success", integration.GetObject_overrides_presign_success},
		{"overrides_success", integration.GetObject_overrides_success},
		{"range_and_part_number", integration.GetObject_range_and_part_number},
		{"ranged_with_checksum_mode", integration.GetObject_ranged_with_checksum_mode},
		{"with_range", integration.GetObject_with_range},
		{"zero_len_with_range", integration.GetObject_zero_len_with_range},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_HeadObject(t *testing.T) {
	tests := []smokeCase{
		{"checksums", integration.HeadObject_checksums},
		{"conditional_reads", integration.HeadObject_conditional_reads},
		{"directory_object_noslash", integration.HeadObject_directory_object_noslash},
		{"empty_object_part_number_1", integration.HeadObject_empty_object_part_number_1},
		{"invalid_parent_dir", integration.HeadObject_invalid_parent_dir},
		{"invalid_part_number", integration.HeadObject_invalid_part_number},
		{"mp_part_number_exceeds_parts_count", integration.HeadObject_mp_part_number_exceeds_parts_count},
		{"mp_part_number_resp_status", integration.HeadObject_mp_part_number_resp_status},
		{"mp_part_number_success", integration.HeadObject_mp_part_number_success},
		{"non_mp_part_number_1_success", integration.HeadObject_non_mp_part_number_1_success},
		{"non_existing_dir_object", integration.HeadObject_non_existing_dir_object},
		{"non_existing_object", integration.HeadObject_non_existing_object},
		{"not_enabled_checksum_mode", integration.HeadObject_not_enabled_checksum_mode},
		{"overrides_presign_success", integration.HeadObject_overrides_presign_success},
		{"overrides_success", integration.HeadObject_overrides_success},
		{"range_and_part_number", integration.HeadObject_range_and_part_number},
		{"by_range_resp_status", integration.HeadObject_by_range_resp_status},
		{"dir_with_range", integration.HeadObject_dir_with_range},
		{"ranged_with_checksum_mode", integration.HeadObject_ranged_with_checksum_mode},
		{"with_range", integration.HeadObject_with_range},
		{"zero_len_with_range", integration.HeadObject_zero_len_with_range},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_DeleteObject(t *testing.T) {
	tests := []smokeCase{
		{"conditional_writes", integration.DeleteObject_conditional_writes},
		{"directory_object", integration.DeleteObject_directory_object},
		{"directory_object_noslash", integration.DeleteObject_directory_object_noslash},
		{"expected_bucket_owner", integration.DeleteObject_expected_bucket_owner},
		{"incorrect_expected_bucket_owner", integration.DeleteObject_incorrect_expected_bucket_owner},
		{"non_empty_dir_obj", integration.DeleteObject_non_empty_dir_obj},
		{"non_existing_dir_object", integration.DeleteObject_non_existing_dir_object},
		{"non_existing_object", integration.DeleteObject_non_existing_object},
		{"success", integration.DeleteObject_success},
		{"success_status_code", integration.DeleteObject_success_status_code},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

// =============================================================
// Known-failing cases (XFail)
// =============================================================

func TestSmokeXFail_CreateBucket(t *testing.T) {
	tests := []smokeCase{
		{"as_user", integration.CreateBucket_as_user},
		{"default_acl", integration.CreateBucket_default_acl},
		{"default_object_lock", integration.CreateBucket_default_object_lock},
		{"duplicate_keys", integration.CreateBucket_duplicate_keys},
		{"existing_bucket", integration.CreateBucket_existing_bucket},
		{"invalid_tags", integration.CreateBucket_invalid_tags},
		{"long_tags", integration.CreateBucket_long_tags},
		{"non_default_acl", integration.CreateBucket_non_default_acl},
		{"owned_by_you", integration.CreateBucket_owned_by_you},
		{"private_canned_acl", integration.CreateBucket_private_canned_acl},
		{"private_canned_acl_bucket_owner_enforced_ownership", integration.CreateBucket_private_canned_acl_bucket_owner_enforced_ownership},
		{"tag_count_limit", integration.CreateBucket_tag_count_limit},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_CreateBucket to TestSmoke_CreateBucket")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmokeXFail_ListBuckets(t *testing.T) {
	tests := []smokeCase{
		{"as_admin", integration.ListBuckets_as_admin},
		{"as_user", integration.ListBuckets_as_user},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_ListBuckets to TestSmoke_ListBuckets")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmokeXFail_PutObject(t *testing.T) {
	tests := []smokeCase{
		{"missing_bucket_lock", integration.PutObject_missing_bucket_lock},
		{"object_acl_not_supported", integration.PutObject_object_acl_not_supported},
		{"tagging", integration.PutObject_tagging},
		{"with_object_lock", integration.PutObject_with_object_lock},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_PutObject to TestSmoke_PutObject")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmokeXFail_GetObject(t *testing.T) {
	tests := []smokeCase{
		{"directory_success", integration.GetObject_directory_success},
		{"overrides_fail_public", integration.GetObject_overrides_fail_public},
		{"success", integration.GetObject_success},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_GetObject to TestSmoke_GetObject")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmokeXFail_HeadObject(t *testing.T) {
	tests := []smokeCase{
		{"overrides_fail_public", integration.HeadObject_overrides_fail_public},
		{"success", integration.HeadObject_success},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_HeadObject to TestSmoke_HeadObject")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmokeXFail_DeleteObject(t *testing.T) {
	tests := []smokeCase{}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_DeleteObject to TestSmoke_DeleteObject")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}

func TestSmoke_CopyObject(t *testing.T) {
	tests := []smokeCase{
		{"success", integration.CopyObject_success},
		{"copy_to_itself", integration.CopyObject_copy_to_itself},
		{"copy_to_itself_invalid_directive", integration.CopyObject_copy_to_itself_invalid_directive},
		{"copy_source_starting_with_slash", integration.CopyObject_copy_source_starting_with_slash},
		{"default_content_type_with_replace_metadata", integration.CopyObject_default_content_type_with_replace_metadata},
		{"invalid_copy_source", integration.CopyObject_invalid_copy_source},
		{"non_existing_dst_bucket", integration.CopyObject_non_existing_dst_bucket},
		{"non_existing_dir_object", integration.CopyObject_non_existing_dir_object},
		{"with_metadata", integration.CopyObject_with_metadata},
		{"to_itself_with_new_metadata", integration.CopyObject_to_itself_with_new_metadata},
		{"conditional_reads", integration.CopyObject_conditional_reads},
		{"with_special_characters", integration.CopyObject_with_special_characters},
		{"long_metadata", integration.CopyObject_long_metadata},
		{"should_copy_meta_props", integration.CopyObject_should_copy_meta_props},
		{"should_replace_meta_props", integration.CopyObject_should_replace_meta_props},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestSmoke_DeleteObjects(t *testing.T) {
	tests := []smokeCase{
		{"success", integration.DeleteObjects_success},
		{"empty_input", integration.DeleteObjects_empty_input},
		{"non_existing_objects", integration.DeleteObjects_non_existing_objects},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(s3conf); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

// TestSmokeXFail_CopyObject: cases ingot fails today. overwrite_same_file_object
// expects a posix filesystem error that doesn't apply to ingot's flat keyspace.
func TestSmokeXFail_CopyObject(t *testing.T) {
	tests := []smokeCase{
		{"overwrite_same_file_object", integration.CopyObject_overwrite_same_file_object},
	}
	s3conf := newS3Conf(smokeHarness(t).Config())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(s3conf)
			if err == nil {
				t.Errorf("case unexpectedly passed; promote it from TestSmokeXFail_CopyObject to TestSmoke_CopyObject")
				return
			}
			t.Skipf("known-failing: %v", err)
		})
	}
}
