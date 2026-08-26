//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Versioning groups of the S3 conformance partition (upstream TestVersioning
// + the versioned ListObjectVersions cases), curated per docs/s3-versioning.md.
//
// These categories run with the VERSIONED S3Conf (WithVersioningEnabled), so
// upstream teardown empties buckets via ListObjectVersions + per-version
// deletes rather than plain DeleteObject (which would only stack up delete
// markers).
//
// Body-storing rows pass end-to-end: releasing a version's blobs traverses
// the whole delete chain (ingot /blob/remove → sprue forward → piri
// /blob/release), which needs hilt ≥ #37 (blob.Remove in the write set) and
// smelt ≥ #19 (the piri blob/release + blob/reject seed delegations).
//
// Excluded entirely (feature not modeled): the Versioning_*
// GetObjectAttributes, UploadPartCopy, and AccessControl (admin-API users)
// cases. Also excluded: the upstream
// VersioningDisabled_{Get,Put}BucketVersioning_not_configured pair, which
// asserts versitygw's versioning-disabled deployment mode
// (ErrVersioningNotConfigured) — ingot always implements versioning, so that
// mode never exists here. The object-lock / retention / legal-hold /
// versioned-WORM rows live in the tables below (docs/s3-object-lock.md §11),
// as do the versioned tagging rows (docs/s3-object-tagging.md §6); the
// standalone lock and tagging groups are curated in versity_lock_test.go and
// versity_tagging_test.go.

var putBucketVersioningPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.PutBucketVersioning_non_existing_bucket},
	{name: "invalid_status", fn: integration.PutBucketVersioning_invalid_status},
	{name: "success_enabled", fn: integration.PutBucketVersioning_success_enabled},
	{name: "success_suspended", fn: integration.PutBucketVersioning_success_suspended},
}

var getBucketVersioningPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.GetBucketVersioning_non_existing_bucket},
	{name: "empty_response", fn: integration.GetBucketVersioning_empty_response},
	{name: "success", fn: integration.GetBucketVersioning_success},
}

var listObjectVersionsPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.ListObjectVersions_non_existing_bucket},
	{name: "negative_max_keys", fn: integration.ListObjectVersions_negative_max_keys},
	{name: "list_single_object_versions", fn: integration.ListObjectVersions_list_single_object_versions},
	{name: "list_multiple_object_versions", fn: integration.ListObjectVersions_list_multiple_object_versions},
	{name: "multiple_object_versions_truncated", fn: integration.ListObjectVersions_multiple_object_versions_truncated},
	{name: "with_delete_markers", fn: integration.ListObjectVersions_with_delete_markers},
	{name: "containing_null_versionId_obj", fn: integration.ListObjectVersions_containing_null_versionId_obj},
	{name: "single_null_versionId_object", fn: integration.ListObjectVersions_single_null_versionId_object},
	{name: "checksum", fn: integration.ListObjectVersions_checksum},
}

// Upstream group TestListObjectVersions_VD: ListObjectVersions against a
// bucket that never configured versioning (plain conf — the bucket stays
// unversioned, so the plain teardown applies).
var listObjectVersionsVDPass = []forgeCase{
	{name: "VD_success", fn: integration.ListObjectVersions_VD_success},
}

var versioningPass = []forgeCase{
	// PutObject
	{name: "PutObject_success", fn: integration.Versioning_PutObject_success},
	{name: "PutObject_suspended_null_versionId_obj", fn: integration.Versioning_PutObject_suspended_null_versionId_obj},
	{name: "PutObject_null_versionId_obj", fn: integration.Versioning_PutObject_null_versionId_obj},
	{name: "PutObject_overwrite_null_versionId_obj", fn: integration.Versioning_PutObject_overwrite_null_versionId_obj},
	// CopyObject
	{name: "CopyObject_invalid_versionId", fn: integration.Versioning_CopyObject_invalid_versionId},
	{name: "CopyObject_encoded_versionid_separator_invalid_versionId", fn: integration.Versioning_CopyObject_encoded_versionid_separator_invalid_versionId},
	{name: "CopyObject_non_existing_version_id", fn: integration.Versioning_CopyObject_non_existing_version_id},
	{name: "CopyObject_special_chars", fn: integration.Versioning_CopyObject_special_chars},
	// HeadObject
	{name: "HeadObject_invalid_versionId", fn: integration.Versioning_HeadObject_invalid_versionId},
	{name: "HeadObject_non_existing_object_version", fn: integration.Versioning_HeadObject_non_existing_object_version},
	{name: "HeadObject_invalid_parent", fn: integration.Versioning_HeadObject_invalid_parent},
	{name: "HeadObject_success", fn: integration.Versioning_HeadObject_success},
	{name: "HeadObject_without_versionId", fn: integration.Versioning_HeadObject_without_versionId},
	{name: "HeadObject_delete_marker", fn: integration.Versioning_HeadObject_delete_marker},
	// GetObject
	{name: "GetObject_invalid_versionId", fn: integration.Versioning_GetObject_invalid_versionId},
	{name: "GetObject_non_existing_object_version", fn: integration.Versioning_GetObject_non_existing_object_version},
	{name: "GetObject_success", fn: integration.Versioning_GetObject_success},
	{name: "GetObject_delete_marker_without_versionId", fn: integration.Versioning_GetObject_delete_marker_without_versionId},
	{name: "GetObject_delete_marker", fn: integration.Versioning_GetObject_delete_marker},
	{name: "GetObject_null_versionId_obj", fn: integration.Versioning_GetObject_null_versionId_obj},
	// DeleteObject / DeleteObjects
	{name: "DeleteObject_non_existing_object", fn: integration.Versioning_DeleteObject_non_existing_object},
	{name: "DeleteObject_invalid_versionId", fn: integration.Versioning_DeleteObject_invalid_versionId},
	{name: "DeleteObject_delete_object_version", fn: integration.Versioning_DeleteObject_delete_object_version},
	{name: "DeleteObject_delete_a_delete_marker", fn: integration.Versioning_DeleteObject_delete_a_delete_marker},
	{name: "Delete_null_versionId_object", fn: integration.Versioning_Delete_null_versionId_object},
	{name: "DeleteObject_nested_dir_object", fn: integration.Versioning_DeleteObject_nested_dir_object},
	{name: "DeleteObject_suspended", fn: integration.Versioning_DeleteObject_suspended},
	{name: "DeleteObjects_success", fn: integration.Versioning_DeleteObjects_success},
	{name: "DeleteObjects_delete_deleteMarkers", fn: integration.Versioning_DeleteObjects_delete_deleteMarkers},
	// DeleteBucket
	{name: "DeleteBucket_not_empty", fn: integration.Versioning_DeleteBucket_not_empty},
	// Multipart
	{name: "Multipart_Upload_success", fn: integration.Versioning_Multipart_Upload_success},
	{name: "Multipart_Upload_overwrite_an_object", fn: integration.Versioning_Multipart_Upload_overwrite_an_object},
	// DeleteObjects on a withLock() bucket (docs/s3-object-lock.md §11).
	{name: "DeleteObject_non_existing_objects", fn: integration.Versioning_DeleteObject_non_existing_objects},
	// Object lock configuration (docs/s3-object-lock.md §5)
	{name: "object_lock_not_enabled_on_bucket_creation", fn: integration.Versioning_object_lock_not_enabled_on_bucket_creation},
	{name: "Enable_object_lock", fn: integration.Versioning_Enable_object_lock},
	{name: "status_switch_to_suspended_with_object_lock", fn: integration.Versioning_status_switch_to_suspended_with_object_lock},
	// Retention (docs/s3-object-lock.md §6)
	{name: "PutObjectRetention_invalid_versionId", fn: integration.Versioning_PutObjectRetention_invalid_versionId},
	{name: "PutObjectRetention_non_existing_object_version", fn: integration.Versioning_PutObjectRetention_non_existing_object_version},
	{name: "GetObjectRetention_invalid_versionId", fn: integration.Versioning_GetObjectRetention_invalid_versionId},
	{name: "GetObjectRetention_non_existing_object_version", fn: integration.Versioning_GetObjectRetention_non_existing_object_version},
	{name: "Put_GetObjectRetention_delete_marker", fn: integration.Versioning_Put_GetObjectRetention_delete_marker},
	{name: "Put_GetObjectRetention_success", fn: integration.Versioning_Put_GetObjectRetention_success},
	// Legal hold (docs/s3-object-lock.md §6)
	{name: "PutObjectLegalHold_invalid_versionId", fn: integration.Versioning_PutObjectLegalHold_invalid_versionId},
	{name: "PutObjectLegalHold_non_existing_object_version", fn: integration.Versioning_PutObjectLegalHold_non_existing_object_version},
	{name: "GetObjectLegalHold_invalid_versionId", fn: integration.Versioning_GetObjectLegalHold_invalid_versionId},
	{name: "GetObjectLegalHold_non_existing_object_version", fn: integration.Versioning_GetObjectLegalHold_non_existing_object_version},
	{name: "PutGetObjectLegalHold_delete_marker", fn: integration.Versioning_PutGetObjectLegalHold_delete_marker},
	{name: "Put_GetObjectLegalHold_success", fn: integration.Versioning_Put_GetObjectLegalHold_success},
	// Versioned WORM: enforcement through CheckObjectAccess against real
	// versions — version-scoped deletes of locked versions refuse, overwrites
	// and marker insertion stack versions (docs/s3-object-lock.md §11).
	{name: "WORM_obj_version_locked_with_legal_hold", fn: integration.Versioning_WORM_obj_version_locked_with_legal_hold},
	{name: "WORM_obj_version_locked_with_governance_retention", fn: integration.Versioning_WORM_obj_version_locked_with_governance_retention},
	{name: "WORM_obj_version_locked_with_compliance_retention", fn: integration.Versioning_WORM_obj_version_locked_with_compliance_retention},
	{name: "WORM_delete_marker_locked_object_legal_hold", fn: integration.Versioning_WORM_delete_marker_locked_object_legal_hold},
	{name: "WORM_delete_marker_locked_object_governance_retention", fn: integration.Versioning_WORM_delete_marker_locked_object_governance_retention},
	{name: "WORM_delete_marker_locked_object_compliance_retention", fn: integration.Versioning_WORM_delete_marker_locked_object_compliance_retention},
	{name: "WORM_PutObject_overwrite_locked_object", fn: integration.Versioning_WORM_PutObject_overwrite_locked_object},
	{name: "WORM_CopyObject_overwrite_locked_object", fn: integration.Versioning_WORM_CopyObject_overwrite_locked_object},
	{name: "WORM_CompleteMultipartUpload_overwrite_locked_object", fn: integration.Versioning_WORM_CompleteMultipartUpload_overwrite_locked_object},
	{name: "WORM_remove_delete_marker_under_bucket_default_retention", fn: integration.Versioning_WORM_remove_delete_marker_under_bucket_default_retention},
	// Tagging (docs/s3-object-tagging.md §6)
	{name: "PutObjectTagging_invalid_versionId", fn: integration.Versioning_PutObjectTagging_invalid_versionId},
	{name: "PutObjectTagging_non_existing_object_version", fn: integration.Versioning_PutObjectTagging_non_existing_object_version},
	{name: "GetObjectTagging_invalid_versionId", fn: integration.Versioning_GetObjectTagging_invalid_versionId},
	{name: "GetObjectTagging_non_existing_object_version", fn: integration.Versioning_GetObjectTagging_non_existing_object_version},
	{name: "DeleteObjectTagging_invalid_versionId", fn: integration.Versioning_DeleteObjectTagging_invalid_versionId},
	{name: "DeleteObjectTagging_non_existing_object_version", fn: integration.Versioning_DeleteObjectTagging_non_existing_object_version},
	{name: "PutGetDeleteObjectTagging_delete_marker", fn: integration.Versioning_PutGetDeleteObjectTagging_delete_marker},
	{name: "PutGetDeleteObjectTagging_success", fn: integration.Versioning_PutGetDeleteObjectTagging_success},
}

// versioningXFail: both cases copy from a second bucket, and cross-bucket
// copies are cross-SPACE copies (every bucket has its own space), rejected
// NotImplemented — each blob's CEK wrap is bound to (space, digest), so
// serving them needs the rewrap flow (a filed follow-up; see copyObjectXFail
// in versity_object_test.go).
var versioningXFail = []forgeCase{
	{name: "CopyObject_success", fn: integration.Versioning_CopyObject_success},
	{name: "CopyObject_from_an_object_version", fn: integration.Versioning_CopyObject_from_an_object_version},
}
