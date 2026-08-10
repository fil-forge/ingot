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
// Excluded entirely (feature not modeled, mirroring the absent Tagging /
// ObjectLock categories): the Versioning_* object-tagging, object-lock /
// retention / legal-hold / WORM, GetObjectAttributes, UploadPartCopy, and
// AccessControl (admin-API users) cases. Also excluded: the upstream
// VersioningDisabled_{Get,Put}BucketVersioning_not_configured pair, which
// asserts versitygw's versioning-disabled deployment mode
// (ErrVersioningNotConfigured) — ingot always implements versioning, so that
// mode never exists here.

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
	{name: "CopyObject_success", fn: integration.Versioning_CopyObject_success},
	{name: "CopyObject_non_existing_version_id", fn: integration.Versioning_CopyObject_non_existing_version_id},
	{name: "CopyObject_from_an_object_version", fn: integration.Versioning_CopyObject_from_an_object_version},
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
}

var versioningXFail = []forgeCase{
	// Upstream creates its bucket withLock(), and ingot does not model
	// object-lock creation (which on AWS auto-enables versioning), so the
	// versionId echo assertions fail before teardown.
	{name: "DeleteObject_non_existing_objects", fn: integration.Versioning_DeleteObject_non_existing_objects},
}
