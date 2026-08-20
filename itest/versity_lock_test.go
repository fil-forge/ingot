//go:build itest

package itest

import (
	"github.com/fil-forge/versitygw/tests/integration"
)

// Object-lock groups of the S3 conformance partition (upstream
// TestPutObjectLockConfiguration, TestGetObjectLockConfiguration,
// TestPut/GetObjectRetention, TestPut/GetObjectLegalHold, and
// TestWORMProtection), curated per docs/s3-object-lock.md §11.
//
// These categories run with the VERSIONED S3Conf: their buckets are created
// withLock(), which makes them versioning-Enabled, so upstream teardown must
// empty them via ListObjectVersions + per-version deletes. Teardown of locked
// objects works without governance bypass: the cases' own
// cleanupLockedObjects shortens each retention to seconds out (same-mode
// replacement, which the fork allows even for COMPLIANCE) and waits past
// expiry.
//
// Excluded entirely (structural, the VersioningDisabled_* treatment —
// versity_versioning_test.go):
//
//   - PutObjectLockConfiguration_not_enabled_on_bucket_creation asserts the
//     versioning-disabled gateway mode, where a lock configuration lands on
//     an unversioned bucket with no versioning check (upstream's own comment:
//     "this is not S3 compatible"). Ingot requires versioning Enabled and
//     answers 409, the AWS behavior
//     (Versioning_object_lock_not_enabled_on_bucket_creation pins it).
//   - The checkWORMProtection-based TestWORMProtection cases assert that a
//     plain PUT or unscoped DELETE against a locked object fails with
//     ErrObjectLocked — the unversioned-gateway lock model. A lock bucket in
//     ingot is always versioning-Enabled, so those requests succeed by
//     stacking a version or a marker (AWS semantics; docs/s3-object-lock.md
//     §1). The excluded rows: bucket_object_lock_configuration_compliance_mode,
//     bucket_object_lock_configuration_governance_mode,
//     object_lock_retention_compliance_locked,
//     object_lock_retention_governance_locked,
//     unable_to_overwrite_locked_object_{put,copy,mp},
//     object_lock_legal_hold_locked, and
//     root_bypass_governance_retention_delete_object (which also needs bucket
//     policy). Versioned WORM behavior is covered by the Versioning_WORM_*
//     rows in versity_versioning_test.go instead.

var putObjectLockConfigurationPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.PutObjectLockConfiguration_non_existing_bucket},
	{name: "empty_request_body", fn: integration.PutObjectLockConfiguration_empty_request_body},
	{name: "malformed_body", fn: integration.PutObjectLockConfiguration_malformed_body},
	{name: "invalid_status", fn: integration.PutObjectLockConfiguration_invalid_status},
	{name: "invalid_mode", fn: integration.PutObjectLockConfiguration_invalid_mode},
	{name: "both_years_and_days", fn: integration.PutObjectLockConfiguration_both_years_and_days},
	{name: "invalid_years_days", fn: integration.PutObjectLockConfiguration_invalid_years_days},
	{name: "success", fn: integration.PutObjectLockConfiguration_success},
}

var getObjectLockConfigurationPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.GetObjectLockConfiguration_non_existing_bucket},
	{name: "unset_config", fn: integration.GetObjectLockConfiguration_unset_config},
	{name: "success", fn: integration.GetObjectLockConfiguration_success},
}

var putObjectRetentionPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.PutObjectRetention_non_existing_bucket},
	{name: "non_existing_object", fn: integration.PutObjectRetention_non_existing_object},
	{name: "unset_bucket_object_lock_config", fn: integration.PutObjectRetention_unset_bucket_object_lock_config},
	{name: "expired_retain_until_date", fn: integration.PutObjectRetention_expired_retain_until_date},
	{name: "invalid_mode", fn: integration.PutObjectRetention_invalid_mode},
	{name: "overwrite_compliance_mode", fn: integration.PutObjectRetention_overwrite_compliance_mode},
	{name: "overwrite_compliance_with_compliance", fn: integration.PutObjectRetention_overwrite_compliance_with_compliance},
	{name: "overwrite_governance_with_governance", fn: integration.PutObjectRetention_overwrite_governance_with_governance},
	{name: "overwrite_governance_without_bypass_specified", fn: integration.PutObjectRetention_overwrite_governance_without_bypass_specified},
	{name: "success", fn: integration.PutObjectRetention_success},
}

var putObjectRetentionXFail = []forgeCase{
	// Installs a bucket policy granting s3:BypassGovernanceRetention; ingot
	// does not model bucket policies yet (docs/s3-object-lock.md §10). Flips
	// when they land.
	{name: "overwrite_governance_with_permission", fn: integration.PutObjectRetention_overwrite_governance_with_permission},
}

var getObjectRetentionPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.GetObjectRetention_non_existing_bucket},
	{name: "non_existing_object", fn: integration.GetObjectRetention_non_existing_object},
	{name: "disabled_lock", fn: integration.GetObjectRetention_disabled_lock},
	{name: "unset_config", fn: integration.GetObjectRetention_unset_config},
	{name: "success", fn: integration.GetObjectRetention_success},
}

var putObjectLegalHoldPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.PutObjectLegalHold_non_existing_bucket},
	{name: "non_existing_object", fn: integration.PutObjectLegalHold_non_existing_object},
	{name: "invalid_body", fn: integration.PutObjectLegalHold_invalid_body},
	{name: "invalid_status", fn: integration.PutObjectLegalHold_invalid_status},
	{name: "unset_bucket_object_lock_config", fn: integration.PutObjectLegalHold_unset_bucket_object_lock_config},
	{name: "success", fn: integration.PutObjectLegalHold_success},
}

var getObjectLegalHoldPass = []forgeCase{
	{name: "non_existing_bucket", fn: integration.GetObjectLegalHold_non_existing_bucket},
	{name: "non_existing_object", fn: integration.GetObjectLegalHold_non_existing_object},
	{name: "disabled_lock", fn: integration.GetObjectLegalHold_disabled_lock},
	{name: "unset_config", fn: integration.GetObjectLegalHold_unset_config},
	{name: "success", fn: integration.GetObjectLegalHold_success},
}

// Lock-enabled-bucket cases from the PutObject / CopyObject /
// CreateMultipartUpload groups. They live here rather than in their home
// categories because their buckets are created lock-enabled (withLock(), or
// by hand for racey_success) and are therefore versioning-Enabled, needing
// the versioned teardown; the home categories run the plain conf, whose
// teardown cannot empty a non-empty versioned bucket (unscoped deletes only
// stack markers).
var lockCreationPass = []forgeCase{
	{name: "PutObject_with_object_lock", fn: integration.PutObject_with_object_lock},
	{name: "PutObject_racey_success", fn: integration.PutObject_racey_success},
	{name: "CopyObject_with_legal_hold", fn: integration.CopyObject_with_legal_hold},
	{name: "CopyObject_with_retention_lock", fn: integration.CopyObject_with_retention_lock},
	{name: "CopyObject_invalid_legal_hold", fn: integration.CopyObject_invalid_legal_hold},
	{name: "CopyObject_invalid_object_lock_mode", fn: integration.CopyObject_invalid_object_lock_mode},
	{name: "CreateMultipartUpload_with_object_lock", fn: integration.CreateMultipartUpload_with_object_lock},
}

// TestWORMProtection contributes no pass rows yet: the governance-bypass
// family below installs bucket policies, and the rest is excluded (header
// comment). Every row here fails on PutBucketPolicy (ErrNotImplemented) and
// flips when bucket policies land.
var wormProtectionXFail = []forgeCase{
	{name: "bucket_object_lock_governance_bypass_delete", fn: integration.WORMProtection_bucket_object_lock_governance_bypass_delete},
	{name: "bucket_object_lock_governance_bypass_delete_multiple", fn: integration.WORMProtection_bucket_object_lock_governance_bypass_delete_multiple},
	{name: "object_lock_retention_governance_bypass_overwrite_put", fn: integration.WORMProtection_object_lock_retention_governance_bypass_overwrite_put},
	{name: "object_lock_retention_governance_bypass_overwrite_copy", fn: integration.WORMProtection_object_lock_retention_governance_bypass_overwrite_copy},
	{name: "object_lock_retention_governance_bypass_overwrite_mp", fn: integration.WORMProtection_object_lock_retention_governance_bypass_overwrite_mp},
	{name: "object_lock_retention_governance_bypass_delete", fn: integration.WORMProtection_object_lock_retention_governance_bypass_delete},
	{name: "object_lock_retention_governance_bypass_delete_mul", fn: integration.WORMProtection_object_lock_retention_governance_bypass_delete_mul},
}
