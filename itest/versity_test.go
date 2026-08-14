//go:build itest

package itest

import (
	"testing"

	"github.com/fil-forge/versitygw/tests/integration"
)

// forgeCase pairs an upstream versitygw integration case with its subtest
// name, so each category table below renders one runnable row per case.
// skip, when non-nil and returning a non-empty reason, skips the case —
// used for the order-sensitive ListBuckets_truncated (under -shuffle) and
// the load-sensitive CompleteMultipartUpload racey cases.
type forgeCase struct {
	name string
	fn   integration.IntTest
	skip func() string
}

// TestForgeVersity is the S3 conformance partition, run against a forge-mode
// ingot in the smelt stack (this working tree's binary). One stack boot is
// shared by every category — the upstream cases create and tear down their
// own buckets, so categories don't interfere — and the stack lives until this
// test (including all subtests) completes.
//
// Layout mirrors the curated format: per upstream group, a <Group> subtest of
// known-passing cases (every case must pass) and a <Group>XFail subtest of
// cases ingot fails today (each expected to fail and reported as SKIP; an
// unexpected pass errors so the case can be promoted).
//
// Promoting a case: when a fix lands, run the matching XFail subtest. Cases
// that flip green report "case unexpectedly passed" — move the line from the
// xfail table to the pass table.
//
// Run one category or one case:
//
//	go test -tags itest ./itest -run 'TestForgeVersity/PutObject' -v
//	go test -tags itest ./itest -run 'TestForgeVersity/PutObject/success' -v
func TestForgeVersity(t *testing.T) {
	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, t.Context(), s, "versity")
	conf := forgeS3Conf(endpoint, accessKey, secretKey)
	confVersioned := forgeS3ConfVersioned(endpoint, accessKey, secretKey)

	categories := []struct {
		name      string
		pass      []forgeCase
		xfail     []forgeCase
		versioned bool // run with confVersioned (versioned teardown)
	}{
		// ListBuckets runs FIRST: its empty_success/success cases assert the
		// exact bucket listing, which only holds on the pristine stack —
		// failed cases in other categories leak their buckets (upstream
		// teardown doesn't run on error), and unlike the old per-group
		// in-memory harnesses, all categories share this one deployment.
		{"ListBuckets", listBucketsPass, listBucketsXFail, false},
		{"CreateBucket", createBucketPass, createBucketXFail, false},
		{"HeadBucket", headBucketPass, nil, false},
		{"DeleteBucket", deleteBucketPass, nil, false},
		{"PutObject", putObjectPass, putObjectXFail, false},
		{"GetObject", getObjectPass, getObjectXFail, false},
		{"HeadObject", headObjectPass, headObjectXFail, false},
		{"DeleteObject", deleteObjectPass, deleteObjectXFail, false},
		{"CopyObject", copyObjectPass, copyObjectXFail, false},
		{"DeleteObjects", deleteObjectsPass, nil, false},
		{"CreateMultipartUpload", createMultipartPass, createMultipartXFail, false},
		{"UploadPart", uploadPartPass, uploadPartXFail, false},
		{"UploadPartCopy", uploadPartCopyPass, uploadPartCopyXFail, false},
		{"ListParts", listPartsPass, listPartsXFail, false},
		{"ListMultipartUploads", listMultipartUploadsPass, listMultipartUploadsXFail, false},
		{"AbortMultipartUpload", abortMultipartPass, abortMultipartXFail, false},
		{"CompleteMultipartUpload", completeMultipartPass, completeMultipartXFail, false},
		// The versioning partition (docs/s3-versioning.md) runs with the
		// versioned conf so upstream teardown empties buckets per-version.
		{"PutBucketVersioning", putBucketVersioningPass, nil, true},
		{"GetBucketVersioning", getBucketVersioningPass, nil, true},
		{"ListObjectVersions", listObjectVersionsPass, nil, true},
		{"Versioning", versioningPass, versioningXFail, true},
		// The object-lock partition (docs/s3-object-lock.md §11): withLock()
		// buckets are versioning-Enabled, so these also need the versioned
		// teardown.
		{"PutObjectLockConfiguration", putObjectLockConfigurationPass, nil, true},
		{"GetObjectLockConfiguration", getObjectLockConfigurationPass, nil, true},
		{"PutObjectRetention", putObjectRetentionPass, putObjectRetentionXFail, true},
		{"GetObjectRetention", getObjectRetentionPass, nil, true},
		{"PutObjectLegalHold", putObjectLegalHoldPass, nil, true},
		{"GetObjectLegalHold", getObjectLegalHoldPass, nil, true},
		// Creation-time stamping cases whose withLock() buckets need the
		// versioned teardown their home (plain-conf) categories can't give
		// them (versity_lock_test.go).
		{"LockCreation", lockCreationPass, nil, true},
		{"WORMProtection", nil, wormProtectionXFail, true},
		// TestListObjectVersions_VD: listing a never-versioned bucket; runs
		// with the plain conf (the bucket stays unversioned).
		{"ListObjectVersionsVD", listObjectVersionsVDPass, nil, false},
	}

	for _, cat := range categories {
		catConf := conf
		if cat.versioned {
			catConf = confVersioned
		}
		t.Run(cat.name, func(t *testing.T) {
			for _, tt := range cat.pass {
				t.Run(tt.name, func(t *testing.T) {
					if tt.skip != nil {
						if reason := tt.skip(); reason != "" {
							t.Skip(reason)
						}
					}
					if err := tt.fn(catConf); err != nil {
						t.Fatalf("%v", err)
					}
				})
			}
		})
		if cat.xfail == nil {
			continue
		}
		t.Run(cat.name+"XFail", func(t *testing.T) {
			for _, tt := range cat.xfail {
				t.Run(tt.name, func(t *testing.T) {
					err := tt.fn(catConf)
					if err == nil {
						t.Errorf("case unexpectedly passed; promote it from the %s xfail table to the pass table", cat.name)
						return
					}
					t.Skipf("known-failing: %v", err)
				})
			}
		})
	}
}
