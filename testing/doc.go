// Package testing is thin S3-client glue for tests that drive a running
// ingot listener. The listener itself is always provided by the caller —
// there is no in-process harness; the deployment under test is the real
// forge-mode daemon in the smelt stack (see the itest package).
//
//   - config.go — Config (connection details) and NewS3Conf, the bridge to
//     the upstream versitygw integration suite: upstream case functions have
//     signature func(*integration.S3Conf) error and are invoked directly by
//     itest's curated conformance partition.
//   - roundtrip.go — CreateBucket/PutBytes/GetBytes: minimal single-object
//     round-trip helpers for scenario tests (e.g. read-after-eviction).
//
// The S3 conformance partition — the curated lists of upstream cases ingot
// is expected to pass and known to fail — lives in itest/versity_*_test.go
// and runs against the forge-mode deployment: `make itest` (Docker). Unit
// tests (`make test`) cover libraries and pure logic only.
package testing
