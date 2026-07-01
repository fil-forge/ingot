package testing

import (
	"reflect"
	"testing"
)

// TestParseCaseLines locks the recovery of per-case names from the upstream
// suite's ANSI-colored stdout (PASS/FAIL lines), which is how Result.{Passed,
// Failed}Cases is built. Teardown-failure lines (no "FAIL " prefix) are ignored.
func TestParseCaseLines(t *testing.T) {
	const (
		red   = "\033[31m"
		green = "\033[32m"
		cyan  = "\033[36m"
		reset = "\033[0m"
	)
	out := cyan + "RUN  " + reset + "PutObject_success\n" +
		green + "PASS " + reset + "PutObject_success\n" +
		cyan + "RUN  " + reset + "PutObject_conditional_writes\n" +
		red + "FAIL " + reset + "PutObject_conditional_writes: operation error S3: PutObject, StatusCode: 500\n" +
		red + "SomeCase: failed to delete the bucket: boom\n" + // teardown noise, no FAIL prefix
		green + "PASS " + reset + "GetObject_success\n" +
		red + "FAIL " + reset + "CreateBucket_default_acl: expected grants length to be 1, instead got 0\n"

	passed, failed := parseCaseLines(out)

	wantPassed := []string{"PutObject_success", "GetObject_success"}
	wantFailed := []string{"PutObject_conditional_writes", "CreateBucket_default_acl"}
	if !reflect.DeepEqual(passed, wantPassed) {
		t.Errorf("passed = %v, want %v", passed, wantPassed)
	}
	if !reflect.DeepEqual(failed, wantFailed) {
		t.Errorf("failed = %v, want %v", failed, wantFailed)
	}
}
