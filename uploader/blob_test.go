package uploader

import (
	"errors"
	"fmt"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	edm "github.com/fil-forge/ucantone/errors/datamodel"
)

// TestAllocationGone: a /blob/accept failure named AllocationExpired or
// AllocationNotFound is found however the conclude's error wraps or joins
// it, and no other failure counts.
func TestAllocationGone(t *testing.T) {
	failure := func(name string) error {
		return fmt.Errorf("blob x: %w", fmt.Errorf("failure in accept receipt: %w", edm.ErrorModel{ErrorName: name, Message: "refused"}))
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"expired", failure(blobcmds.AllocationExpiredErrorName), true},
		{"not found", failure(blobcmds.AllocationNotFoundErrorName), true},
		{"joined behind another failure", errors.Join(failure(blobcmds.BlobDigestMismatchErrorName), failure(blobcmds.AllocationNotFoundErrorName)), true},
		{"another named failure", failure(blobcmds.BlobDigestMismatchErrorName), false},
		{"an unnamed error", errors.New("upload service unreachable"), false},
		{"no error", nil, false},
	}
	for _, c := range cases {
		if got := allocationGone(c.err); got != c.want {
			t.Errorf("%s: allocationGone = %v, want %v", c.name, got, c.want)
		}
	}
}
