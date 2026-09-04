package s3frontend

import (
	"errors"
	"strings"
	"testing"

	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/versitygw/s3err"
)

// TestObjectKeyError locks the key-validation error mapping: a key over the
// size limit is KeyTooLongError (matching AWS), other invalid keys are
// InvalidRequest, and valid keys pass.
func TestObjectKeyError(t *testing.T) {
	cases := []struct {
		name     string
		key      string
		wantCode string // "" means expect nil error
	}{
		{"max length ok", strings.Repeat("a", mst.MaxKeyBytes), ""},
		{"one over limit", strings.Repeat("a", mst.MaxKeyBytes+1), "KeyTooLongError"},
		{"empty", "", "InvalidRequest"},
		{"nul byte", "a\x00b", "InvalidRequest"},
		{"normal", "path/to/obj.txt", ""},
	}
	for _, c := range cases {
		err := objectKeyError(c.key)
		if c.wantCode == "" {
			if err != nil {
				t.Errorf("%s: got %v, want nil", c.name, err)
			}
			continue
		}
		var apiErr s3err.APIError
		if !errors.As(err, &apiErr) {
			t.Errorf("%s: got %v, want APIError with code %q", c.name, err, c.wantCode)
			continue
		}
		if apiErr.Code != c.wantCode {
			t.Errorf("%s: code = %q, want %q", c.name, apiErr.Code, c.wantCode)
		}
	}
}
