package s3frontend

import (
	"strings"
	"testing"

	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/assert"
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
			assert.NoError(t, err, c.name)
			continue
		}
		var apiErr s3err.APIError
		if !assert.ErrorAs(t, err, &apiErr, "%s: want APIError with code %q", c.name, c.wantCode) {
			continue
		}
		assert.Equal(t, c.wantCode, apiErr.Code, "%s: code", c.name)
	}
}
