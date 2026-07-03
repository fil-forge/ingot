//go:build !unix

package regionkey

import "errors"

// errLockUnsupported reports that this platform has no supported way to lock
// memory against swapping. The region provider fails closed on it: newKEK
// returns an error rather than holding a raw KEK in swappable memory, so the
// exposure-hygiene guarantee is never silently dropped. Region custody is a
// server-side concern and the supported deployment targets (Linux, macOS) are
// all unix; this fallback exists so the package still builds elsewhere.
var errLockUnsupported = errors.New("regionkey: locking memory is not supported on this platform")

func lock(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return errLockUnsupported
}

func unlock(b []byte) error {
	return nil
}
