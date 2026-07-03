//go:build unix

package regionkey

import "golang.org/x/sys/unix"

// lock pins b's backing pages in RAM with mlock(2) so the KEK they hold cannot
// be paged to swap. mlock operates at page granularity, so this locks the whole
// page(s) the slice spans; over-locking neighbouring bytes is harmless for the
// no-swap guarantee. Go's garbage collector does not move heap allocations, so
// the pinned pages stay valid for the buffer's lifetime.
//
// mlock fails if the process would exceed RLIMIT_MEMLOCK; a host running the
// region provider must raise that limit (for a container, grant CAP_IPC_LOCK or
// set --ulimit memlock). newKEK surfaces the failure rather than proceeding
// with an unlocked key.
func lock(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Mlock(b)
}

// unlock reverses lock with munlock(2), allowing the pages to be paged again.
func unlock(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munlock(b)
}
