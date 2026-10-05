package torrent

import (
	"errors"
	"os"
	"syscall"
)

// freeSpace returns the bytes available to unprivileged users on dir's
// filesystem. The bool is false if statfs failed, in which case callers
// skip the check rather than block startup.
func freeSpace(dir string) (uint64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return uint64(st.Bavail) * uint64(st.Bsize), true
}

// allocate reserves size bytes for f using fallocate(2), leaving existing
// data untouched. Filesystems without support (ZFS, some FUSE/network
// mounts) get a zero-fill instead.
func allocate(f *os.File, size int64) error {
	err := syscall.Fallocate(int(f.Fd()), 0, 0, size)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EOPNOTSUPP), errors.Is(err, syscall.ENOSYS), errors.Is(err, syscall.EINVAL):
		return allocateFallback(f, size)
	default:
		return err // includes ENOSPC
	}
}

// allocateFallback zero-fills from the current end of file up to size. It
// never overwrites existing bytes, but must run before concurrent writers
// start (OpenStorage guarantees that).
func allocateFallback(f *os.File, size int64) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	zeros := make([]byte, 1<<20)
	for off := st.Size(); off < size; {
		n := int(min(int64(len(zeros)), size-off))
		if _, err := f.WriteAt(zeros[:n], off); err != nil {
			return err
		}
		off += int64(n)
	}
	return nil
}
