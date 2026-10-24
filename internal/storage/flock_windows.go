//go:build windows

package storage

import (
	"os"

	"golang.org/x/sys/windows"
)

// DirLock holds a file descriptor for directory exclusivity.
type DirLock struct {
	file *os.File
}

// AcquireDirLock attempts to acquire an exclusive lock on path.
func AcquireDirLock(path string) (*DirLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	handle := windows.Handle(f.Fd())
	var overlapped windows.Overlapped
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	err = windows.LockFileEx(handle, flags, 0, 1, 0, &overlapped)
	if err != nil {
		_ = f.Close()
		return nil, ErrDirectoryLocked
	}
	return &DirLock{file: f}, nil
}

// Release releases the lock and closes the lock file.
func (l *DirLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	handle := windows.Handle(l.file.Fd())
	var overlapped windows.Overlapped
	_ = windows.UnlockFileEx(handle, 0, 1, 0, &overlapped)
	return l.file.Close()
}
