//go:build !windows

package storage

import (
	"os"
	"syscall"
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
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
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
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	return l.file.Close()
}
