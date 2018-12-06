package libs

import (
	"errors"
	"syscall"
)

func Flock(fd int) (err error) {
	return syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
}

func Unflock(fd int) error {
	return syscall.Flock(fd, syscall.LOCK_UN)
}

func GetDiskFreeSpace(path string, unit int) (size uint64, err error) {
	fs := syscall.Statfs_t{}
	if err = syscall.Statfs(path, &fs); err != nil {
		return
	}
	if unit == 0 { // Bytes
		size = uint64(fs.Bsize) * fs.Bfree
	} else if unit == 1 { // KB
		size = uint64(float64(fs.Bsize) / float64(1024) * float64(fs.Bfree))
	} else if unit == 2 { // MB
		size = uint64(float64(fs.Bsize) / float64(1024*1024) * float64(fs.Bfree))
	} else if unit == 3 { // GB
		size = uint64(float64(fs.Bsize) / float64(1024*1024*1024) * float64(fs.Bfree))
	} else {
		err = errors.New("GetDiskFreeSpace, unsupported unit (0-3)")
	}
	return
}
