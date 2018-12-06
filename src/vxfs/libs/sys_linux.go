// +build linux

package libs

import (
	"syscall"
)

const (
	O_NOATIME = syscall.O_NOATIME
)

func Fdatasync(fd int) (err error) {
	return syscall.Fdatasync(fd)

}
