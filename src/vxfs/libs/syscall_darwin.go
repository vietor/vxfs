// +build darwin

package libs

const (
	O_NOATIME = 0
)

func Fdatasync(fd int) (err error) {
	return
}
