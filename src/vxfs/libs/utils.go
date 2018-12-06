package libs

import (
	"os"
	"sync"
	"syscall"
)

var (
	_pageSize = syscall.Getpagesize()
	_bufPool  = sync.Pool{
		New: func() interface{} {
			return make([]byte, _pageSize)
		},
	}
)

func AlignSize(s int32, d int32) (int32, int32) {
	var n = (s + (d - 1)) & ^(d - 1)
	return n, n - s
}

func AllocBuffer(size int32) []byte {
	if size > int32(_pageSize) {
		return make([]byte, size)
	} else {
		return _bufPool.Get().([]byte)
	}
}

func FreeBuffer(buffer []byte) {
	if buffer != nil && len(buffer) <= _pageSize {
		_bufPool.Put(buffer)
	}
}

func CloneBuffer(buffer []byte) []byte {
	tmp := make([]byte, len(buffer))
	copy(tmp, buffer)
	return tmp
}

func TestWriteDir(filename string) (err error) {
	var i os.FileInfo
	if i, err = os.Lstat(filename); err != nil {
		return
	}
	if !i.IsDir() || i.Mode().Perm()&(1<<7) == 0 {
		err = os.ErrPermission
	}
	return
}

func TestWriteFile(filename string) (err error) {
	var i os.FileInfo
	if i, err = os.Lstat(filename); err != nil {
		return
	}
	if i.IsDir() || i.Mode().Perm()&(1<<7) == 0 {
		err = os.ErrPermission
	}
	return
}
