package libs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type OnProcessExit func()

type ProcessLock struct {
	File string
	Body string

	f *os.File
}

func NewProcessLock(file string, body string) *ProcessLock {
	if strings.HasSuffix(file, "/") {
		file += "vxfs.lock"
	}
	return &ProcessLock{
		File: file,
		Body: body,
	}
}

func (l *ProcessLock) Lock() (err error) {
	if err = os.MkdirAll(filepath.Dir(l.File), 0777); err != nil {
		return
	}
	if l.f, err = os.OpenFile(l.File, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return
	}
	if err = Flock(int(l.f.Fd())); err != nil {
		l.f.Close()
		l.f = nil
		emsg := "file already locked"
		body, err1 := ReadTextFile(l.File)
		if err1 == nil && len(body) > 0 {
			emsg += " by (" + body + ")"
		}
		err = errors.New(emsg)
		return
	}
	stat, _ := l.f.Stat()
	if stat.Size() < 1 {
		l.f.Write([]byte(l.Body))
		l.f.Sync()
	}
	return
}

func (l *ProcessLock) Unlock() {
	if l.f != nil {
		Unflock(int(l.f.Fd()))
		l.f.Close()
		l.f = nil
	}
	return
}
