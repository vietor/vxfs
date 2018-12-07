package libs

import (
	"io/ioutil"
	"os"
)

const (
	ModeFile = 0644
)

func readTextFile(f *os.File) (body string, err error) {
	var b []byte
	if b, err = ioutil.ReadAll(f); err != nil {
		return
	}
	body = string(b)
	return
}

func ReadTextFile(file string) (body string, err error) {
	var f *os.File
	if f, err = os.OpenFile(file, os.O_RDONLY|O_NOATIME, ModeFile); err != nil {
		return
	}
	defer f.Close()

	return readTextFile(f)
}
