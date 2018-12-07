package libs

import (
	"io/ioutil"
	"os"
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
	if f, err = os.OpenFile(file, os.O_RDONLY|O_NOATIME, 0644); err != nil {
		return
	}
	defer f.Close()

	return readTextFile(f)
}
