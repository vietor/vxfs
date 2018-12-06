package libs

import (
	"io/ioutil"
	"os"
)

func ReadTextFile(file string) (body string, err error) {
	var (
		b []byte
		f *os.File
	)
	if f, err = os.Open(file); err != nil {
		return
	}
	defer f.Close()

	if b, err = ioutil.ReadAll(f); err != nil {
		return
	}
	body = string(b)
	return
}
