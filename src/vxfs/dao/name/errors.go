package name

import (
	"errors"
)

var (
	ErrNameClosed = errors.New("volume closed")

	ErrNameKeyExists = errors.New("name exists")
	ErrNameNotExists = errors.New("name not exists")

	ErrDataNoDiskSpace = errors.New("data no disk space")
	ErrDataFileClosed  = errors.New("data file closed")
	ErrDataHeadMagic   = errors.New("data head magic not match")
	ErrDataHeadVersion = errors.New("data head version not match")
	ErrDataBlockMagic  = errors.New("data block magic not match")
)
