package name

import (
	"errors"
)

var (
	ErrNameClosed = errors.New("volume closed")

	ErrNameExists    = errors.New("name exists")
	ErrNameNotExists = errors.New("name not exists")

	ErrDataNoSpace     = errors.New("data no disk space")
	ErrDataHeadMagic   = errors.New("data head magic not match")
	ErrDataHeadVersion = errors.New("data head version not match")
	ErrDataBlockMagic  = errors.New("data block magic not match")
)
