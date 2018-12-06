package store

import (
	"errors"
)

var (
	ErrVolumeClosed = errors.New("volume closed")

	ErrStoreKeyExists = errors.New("store exists")
	ErrStoreNotExists = errors.New("store not exists")

	ErrIndexNoDiskSpace = errors.New("index no disk space")
	ErrIndexFileClosed  = errors.New("index file closed")
	ErrIndexHeadMagic   = errors.New("index head magic not match")
	ErrIndexHeadVersion = errors.New("index head version not match")
	ErrIndexBlockOffset = errors.New("index block offset failed")
	ErrIndexBlockSize   = errors.New("index block size failed")

	ErrDataNoDiskSpace = errors.New("data no disk space")
	ErrDataFileClosed  = errors.New("data file closed")
	ErrDataHeadMagic   = errors.New("data head magic not match")
	ErrDataHeadVersion = errors.New("data head version not match")
	ErrDataBlockMagic  = errors.New("data block magic not match")
	ErrDataBlockSizes  = errors.New("data block sizes failed")
)
