package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"vxfs/libs"
	"vxfs/libs/glog"
)
import . "vxfs/dao/store"

// file
// -----------------
// | magic number  | --- 4 bytes
// | version       | --- 1 byte
// | padding       | --- 11 byte
// | ~~~~~~~~~~~~~ |
// | block ...     |
// -----------------

// block
// -----------------
// | key           | --- 8 bytes
// | offset        | --- 8 byte
// | size          | --- 4 byte
// -----------------

const (
	indexHeadSize  = 16
	indexBlockSize = 20
)

var (
	indexHeadMagic       = []byte{0xff, 0x56, 0x46, 0x49}
	indexHeadMagicSize   = len(indexHeadMagic)
	indexHeadVersion     = []byte{0x10}
	indexHeadVersionSize = len(indexHeadVersion)
	indexHeadPadding     = bytes.Repeat([]byte{0x00}, indexHeadSize-indexHeadMagicSize-indexHeadVersionSize)
)

type IndexFile struct {
	f      *os.File
	closed bool
	File   string
	Size   int64
	Offset int64
}

func NewIndexFile(file string) (i *IndexFile, err error) {
	i = &IndexFile{}
	i.File = file
	if i.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE|libs.O_NOATIME, 0644); err != nil {
		glog.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		i.Close()
		i = nil
		return
	}
	if err = i.init(); err != nil {
		i.Close()
		i = nil
		return
	}
	return
}

func (i *IndexFile) init() (err error) {
	var stat os.FileInfo
	if stat, err = i.f.Stat(); err != nil {
		glog.Errorf("IndexFile: \"%s\" Stat() error(%v)", i.File, err)
		return
	}
	if i.Size = stat.Size(); i.Size == 0 {
		if err = i.writeHead(); err != nil {
			glog.Errorf("IndexFile: \"%s\" writeHead() error(%v)", i.File, err)
			return
		}
		i.Size = indexHeadSize
	} else {
		if err = i.parseHead(); err != nil {
			glog.Errorf("IndexFile: \"%s\" parseHead() error(%v)", i.File, err)
			return
		}
		if _, err = i.f.Seek(indexHeadSize, os.SEEK_SET); err != nil {
			glog.Errorf("IndexFile: \"%s\" Seek() error(%v)", i.File, err)
			return
		}
	}
	i.Offset = indexHeadSize
	return
}

func (i *IndexFile) writeHead() (err error) {
	var (
		cursor = 0
		header = make([]byte, indexHeadSize)
	)
	copy(header[cursor:], indexHeadMagic)
	cursor += indexHeadMagicSize
	copy(header[cursor:], indexHeadVersion)
	cursor += indexHeadVersionSize
	copy(header[cursor:], indexHeadPadding)
	if i.f.Write(header); err != nil {
		return
	}
	return
}

func (i *IndexFile) parseHead() (err error) {
	var (
		cursor = 0
		header = make([]byte, indexHeadSize)
	)
	if _, err = i.f.Read(header); err != nil {
		return
	}
	if !bytes.Equal(header[cursor:cursor+indexHeadMagicSize], indexHeadMagic) {
		return ErrIndexHeadMagic
	}
	cursor += indexHeadMagicSize
	if !bytes.Equal(header[cursor:cursor+indexHeadVersionSize], indexHeadVersion) {
		return ErrIndexHeadVersion
	}
	return
}

func (i *IndexFile) Write(key uint64, offset int64, size int32) (err error) {
	if i.closed {
		return ErrIndexFileClosed
	}

	var (
		cursor      = 0
		blockBuffer = make([]byte, indexBlockSize)
	)
	binary.BigEndian.PutUint64(blockBuffer[cursor:], key)
	cursor += 8
	binary.BigEndian.PutUint64(blockBuffer[cursor:], uint64(offset))
	cursor += 8
	binary.BigEndian.PutUint32(blockBuffer[cursor:], uint32(size))
	if _, err = i.f.Write(blockBuffer); err != nil {
		return
	}
	if err = i.flush(); err != nil {
		return
	}
	i.Offset += indexBlockSize
	return
}

func (i *IndexFile) flush() (err error) {
	if err = libs.Fdatasync(int(i.f.Fd())); err != nil {
		glog.Errorf("IndexFile: \"%s\" Fdatasync() error(%v)", i.File, err)
		return
	}
	return
}

func (i *IndexFile) Flush() (err error) {
	return i.flush()
}

func (i *IndexFile) Recovery(fn func(uint64, int64, int32) error) (err error) {
	if i.closed {
		return ErrIndexFileClosed
	}
	var (
		key         uint64
		offset      int64
		size        int32
		cursor      int
		blockBuffer = make([]byte, indexBlockSize)
	)
	if _, err = i.f.Seek(indexHeadSize, os.SEEK_SET); err != nil {
		return
	}
	i.Offset = indexHeadSize
	for {
		if _, err = i.f.Read(blockBuffer); err != nil {
			if err != io.EOF {
				glog.Errorf("IndexFile: \"%s\" Read (%d) error(%v)", i.File, indexBlockSize, err)
			}
			break
		}
		cursor = 0
		key = binary.BigEndian.Uint64(blockBuffer[cursor:])
		cursor += 8
		offset = int64(binary.BigEndian.Uint64(blockBuffer[cursor:]))
		cursor += 8
		size = int32(binary.BigEndian.Uint32(blockBuffer[cursor:]))
		if err = fn(key, offset, size); err != nil {
			glog.Errorf("IndexFile: \"%s\" callback (%d,%d,%d,%d) error(%v)", i.File, key, offset, size, err)
			break
		}
		i.Offset += indexBlockSize
	}
	if err == io.EOF {
		err = nil
	} else if err != nil {
		return
	}
	if _, err = i.f.Seek(i.Offset, os.SEEK_SET); err != nil {
		glog.Errorf("IndexFile: \"%s\" Seek(%d) error(%v)", i.File, i.Offset, err)
		return
	}
	if i.Size != i.Offset {
		if err = i.f.Truncate(i.Offset); err != nil {
			glog.Errorf("IndexFile: \"%s\" Truncate(%d) error(%v)", i.File, i.Offset, err)
			return
		}
		i.Size = i.Offset
	}
	return
}

func (i *IndexFile) Close() {
	var err error
	if i.f != nil {
		i.flush()
		if err = i.f.Sync(); err != nil {
			glog.Errorf("IndexFile: \"%s\" Sync() error(%v)", i.File, err)
		}
		if err = i.f.Close(); err != nil {
			glog.Errorf("IndexFile: \"%s\" Close() error(%v)", i.File, err)
		}
		i.f = nil
	}
	i.closed = true
}
