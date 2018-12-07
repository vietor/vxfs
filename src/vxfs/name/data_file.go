package name

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"vxfs/libs"
	"vxfs/libs/glog"
)
import . "vxfs/dao/name"

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
// | magic number  | --- 4 bytes
// | store id      | --- 4 bytes
// | store key     | --- 8 bytes
// | flag          | --- 1 byte
// | padding size  | --- 1 bytes
// | name size     | --- 2 bytes
// | ~~~~~~~~~~~~~ |
// |    name ...   | --- 0~65534 bytes
// | ~~~~~~~~~~~~~ |
// | padding       | --- 0~7 bytes
// -----------------

const (
	dataHeadSize      = 16
	dataBlockHeadSize = 20

	FlagOk  = byte(0)
	FlagDel = byte(1)
)

var (
	dataHeadMagic       = []byte{0xff, 0x4e, 0x46, 0x49}
	dataHeadMagicSize   = len(dataHeadMagic)
	dataHeadVersion     = []byte{0x10}
	dataHeadVersionSize = len(dataHeadVersion)
	dataHeadPadding     = bytes.Repeat([]byte{0x00}, dataHeadSize-dataHeadMagicSize-dataHeadVersionSize)

	dataBlockHeadMagic     = []byte{0xff, 0x62, 0x6c, 0x6b}
	dataBlockHeadMagicSize = len(dataBlockHeadMagic)
	dataBlockFlagOffset    = dataBlockHeadMagicSize + 12
)

type DataFile struct {
	f *os.File

	File   string
	Size   int64
	Offset int64
}

func NewDataFile(file string) (d *DataFile, err error) {
	d = &DataFile{}
	d.File = file
	if d.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE|libs.O_NOATIME, libs.ModeFile); err != nil {
		glog.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		d.Close()
		d = nil
		return
	}
	if err = d.init(); err != nil {
		d.Close()
		d = nil
		return
	}
	return
}

func (d *DataFile) init() (err error) {
	var stat os.FileInfo
	if stat, err = d.f.Stat(); err != nil {
		glog.Errorf("DataFile: \"%s\" Stat() error(%v)", d.File, err)
		return
	}
	if d.Size = stat.Size(); d.Size == 0 {
		if err = d.writeHead(); err != nil {
			glog.Errorf("DataFile: \"%s\" writeHead() error(%v)", d.File, err)
			return
		}
		d.Size = dataHeadSize
	} else {
		if err = d.parseHead(); err != nil {
			glog.Errorf("DataFile: \"%s\" parseHead() error(%v)", d.File, err)
			return
		}
		if _, err = d.f.Seek(dataHeadSize, os.SEEK_SET); err != nil {
			glog.Errorf("DataFile: \"%s\" Seek() error(%v)", d.File, err)
			return
		}
	}
	d.Offset = dataHeadSize
	return
}

func (d *DataFile) flush() (err error) {
	if err = libs.Fdatasync(int(d.f.Fd())); err != nil {
		glog.Errorf("DataFile: \"%s\" Fdatasync() error(%v)", d.File, err)
		return
	}
	return
}

func (d *DataFile) writeHead() (err error) {
	var (
		cursor = 0
		header = make([]byte, dataHeadSize)
	)
	copy(header[cursor:], dataHeadMagic)
	cursor += dataHeadMagicSize
	copy(header[cursor:], dataHeadVersion)
	cursor += dataHeadVersionSize
	copy(header[cursor:], dataHeadPadding)
	if d.f.Write(header); err != nil {
		return
	}
	return
}

func (d *DataFile) parseHead() (err error) {
	var (
		cursor = 0
		header = make([]byte, dataHeadSize)
	)
	if _, err = d.f.Read(header); err != nil {
		return
	}
	if !bytes.Equal(header[cursor:cursor+dataBlockHeadMagicSize], dataHeadMagic) {
		return ErrDataHeadMagic
	}
	cursor += dataBlockHeadMagicSize
	if !bytes.Equal(header[cursor:cursor+dataHeadVersionSize], dataHeadVersion) {
		return ErrDataHeadVersion
	}
	return
}

func (d *DataFile) Write(name string, sid int32, skey uint64) (offset int64, size int32, err error) {
	var (
		cursor                 = 0
		nameBuffer             = []byte(name)
		nameSize               = len(nameBuffer)
		blockSize, paddingSize = libs.AlignSize(int32(dataBlockHeadSize+nameSize), 8)
		blockBuffer            = libs.AllocBuffer(blockSize)
	)
	defer libs.FreeBuffer(blockBuffer)

	copy(blockBuffer[cursor:], dataBlockHeadMagic)
	cursor += dataBlockHeadMagicSize
	binary.BigEndian.PutUint32(blockBuffer[cursor:], uint32(sid))
	cursor += 4
	binary.BigEndian.PutUint64(blockBuffer[cursor:], skey)
	cursor += 8
	blockBuffer[cursor] = byte(0)
	cursor += 1
	blockBuffer[cursor] = byte(paddingSize)
	cursor += 1
	binary.BigEndian.PutUint16(blockBuffer[cursor:], uint16(nameSize))
	cursor += 2
	copy(blockBuffer[cursor:], nameBuffer)

	offset = d.Offset
	size = blockSize

	if _, err = d.f.Write(blockBuffer[:blockSize]); err != nil {
		return
	}
	if err = d.flush(); err != nil {
		return
	}
	d.Offset += int64(blockSize)
	return
}

func (d *DataFile) Delete(offset int64) (err error) {
	_, err = d.f.WriteAt([]byte{FlagDel}, offset+int64(dataBlockFlagOffset))
	return
}

func (d *DataFile) Recovery(fn func(string, byte, int32, uint64, int64, int32) error) (err error) {
	var (
		name        string
		flag        byte
		sid         int32
		skey        uint64
		paddingSize int32
		nameSize    int32
		bodySize    int32
		bodyBuffer  []byte
		cursor      int
		blockBuffer = make([]byte, dataBlockHeadSize)
	)
	if _, err = d.f.Seek(dataHeadSize, os.SEEK_SET); err != nil {
		return
	}
	d.Offset = dataHeadSize
	for {
		if _, err = d.f.Read(blockBuffer); err != nil {
			if err != io.EOF {
				glog.Errorf("DataFile: \"%s\" Read (%d) error(%v)", d.File, dataBlockHeadSize, err)
			}
			break
		}

		cursor = 0
		if !bytes.Equal(blockBuffer[cursor:cursor+dataBlockHeadMagicSize], dataBlockHeadMagic) {
			err = ErrDataBlockMagic
			glog.Errorf("DataFile: \"%s\" Block parseHead (%d) error(%v)", d.File, dataBlockHeadSize, err)
			break
		}
		cursor += dataBlockHeadMagicSize
		sid = int32(binary.BigEndian.Uint32(blockBuffer[cursor:]))
		cursor += 4
		skey = binary.BigEndian.Uint64(blockBuffer[cursor:])
		cursor += 8
		flag = blockBuffer[cursor]
		cursor += 1
		paddingSize = int32(blockBuffer[cursor])
		cursor += 1
		nameSize = int32(binary.BigEndian.Uint16(blockBuffer[cursor:]))
		cursor += 2

		bodySize = nameSize + paddingSize
		if int32(len(bodyBuffer)) < bodySize {
			bodyBuffer = make([]byte, bodySize)
		}
		if _, err = d.f.Read(bodyBuffer[:bodySize]); err != nil {
			if err != io.EOF {
				glog.Errorf("DataFile: \"%s\" Block readBody (%d) error(%v)", d.File, dataBlockHeadSize, err)
			}
			break
		}
		name = string(bodyBuffer[:nameSize])
		if err = fn(name, flag, sid, skey, d.Offset, dataBlockHeadSize+bodySize); err != nil {
			glog.Errorf("DataFile: \"%s\" callback (%d,%d,%d,%d) error(%v)", d.File, name, flag, sid, skey, err)
			break
		}
		d.Offset += int64(dataBlockHeadSize + bodySize)
	}
	if err == io.EOF {
		err = nil
	} else if err != nil {
		return
	}
	if _, err = d.f.Seek(d.Offset, os.SEEK_SET); err != nil {
		glog.Errorf("DataFile: \"%s\" Seek(%d) error(%v)", d.File, d.Offset, err)
		return
	}
	if d.Size != d.Offset {
		if err = d.f.Truncate(d.Offset); err != nil {
			glog.Errorf("DataFile: \"%s\" Truncate(%d) error(%v)", d.File, d.Offset, err)
			return
		}
		d.Size = d.Offset
	}
	return
}

func (d *DataFile) Close() {
	var err error
	if d.f != nil {
		d.flush()
		if err = d.f.Sync(); err != nil {
			glog.Errorf("DataFile: \"%s\" Sync() error(%v)", d.File, err)
		}
		if err = d.f.Close(); err != nil {
			glog.Errorf("DataFile: \"%s\" Close() error(%v)", d.File, err)
		}
		d.f = nil
	}
}
