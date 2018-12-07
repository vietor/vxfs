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
// | magic number  | --- 4 bytes
// | key           | --- 8 bytes
// | flag          | --- 1 byte
// | padding size  | --- 1 bytes
// | meta size     | --- 2 bytes
// | data size     | --- 4 bytes
// | ~~~~~~~~~~~~~ |
// |    meta ...   | --- 0~65534 bytes
// | ~~~~~~~~~~~~~ |
// |    data ...   |
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
	dataHeadMagic       = []byte{0xff, 0x56, 0x46, 0x44}
	dataHeadMagicSize   = len(dataHeadMagic)
	dataHeadVersion     = []byte{0x10}
	dataHeadVersionSize = len(dataHeadVersion)
	dataHeadPadding     = bytes.Repeat([]byte{0x00}, dataHeadSize-dataHeadMagicSize-dataHeadVersionSize)

	dataBlockHeadMagic     = []byte{0xff, 0x62, 0x6c, 0x6b}
	dataBlockHeadMagicSize = len(dataBlockHeadMagic)
	dataBlockFlagOffset    = dataBlockHeadMagicSize + 8
)

type DataFile struct {
	r *os.File
	w *os.File

	File   string
	Size   int64
	Offset int64
}

func NewDataFile(file string) (d *DataFile, err error) {
	d = &DataFile{}
	d.File = file
	if d.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE|libs.O_NOATIME, libs.ModeFile); err != nil {
		glog.Errorf("os.OpenFile(\"%s\") WRITE error(%v)", file, err)
		d.Close()
		d = nil
		return
	}
	if d.r, err = os.OpenFile(file, os.O_RDONLY|libs.O_NOATIME, libs.ModeFile); err != nil {
		glog.Errorf("os.OpenFile(\"%s\") READ error(%v)", file, err)
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
	if stat, err = d.r.Stat(); err != nil {
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
		if _, err = d.w.Seek(dataHeadSize, os.SEEK_SET); err != nil {
			glog.Errorf("DataFile: \"%s\" Seek() error(%v)", d.File, err)
			return
		}
	}
	d.Offset = dataHeadSize
	return
}

func (d *DataFile) flush() (err error) {
	if err = libs.Fdatasync(int(d.w.Fd())); err != nil {
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
	if d.w.Write(header); err != nil {
		return
	}
	return
}

func (d *DataFile) parseHead() (err error) {
	var (
		cursor = 0
		header = make([]byte, dataHeadSize)
	)
	if _, err = d.r.Read(header); err != nil {
		return
	}
	if !bytes.Equal(header[cursor:cursor+dataHeadMagicSize], dataHeadMagic) {
		return ErrDataHeadMagic
	}
	cursor += dataHeadMagicSize
	if !bytes.Equal(header[cursor:cursor+dataHeadVersionSize], dataHeadVersion) {
		return ErrDataHeadVersion
	}
	return
}

func (d *DataFile) Read(offset int64, size int32) (key uint64, flag byte, meta []byte, data []byte, err error) {
	var (
		cursor      = 0
		metaSize    int32
		dataSize    int32
		paddingSize int32
		blockBuffer = make([]byte, size)
	)

	if _, err = d.r.ReadAt(blockBuffer, offset); err != nil {
		return
	}
	if !bytes.Equal(blockBuffer[cursor:cursor+dataBlockHeadMagicSize], dataBlockHeadMagic) {
		err = ErrDataBlockMagic
		return
	}
	cursor += dataBlockHeadMagicSize
	key = binary.BigEndian.Uint64(blockBuffer[cursor:])
	cursor += 8
	flag = blockBuffer[cursor]
	cursor += 1
	paddingSize = int32(blockBuffer[cursor])
	cursor += 1
	metaSize = int32(binary.BigEndian.Uint16(blockBuffer[cursor:]))
	cursor += 2
	dataSize = int32(binary.BigEndian.Uint32(blockBuffer[cursor:]))
	cursor += 4
	if dataBlockHeadSize+metaSize+dataSize+paddingSize != size {
		err = ErrDataBlockSizes
		return
	}
	if metaSize > 0 {
		meta = blockBuffer[cursor : cursor+int(metaSize)]
		cursor += int(metaSize)
	}
	data = blockBuffer[cursor : cursor+int(dataSize)]
	return
}

func (d *DataFile) Write(key uint64, meta []byte, data []byte) (offset int64, size int32, err error) {
	var (
		cursor                 = 0
		metaSize               = len(meta)
		dataSize               = len(data)
		blockSize, paddingSize = libs.AlignSize(int32(dataBlockHeadSize+metaSize+dataSize), 8)
		blockBuffer            = libs.AllocBuffer(blockSize)
	)
	defer libs.FreeBuffer(blockBuffer)

	copy(blockBuffer[cursor:], dataBlockHeadMagic)
	cursor += dataBlockHeadMagicSize
	binary.BigEndian.PutUint64(blockBuffer[cursor:], key)
	cursor += 8
	blockBuffer[cursor] = FlagOk
	cursor += 1
	blockBuffer[cursor] = byte(paddingSize)
	cursor += 1
	binary.BigEndian.PutUint16(blockBuffer[cursor:], uint16(metaSize))
	cursor += 2
	binary.BigEndian.PutUint32(blockBuffer[cursor:], uint32(dataSize))
	cursor += 4
	if metaSize > 0 {
		copy(blockBuffer[cursor:], meta)
		cursor += metaSize
	}
	copy(blockBuffer[cursor:], data)

	offset = d.Offset
	size = blockSize

	if d.w.Write(blockBuffer[:blockSize]); err != nil {
		return
	}
	if err = d.flush(); err != nil {
		return
	}
	d.Offset += int64(blockSize)
	if d.Size < d.Offset {
		d.Size = d.Offset
	}
	return
}

func (d *DataFile) Delete(offset int64) (err error) {
	_, err = d.w.WriteAt([]byte{FlagDel}, offset+int64(dataBlockFlagOffset))
	return
}

func (d *DataFile) Recovery(offset int64, fn func(uint64, byte, int64, int32) error) (err error) {
	var (
		key         uint64
		flag        byte
		cursor      int
		bSize       int32
		metaSize    int32
		dataSize    int32
		paddingSize int32
		blockBuffer = make([]byte, dataBlockHeadSize)
	)
	if offset <= 0 {
		offset = dataHeadSize
	}
	if _, err = d.r.Seek(offset, os.SEEK_SET); err != nil {
		return
	}
	d.Offset = offset
	for {
		if _, err = d.r.Read(blockBuffer); err != nil {
			if err != io.EOF {
				glog.Errorf("DataFile: \"%s\" Read (%d) error(%v)", d.File, dataBlockHeadSize, err)
			}
			break
		}

		cursor = 0
		if !bytes.Equal(blockBuffer[cursor:cursor+dataBlockHeadMagicSize], dataBlockHeadMagic) {
			err = ErrDataBlockMagic
			glog.Errorf("DataFile: \"%s\" Block parseHead error(%v)", d.File, err)
			break
		}
		cursor += dataBlockHeadMagicSize
		key = binary.BigEndian.Uint64(blockBuffer[cursor:])
		cursor += 8
		flag = blockBuffer[cursor]
		cursor += 1
		paddingSize = int32(blockBuffer[cursor])
		cursor += 1
		metaSize = int32(binary.BigEndian.Uint16(blockBuffer[cursor:]))
		cursor += 2
		dataSize = int32(binary.BigEndian.Uint32(blockBuffer[cursor:]))
		cursor += 4
		bSize = dataBlockHeadSize + metaSize + dataSize + paddingSize
		if err = fn(key, flag, d.Offset, bSize); err != nil {
			glog.Errorf("DataFile: \"%s\" callback (%d,%d,%d,%d) error(%v)", d.File, key, flag, d.Offset, bSize, err)
			break
		}

		if _, err = d.r.Seek(d.Offset+int64(bSize), os.SEEK_SET); err != nil {
			glog.Errorf("DataFile: \"%s\" Seek(%d) error(%v)", d.File, offset, err)
			break
		}
		d.Offset += int64(bSize)
	}
	if err == io.EOF {
		err = nil
	} else if err != nil {
		return
	}
	if _, err = d.w.Seek(d.Offset, os.SEEK_SET); err != nil {
		glog.Errorf("DataFile: \"%s\" Seek(%d) error(%v)", d.File, d.Offset, err)
		return
	}
	if d.Size != d.Offset {
		if err = d.w.Truncate(d.Offset); err != nil {
			glog.Errorf("DataFile: \"%s\" Truncate(%d) error(%v)", d.File, d.Offset, err)
			return
		}
		d.Size = d.Offset
	}
	return
}

func (d *DataFile) Close() {
	var err error
	if d.w != nil {
		d.flush()
		if err = d.w.Sync(); err != nil {
			glog.Errorf("DataFile: \"%s\" Sync() error(%v)", d.File, err)
		}
		if err = d.w.Close(); err != nil {
			glog.Errorf("DataFile: \"%s\" Close() error(%v)", d.File, err)
		}
		d.w = nil
	}
	if d.r != nil {
		if err = d.r.Close(); err != nil {
			glog.Errorf("DataFile: \"%s\" Close() error(%v)", d.File, err)
		}
		d.r = nil
	}
}
