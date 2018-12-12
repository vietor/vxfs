package store

import (
	"sync"
)
import . "vxfs/dao/store"

type VolumeFile struct {
	Vid   int32
	Data  *DataFile
	Index *IndexFile

	closed   bool
	wlock    sync.Mutex
	keyCache *KeyCache
}

func NewVolumeFile(vid int32, keyCache *KeyCache, dataFile string, indexFile string) (v *VolumeFile, err error) {
	v = &VolumeFile{
		Vid:      vid,
		keyCache: keyCache,
	}
	if v.Data, err = NewDataFile(dataFile); err != nil {
		v.Close()
		v = nil
		return
	}
	if v.Index, err = NewIndexFile(indexFile); err != nil {
		v.Close()
		v = nil
		return
	}
	if err = v.init(); err != nil {
		v.Close()
		v = nil
		return
	}
	return
}

func (v *VolumeFile) init() (err error) {
	var (
		dataOffset int64 = 0
	)
	if err = v.Index.Recovery(func(key int64, offset int64, size int32) (err error) {
		if offset < dataOffset {
			return ErrIndexBlockOffset
		}
		dataOffset = offset + int64(size)
		if dataOffset > v.Data.Size {
			return ErrIndexBlockSize
		}
		v.keyCache.Set(key, v.Vid, offset, size)
		return
	}); err != nil {
		return
	}
	if err = v.Data.Recovery(dataOffset, func(key int64, flag byte, offset int64, size int32) (err error) {
		if err = v.Index.Write(key, offset, size); err != nil {
			return
		}
		if flag == FlagOk {
			v.keyCache.Set(key, v.Vid, offset, size)
		}
		return
	}); err != nil {
		return
	}
	if err = v.Index.Flush(); err != nil {
		return
	}
	return
}

func (v *VolumeFile) Read(k *KeyBlock, res *ReadResponse) (err error) {
	if v.closed {
		err = ErrVolumeClosed
		return
	}

	var (
		key  int64
		flag byte
		meta []byte
		data []byte
	)
	if key, flag, meta, data, err = v.Data.Read(k.Offset, k.Size); err != nil {
		return
	}
	if flag != FlagOk {
		v.keyCache.Del(key)
		err = ErrStoreNotExists
		return
	}
	res.Meta = meta
	res.Data = data
	return
}

func (v *VolumeFile) Write(req *WriteRequest) (k *KeyBlock, err error) {
	if v.closed {
		err = ErrVolumeClosed
		return
	}

	var (
		offset int64
		size   int32
	)
	v.wlock.Lock()
	if offset, size, err = v.Data.Write(req.Key, req.Meta, req.Data); err != nil {
		v.wlock.Unlock()
		return
	}
	if err = v.Index.Write(req.Key, offset, size); err != nil {
		v.wlock.Unlock()
		return
	}
	v.wlock.Unlock()
	k = v.keyCache.Set(req.Key, v.Vid, offset, size)
	return
}

func (v *VolumeFile) Delete(k *KeyBlock) (err error) {
	if v.closed {
		return ErrVolumeClosed
	}

	v.wlock.Lock()
	if err = v.Data.Delete(k.Offset); err != nil {
		v.wlock.Unlock()
		return
	}
	v.wlock.Unlock()
	return
}

func (v *VolumeFile) Close() {
	v.wlock.Lock()
	defer v.wlock.Unlock()

	v.closed = true
	v.keyCache = nil
	if v.Data != nil {
		v.Data.Close()
		v.Data = nil
	}
	if v.Index != nil {
		v.Index.Close()
		v.Index = nil
	}
}
