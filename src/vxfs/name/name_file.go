package name

import (
	"sync"
)
import . "vxfs/dao/name"

type NameFile struct {
	Nid  int32
	Data *DataFile

	closed    bool
	wlock     sync.Mutex
	nameCache *NameCache
}

func NewNameFile(nid int32, nameCache *NameCache, dataFile string) (n *NameFile, err error) {
	n = &NameFile{
		Nid:       nid,
		nameCache: nameCache,
	}
	if n.Data, err = NewDataFile(dataFile); err != nil {
		n.Close()
		n = nil
		return
	}
	if err = n.init(); err != nil {
		n.Close()
		n = nil
		return
	}
	return
}

func (n *NameFile) init() (err error) {
	if err = n.Data.Recovery(func(name string, flag byte, sid int32, key int64, offset int64, size int32) (err error) {
		if flag == FlagOk {
			n.nameCache.Set(name, n.Nid, sid, key, offset, size)
		}
		return
	}); err != nil {
		return
	}
	return
}

func (n *NameFile) Write(req *WriteRequest) (k *NameBlock, err error) {
	if n.closed {
		err = ErrNameClosed
		return
	}

	var (
		offset int64
		size   int32
	)
	n.wlock.Lock()
	if offset, size, err = n.Data.Write(req.Name, req.Sid, req.Key); err != nil {
		n.wlock.Unlock()
		return
	}
	n.wlock.Unlock()
	k = n.nameCache.Set(req.Name, n.Nid, req.Sid, req.Key, offset, size)
	return
}

func (n *NameFile) Delete(k *NameBlock) (err error) {
	if n.closed {
		return ErrNameClosed
	}

	n.wlock.Lock()
	if err = n.Data.Delete(k.Offset); err != nil {
		n.wlock.Unlock()
		return
	}
	n.wlock.Unlock()
	return
}

func (n *NameFile) Close() {
	n.wlock.Lock()
	defer n.wlock.Unlock()

	n.closed = true
	n.nameCache = nil
	if n.Data != nil {
		n.Data.Close()
		n.Data = nil
	}
}
