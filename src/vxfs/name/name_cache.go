package name

import "sync"

type NameBlock struct {
	Nid    int32
	Sid    int32
	Key    uint64
	Offset int64
}

type NameCache struct {
	rwlock sync.RWMutex
	blocks map[string]*NameBlock
}

func NewNameCache() (c *NameCache) {
	c = &NameCache{}
	c.blocks = make(map[string]*NameBlock)
	return
}

func (c *NameCache) Get(name string) (k *NameBlock) {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	k, _ = c.blocks[name]
	return
}

func (c *NameCache) Set(name string, nid int32, vid int32, key uint64, offset int64, size int32) (k *NameBlock) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	k = &NameBlock{nid, vid, key, offset}
	c.blocks[name] = k
	return
}

func (c *NameCache) Del(name string) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	delete(c.blocks, name)
}
