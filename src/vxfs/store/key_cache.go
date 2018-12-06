package store

import "sync"

type KeyBlock struct {
	Vid    int32
	Size   int32
	Offset int64
}

type KeyCache struct {
	rwlock sync.RWMutex
	blocks map[uint64]*KeyBlock
}

func NewKeyCache() (c *KeyCache) {
	c = &KeyCache{}
	c.blocks = make(map[uint64]*KeyBlock)
	return
}

func (c *KeyCache) Get(key uint64) (k *KeyBlock) {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	k, _ = c.blocks[key]
	return
}

func (c *KeyCache) Set(key uint64, vid int32, offset int64, size int32) (k *KeyBlock) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	k = &KeyBlock{vid, size, offset}
	c.blocks[key] = k
	return
}

func (c *KeyCache) Del(key uint64) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	delete(c.blocks, key)
}
