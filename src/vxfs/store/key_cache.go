package store

import "sync"

type KeyBlock struct {
	Vid    int64
	Offset int64
	Size   int32
}

type KeyCache struct {
	rwlock sync.RWMutex
	blocks map[int64]*KeyBlock
}

func NewKeyCache() (c *KeyCache) {
	c = &KeyCache{}
	c.blocks = make(map[int64]*KeyBlock)
	return
}

func (c *KeyCache) Get(key int64) (k *KeyBlock) {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	k, _ = c.blocks[key]
	return
}

func (c *KeyCache) Set(key int64, vid int64, offset int64, size int32) (k *KeyBlock) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	k = &KeyBlock{vid, offset, size}
	c.blocks[key] = k
	return
}

func (c *KeyCache) Del(key int64) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	delete(c.blocks, key)
}
