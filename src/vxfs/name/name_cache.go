package name

import (
	"crypto/sha256"
	"encoding/base64"
	"sync"
)

type NameBlock struct {
	Nid    int32
	Sid    int32
	Key    int64
	Offset int64
}

type NameCache struct {
	rwlock sync.RWMutex
	blocks map[string]NameBlock
}

func NewNameCache() (c *NameCache) {
	c = &NameCache{}
	c.blocks = make(map[string]NameBlock)
	return
}

func (c *NameCache) toKey(name string) string {
	if len(name) < 50 {
		return name
	}
	s := sha256.Sum224([]byte(name))
	return "S/" + base64.RawStdEncoding.EncodeToString(s[:])
}

func (c *NameCache) Get(name string) (k *NameBlock) {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	if d, ok := c.blocks[c.toKey(name)]; ok {
		k = &d
	}
	return
}

func (c *NameCache) Set(name string, nid int32, sid int32, key int64, offset int64, size int32) (k *NameBlock) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	k = &NameBlock{
		Nid:    nid,
		Sid:    sid,
		Key:    key,
		Offset: offset,
	}
	c.blocks[c.toKey(name)] = *k
	return
}

func (c *NameCache) Del(name string) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	delete(c.blocks, c.toKey(name))
}
