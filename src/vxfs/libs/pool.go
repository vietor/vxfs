package libs

import (
	"container/list"
	"sync"
)

type VxPool struct {
	limit int
	clean func(interface{})

	lock sync.Mutex
	data *list.List
}

func NewVxPool(limit int, clean func(interface{})) (p *VxPool) {
	if limit < 1 {
		limit = 1
	}
	p = &VxPool{
		limit: limit,
		clean: clean,
		data:  list.New(),
	}
	return
}

func (p *VxPool) Get() (x interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()

	e := p.data.Front()
	if e != nil {
		x = p.data.Remove(e)
	}
	return
}

func (p *VxPool) Put(x interface{}) {
	if x == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.data.PushBack(x)
	for p.data.Len() > p.limit {
		e := p.data.Front()
		v := p.data.Remove(e)
		if p.clean != nil {
			p.clean(v)
		}
	}
}
