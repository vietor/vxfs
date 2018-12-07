package libs

import (
	"math/rand"
	"sync"
	"time"
)

type VxRand struct {
	r *rand.Rand
	l sync.Mutex
}

var Rand = NewVxRand()

func NewVxRand() *VxRand {
	return &VxRand{
		r: rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func (r *VxRand) Int() int {
	r.l.Lock()
	defer r.l.Unlock()

	return r.r.Int()
}

func (r *VxRand) Intn(n int) int {
	r.l.Lock()
	defer r.l.Unlock()

	return r.r.Intn(n)
}
