package libs

import (
	"sync"
	"time"
)

type VxTicker struct {
	r      bool
	d      time.Duration
	f      func()
	wg     sync.WaitGroup
	done   chan bool
	ticker *time.Ticker
}

func NewVxTicker(f func(), d time.Duration) (t *VxTicker) {
	t = &VxTicker{
		f:    f,
		d:    d,
		done: make(chan bool, 1),
	}
	return
}

func (t *VxTicker) Start() {
	t.wg.Add(1)
	t.ticker = time.NewTicker(t.d)

	go t.run()
}

func (t *VxTicker) Tick() {
	if !t.r {
		t.r = true
		t.f()
		t.r = false
	}
}

func (t *VxTicker) run() {
	defer t.wg.Done()

	for {
		select {
		case <-t.ticker.C:
			t.Tick()
		case <-t.done:
			return
		}
	}
}

func (t *VxTicker) Stop() {
	t.done <- true

	t.wg.Wait()
	if t.ticker != nil {
		t.ticker.Stop()
		t.ticker = nil
	}
}
