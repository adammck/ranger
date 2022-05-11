package fake_node

import (
	"sync"
)

type barrier struct {
	arrival *sync.WaitGroup
	release *sync.WaitGroup
	cb      func()
}

func NewBarrier(n int, cb func()) *barrier {
	a := &sync.WaitGroup{}
	b := &sync.WaitGroup{}
	a.Add(n)
	b.Add(n)

	return &barrier{a, b, cb}
}

func (b *barrier) Wait() {
	b.arrival.Wait()
}

func (b *barrier) Release() {
	b.release.Done()
	b.cb()
}

func (b *barrier) Arrive() {
	b.arrival.Done()
	b.release.Wait()
}
