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

// Release is called in tests to unblock a state transition, which is currently
// blocked in Arrive. Before returning, as a convenience, it calls the callback
// (which is probably a WaitGroup on the rangelet changing state) so the caller
// doesn't have to wait for that itself.
func (b *barrier) Release() {
	b.release.Done()
	b.cb()
}

func (b *barrier) Arrive() {
	b.arrival.Done()
	b.release.Wait()
}
