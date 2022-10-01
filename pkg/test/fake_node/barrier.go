package fake_node

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type barrier struct {
	desc    string // just for error messages
	arrived uint32
	arrival *sync.WaitGroup
	release *sync.WaitGroup
	cb      func()
}

func NewBarrier(desc string, n int, cb func()) *barrier {
	a := &sync.WaitGroup{}
	b := &sync.WaitGroup{}
	a.Add(n)
	b.Add(n)

	return &barrier{desc, 0, a, b, cb}
}

// Wait blocks until Arrive has been called.
// TODO: Rename this to e.g. AwaitBarrier.
func (b *barrier) Wait() {
	b.arrival.Wait()
}

// Release is called in tests to unblock a state transition, which is currently
// blocked in Arrive. Before returning, as a convenience, it calls the callback
// (which is probably a WaitGroup on the rangelet changing state) so the caller
// doesn't have to wait for that itself.
// TODO: Rename this to e.g. CompleteTransition.
func (b *barrier) Release() {
	if atomic.LoadUint32(&b.arrived) == 0 {
		panic(fmt.Sprintf("Release called before Arrive for barrier: %s", b.desc))
	}

	b.release.Done()
	b.cb()
}

func (b *barrier) Arrive() {
	if atomic.LoadUint32(&b.arrived) == 1 {
		panic(fmt.Sprintf("Arrive already called for barrier: %s", b.desc))
	}

	atomic.StoreUint32(&b.arrived, 1)
	b.arrival.Done()
	b.release.Wait()
}
