package fake_node

import "sync"

type barrier struct {
	arrival *sync.WaitGroup
	release *sync.WaitGroup
}

func NewBarrier(n int) *barrier {
	a := &sync.WaitGroup{}
	b := &sync.WaitGroup{}
	a.Add(n)
	b.Add(n)

	return &barrier{a, b}
}

func (b *barrier) Wait() {
	b.arrival.Wait()
}

func (b *barrier) Release() {
	b.release.Done()
}

func (b *barrier) Arrive() {
	b.arrival.Done()
	b.release.Wait()
}
