package node

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
)

// PrepareAddRange: Create the range, but don't do anything with it yet.
func (n *Node) PrepareAddRange(rm ranje.Meta, parents []rangelet.Parent) error {
	if err := n.performChaos(); err != nil {
		return err
	}

	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	_, ok := n.ranges[rm.Ident]
	if ok {
		panic("rangelet gave duplicate range!")
	}

	// TODO: Ideally we would perform most of the fetch here, and only exchange
	//       the delta (keys which have changed since then) in AddRange.

	n.ranges[rm.Ident] = &Range{
		data:     map[string][]byte{},
		fetcher:  newFetcher(rm, parents),
		writable: 0,
	}

	log.Printf("Prepared to add range: %s", rm)
	return nil
}

// AddRange:
func (n *Node) AddRange(rID ranje.Ident) error {
	if err := n.performChaos(); err != nil {
		return err
	}

	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	r, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called AddRange with unknown range!")
	}

	err := r.fetcher.Fetch(r)
	if err != nil {
		return fmt.Errorf("error fetching range: %s", err)
	}

	r.fetcher = nil
	atomic.StoreUint32(&r.writable, 1)

	log.Printf("Added range: %s", rID)
	return nil
}

// PrepareDropRange: Disable writes to the range, because we're about to move
// it and I don't have the time to implement something better today. In this
// example, keys are writable on exactly one node. (Or zero, during failures!)
func (n *Node) PrepareDropRange(rID ranje.Ident) error {
	if err := n.performChaos(); err != nil {
		return err
	}

	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	r, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called PrepareDropRange with unknown range!")
	}

	// Prevent further writes to the range.
	atomic.StoreUint32(&r.writable, 0)

	log.Printf("Prepared to drop range: %s", rID)
	return nil
}

// DropRange: Discard the range.
func (n *Node) DropRange(rID ranje.Ident) error {
	if err := n.performChaos(); err != nil {
		return err
	}

	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	_, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called DropRange with unknown range!")
	}

	delete(n.ranges, rID)

	log.Printf("Dropped range: %s", rID)
	return nil
}

// performChaos sleeps for a random amount of time between zero and 5000ms,
// biased towards zero. Then returns an error 5% of the time. This is of course
// intended to make our testing a little more chaotic.
func (n *Node) performChaos() error {
	ms := int(3000 * math.Pow(rand.Float64(), 2))
	d := time.Duration(ms) * time.Millisecond
	log.Printf("Sleeping %v", d)
	time.Sleep(d)

	// TODO: This causes actual problems really fast if raised significantly.
	//       Looks like an orchestrator bug. Look into it.
	if rand.Float32() < 0.05 {
		return fmt.Errorf("it's your unlucky day")
	}

	return nil
}
