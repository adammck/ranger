package node

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
)

// PrepareAddShard: Create the range, but don't do anything with it yet.
func (n *Node) PrepareAddShard(rm ranje.Meta, parents []rangelet.Parent) error {
	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	_, ok := n.ranges[rm.Ident]
	if ok {
		panic("rangelet gave duplicate range!")
	}

	// TODO: Ideally we would perform most of the fetch here, and only exchange
	//       the delta (keys which have changed since then) in AddShard.

	n.ranges[rm.Ident] = &Range{
		data:     map[string][]byte{},
		fetcher:  newFetcher(rm, parents),
		writable: 0,
	}

	log.Printf("Prepared to add range: %s", rm)
	return nil
}

// AddShard:
func (n *Node) AddShard(rID ranje.Ident) error {
	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	r, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called AddShard with unknown range!")
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

// PrepareDropShard: Disable writes to the range, because we're about to move
// it and I don't have the time to implement something better today. In this
// example, keys are writable on exactly one node. (Or zero, during failures!)
func (n *Node) PrepareDropShard(rID ranje.Ident) error {
	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	r, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called PrepareDropShard with unknown range!")
	}

	// Prevent further writes to the range.
	atomic.StoreUint32(&r.writable, 0)

	log.Printf("Prepared to drop range: %s", rID)
	return nil
}

// DropShard: Discard the range.
func (n *Node) DropShard(rID ranje.Ident) error {
	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	_, ok := n.ranges[rID]
	if !ok {
		panic("rangelet called DropShard with unknown range!")
	}

	delete(n.ranges, rID)

	log.Printf("Dropped range: %s", rID)
	return nil
}
