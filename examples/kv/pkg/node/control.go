package node

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
)

func (n *Node) GetLoadInfo(rID ranje.Ident) (api.LoadInfo, error) {
	n.rangesMu.RLock()
	defer n.rangesMu.RUnlock()

	r, ok := n.ranges[rID]
	if !ok {
		return api.LoadInfo{}, api.NotFound
	}

	keys := []string{}

	// Find mid-point in an extremely inefficient manner.
	// While holding the lock, no less.
	func() {
		r.dataMu.RLock()
		defer r.dataMu.RUnlock()
		for k := range r.data {
			keys = append(keys, k)
		}
	}()

	var split ranje.Key
	if len(keys) > 2 {
		sort.Strings(keys)
		split = ranje.Key(keys[len(keys)/2])
	}

	return api.LoadInfo{
		Keys:   len(keys),
		Splits: []ranje.Key{split},
	}, nil
}

// PrepareAddRange: Create the range, but don't do anything with it yet.
func (n *Node) PrepareAddRange(rm ranje.Meta, parents []api.Parent) error {
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

	n.rangesMu.RLock()
	r, ok := n.ranges[rID]
	n.rangesMu.RUnlock()
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

// performChaos optionally (if chaos is enabled, via the -chaos flag) sleeps for
// a random amount of time between zero and 5000ms, biased towards zero. Then
// returns an error 5% of the time. This is of course intended to make our
// manual testing a little more chaotic.
func (n *Node) performChaos() error {
	if !n.chaos {
		return nil
	}

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
