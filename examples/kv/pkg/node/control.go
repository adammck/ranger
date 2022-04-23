package node

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
)

// PrepareAddShard: Create the range, but don't do anything with it yet.
func (n *Node) PrepareAddShard(rm ranje.Meta, parents []rangelet.Parent) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.data[rm.Ident]
	if ok {
		panic("rangelet gave duplicate range!")
	}

	// TODO: Ideally we would perform most of the fetch here, and only exchange
	//       the delta (keys which have changed since then) in AddShard.

	n.data[rm.Ident] = &KeysVals{
		data:    map[string][]byte{},
		fetcher: NewFetcher(rm, parents),
		writes:  false,
	}

	log.Printf("Prepared to add range: %s", rm)
	return nil
}

// AddShard:
func (n *Node) AddShard(rID ranje.Ident) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to serve unknown range!")
	}

	err := kv.fetcher.Fetch(kv)
	if err != nil {
		return fmt.Errorf("error fetching range: %s", err)
	}

	kv.fetcher = nil
	kv.writes = true

	log.Printf("Added range: %s", rID)
	return nil
}

// PrepareDropShard: Disable writes to the range, because we're about to move
// it and I don't have the time to implement something better today. In this
// example, keys are writable on exactly one node. (Or zero, during failures!)
func (n *Node) PrepareDropShard(rID ranje.Ident) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to drop unknown range!")
	}

	kv.writes = false

	log.Printf("Prepared to drop range: %s", rID)
	return nil
}

func (n *Node) DropShard(rID ranje.Ident) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// No big deal if the range doesn't exist.
	delete(n.data, rID)

	log.Printf("Dropped range: %s", rID)
	return nil
}
