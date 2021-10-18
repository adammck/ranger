package ranje

import (
	"errors"
	"fmt"
	"sync"
)

// Placement represents a pair of range+node.
type Placement struct {
	rang *Range // owned by Keyspace.
	node *Node  // owned by Roster.

	// Controller-side state machine.
	state StatePlacement

	// Warning! This may not be accurate! The range may have changed state on
	// the remote node since the last successful probe, or the node may have
	// gone away. This is what we *think* the state is.
	remoteState StateRemote

	// loadinfo?
	// The number of keys that the range has.
	K uint64

	// Guards everything.
	sync.Mutex
}

func (p *Placement) Addr() string {

	// This should definitely not ever happen
	if p.node == nil {
		panic("nil node for placement")
		//return ""
	}

	return p.node.addr()
}

func NewPlacement(r *Range, n *Node) (*Placement, error) {
	p := &Placement{
		rang:  r,
		node:  n,
		state: SpPending,
	}

	n.muRanges.Lock()
	defer n.muRanges.Unlock()

	id := r.Meta.Ident
	_, ok := n.ranges[id]
	if ok {
		return nil, fmt.Errorf("node %s already has range %s", n.String(), id.String())
	}

	r.Lock()
	defer r.Unlock()

	r.placements = append(r.placements, p)
	n.ranges[id] = p

	return p, nil
}

func (p *Placement) ToState(new StatePlacement) error {
	p.Lock()
	defer p.Unlock()
	old := p.state
	ok := false

	if new == SpUnknown {
		return errors.New("can't transition placement into SpUnknown")
	}

	// Dropped is terminal. The placement should be destroyed.
	if old == SpDropped {
		return errors.New("can't transition placement out of SpDropped")
	}

	if old == SpPending {
		if new == SpFetching {
			ok = true
			panic("not implemented: pending -> fetching") // 1

		} else if new == SpReady {
			ok = true
			panic("not implemented: pending -> ready") // 2
		}

	} else if old == SpFetching {
		if new == SpFetched {
			ok = true
			panic("not implemented: fetching -> fetched") // 3

		} else if new == SpFetchFailed {
			ok = true
			panic("not implemented: fetching -> fetch_failed") // 4
		}

	} else if old == SpFetched {
		if new == SpReady {
			ok = true
			panic("not implemented: fetched -> ready") // 5
		}

	} else if old == SpFetchFailed {
		if new == SpPending {
			ok = true
			panic("not implemented: fetch_failed -> pending") // 6
		}

	} else if old == SpReady {
		if new == SpTaken {
			ok = true
			panic("not implemented: ready -> taken") // 7
		}

	} else if old == SpTaken {
		if new == SpDropped {
			ok = true
			panic("not implemented: taken -> dropped") // 8
		}
	}

	if !ok {
		return fmt.Errorf("invalid placement state transition: %s -> %s", old.String(), new.String())
	}

	p.state = new

	return nil
}
