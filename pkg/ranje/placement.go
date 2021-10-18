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
	//remoteState StateRemote

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

// Forget removes this placement from the associated node and range.
func (p *Placement) Forget() {
	p.rang.Lock()
	defer p.rang.Unlock()

	p.node.muRanges.Lock()
	defer p.node.muRanges.Unlock()

	// Attempt both whether or not the first one fails.
	// Either one failing indicates a bug.
	err1 := p.rang.UnsafeForgetPlacement(p)
	err2 := p.node.UnsafeForgetPlacement(p)

	// TODO: This is probably a bit excessive.
	if err1 != nil {
		panic(fmt.Sprintf("UnsafeForgetPlacement: %s", err1.Error()))
	}
	if err2 != nil {
		panic(fmt.Sprintf("UnsafeForgetPlacement: %s", err2.Error()))
	}
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
		if new == SpFetching { // 1
			ok = true

		} else if new == SpReady { // 2
			ok = true
		}

	} else if old == SpFetching {
		if new == SpFetched { // 3
			ok = true

		} else if new == SpFetchFailed {
			ok = true
			panic("placement state transition not implemented: fetching -> fetch_failed") // 4
		}

	} else if old == SpFetched {
		if new == SpReady { // 5
			ok = true
		}

	} else if old == SpFetchFailed {
		if new == SpPending {
			ok = true
			panic("placement state transition not implemented: fetch_failed -> pending") // 6
		}

	} else if old == SpReady {
		if new == SpTaken { // 7
			ok = true
		}

	} else if old == SpTaken {
		if new == SpDropped { // 8
			ok = true
		}
	}

	if !ok {
		return fmt.Errorf("invalid placement state transition: %s -> %s", old.String(), new.String())
	}

	p.state = new

	fmt.Printf("P %s -> %s\n", old, new)
	return nil
}

func (p *Placement) Take() error {
	return p.node.take(p)
}

func (p *Placement) Drop() error {
	return p.node.drop(p)
}

func (p *Placement) Serve() error {
	return p.node.serve(p)
}
