package ranje

import (
	"errors"
	"fmt"
	"sync"
	"time"
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
	// TODO: Change into an RWLock, check callers.
	// TODO: Should this also lock the range and node? I think no?
	sync.Mutex
}

func (p *Placement) Addr() string {

	// This should definitely not ever happen
	if p.node == nil {
		panic("nil node for placement")
	}

	return p.node.addr()
}

// TODO: Replace this with a statusz-type page
func (p *Placement) DumpForDebug() string {
	return fmt.Sprintf("P{%s %s}", p.node.addr(), p.state)
}

func NewPlacement(r *Range, n *Node) (*Placement, error) {
	p := &Placement{
		rang:  r,
		node:  n,
		state: SpPending,
	}

	r.Lock()
	defer r.Unlock()

	// TODO: The placement should not care about this! Call this thing via
	// ....  range.NewPlacement to check this.
	if r.next != nil {
		return nil, fmt.Errorf("range %s already has a next placement: %s", r.String(), r.next.Addr())
	}

	n.muRanges.Lock()
	defer n.muRanges.Unlock()

	id := r.Meta.Ident
	_, ok := n.ranges[id]
	if ok {
		return nil, fmt.Errorf("node %s already has range %s", n.String(), id.String())
	}

	r.next = p
	n.ranges[id] = p

	//r.PlacementStateChanged(p)

	return p, nil
}

// Forget removes this placement from the associated node.
// TODO: Do nodes even need a pointer back to the actual placement? Maybe can just cache what they hear via gRPC?
func (p *Placement) Forget() error {
	return p.node.ForgetPlacement(p)
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

	// Notify range of state change, so it can change its own state.
	//p.rang.PlacementStateChanged(p)

	// TODO: Should we notify the node, too?

	return nil
}

func (p *Placement) Give() (StatePlacement, error) {
	// Build the request here to avoid Node having to reach back through us.
	// TODO: Not sure if this actually makes sense.
	req, err := p.rang.GiveRequest(p)
	if err != nil {
		return SpUnknown, fmt.Errorf("error building GiveRequest: %s", err)
	}

	return p.state, p.node.give(p, req)
}

// FetchWait blocks until the placement becomes SpFetched, which hopefully happens
// in some other goroutine.
// TODO: Add a timeout
func (p *Placement) FetchWait() error {
	for {
		p.Lock()
		s := p.state
		p.Unlock()

		if s == SpFetched {
			break

		} else if s == SpFetchFailed {
			// TODO: Can the client provide any info about why this failed?
			return fmt.Errorf("placement failed")

		} else if s != SpFetching {
			return fmt.Errorf("placement became %s, expectd SpFetched", s.String())
		}

		// s == SpFetching, so keep waiting
		time.Sleep(100 * time.Millisecond)
	}

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
