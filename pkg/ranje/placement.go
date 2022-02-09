package ranje

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// DurablePlacement represents a pair of range+node.
// TODO: Rename this back to Placement once VolatilePlacement stuff has been extracted.
type DurablePlacement struct {
	rang   *Range // owned by Keyspace.
	nodeID string

	// Controller-side state machine.
	state StatePlacement

	// Guards everything.
	// TODO: Change into an RWLock, check callers.
	// TODO: Should this also lock the range and node? I think no?
	sync.Mutex
}

// TODO: Rename this to NodeID; that's what it is now.
// TODO: Remove this; placements should not be connected to their node, only by ident.
func (p *DurablePlacement) NodeID() string {

	// This should definitely not ever happen
	if p.nodeID == "" {
		panic("nil node for placement")
	}

	return p.nodeID
}

// TODO: Replace this with a statusz-type page
func (p *DurablePlacement) DumpForDebug() string {
	return fmt.Sprintf("P{%s %s}", p.nodeID, p.state)
}

func NewPlacement(r *Range, nodeID string) (*DurablePlacement, error) {
	p := &DurablePlacement{
		rang:   r,
		nodeID: nodeID,
		state:  SpPending,
	}

	r.Lock()
	defer r.Unlock()

	// TODO: The placement should not care about this! Call this thing via
	// ....  range.NewPlacement to check this.
	if r.next != nil {
		return nil, fmt.Errorf("range %s already has a next placement: %s", r.String(), r.next.NodeID())
	}

	r.next = p
	//r.PlacementStateChanged(p)

	return p, nil
}

// TODO: Remove this
func (p *DurablePlacement) Forget() error {
	return nil
}

func (p *DurablePlacement) State() StatePlacement {
	return p.state
}

func (p *DurablePlacement) ToState(new StatePlacement) error {
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

		if new == SpReady {
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

// FetchWait blocks until the placement becomes SpFetched, which hopefully happens
// in some other goroutine.
// TODO: Add a timeout
func (p *DurablePlacement) FetchWait() error {
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
