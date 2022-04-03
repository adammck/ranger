package ranje

import (
	"fmt"
	"log"
	"sync"
)

// Placement represents a pair of range+node.
type Placement struct {
	rang   *Range // owned by Keyspace.
	NodeID string

	// Controller-side State machine.
	// Never modify this field directly! It's only public for deserialization
	// from the store. Modify it via ToState.
	State PlacementState

	// Guards everything.
	// TODO: Change into an RWLock, check callers.
	// TODO: Should this also lock the range and node? I think no?
	sync.Mutex
}

func NewPlacement(r *Range, nodeID string) *Placement {
	return &Placement{
		rang:   r,
		NodeID: nodeID,
		State:  PsPending,
	}
}

func (p *Placement) Range() *Range {
	return p.rang
}

func (p *Placement) LogString() string {
	return fmt.Sprintf("{%s %s:%s}", p.rang.Meta.String(), p.NodeID, p.State)
}

func (p *Placement) toState(new PlacementState) error {
	ok := false
	old := p.State

	if old == PsPending {
		if new == PsLoading {
			ok = true
		}
	} else if old == PsLoading {
		if new == PsReady {
			ok = true
		}
	}

	if !ok {
		return fmt.Errorf("invalid placement state transition: %s -> %s", old.String(), new.String())
	}

	p.State = new
	p.rang.dirty = true

	log.Printf("R%d P %s -> %s", p.rang.Meta.Ident, old, new)

	return nil
}
