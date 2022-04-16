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

	// ???
	// TODO: Persist this field.
	// Clear this after IsReplacing is set?
	WantMoveTo *Constraint

	// Set by the balancer to indicate that this placement was created to
	// replace the placement of the same range on some other node. Should be
	// cleared once the placement becomes ready.
	IsReplacing string // NodeID

	// Guards everything.
	// TODO: What is "everything" ??
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

func (p *Placement) SetWantMoveTo(c *Constraint) error {
	p.Lock()
	defer p.Unlock()

	if p.WantMoveTo != nil {
		return fmt.Errorf("move already pending: %v", p.WantMoveTo)
	}

	p.WantMoveTo = c
	return nil
}

func (p *Placement) toState(new PlacementState) error {
	ok := false
	old := p.State

	for _, t := range PlacementStateTransitions {
		if t.from == old && t.to == new {
			ok = true
			break
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
