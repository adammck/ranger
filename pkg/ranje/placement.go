package ranje

import (
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

func NewPlacement(r *Range, nodeID string) (*Placement, error) {
	return &Placement{
		rang:   r,
		NodeID: nodeID,
	}, nil
}

func (p *Placement) toState(new PlacementState) error {
	panic("not implemented; see 839595a")
}
