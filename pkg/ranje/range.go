package ranje

import (
	"fmt"
	"log"
	"sync"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	pers Persister
	Meta Meta

	State    RangeState
	Parents  []Ident
	Children []Ident

	// TODO: Docs
	Placements []*Placement

	// Guards everything.
	// TODO: Can we get rid of this and just use the keyspace lock?
	sync.Mutex

	// Indicates that this range needs persisting before the keyspace lock is
	// released. We've made changes locally which will be lost if we crash.
	// TODO: Also store the old state, so we can roll back instead of crash?
	dirty bool
}

func (r *Range) LogString() string {
	ps := ""

	// TODO: Use Placement.LogString
	for i, p := range r.Placements {
		ps = fmt.Sprintf("%s p%d=%s:%s", ps, i, p.NodeID, p.State)

		if p.WantMove {
			ps = fmt.Sprintf("%s:want-move", ps)
		}
	}

	return fmt.Sprintf("{%s %s%s}", r.Meta, r.State, ps)
}

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.State)
}

// func (r *Range) placementStateChanged(rg RangeGetter) {
// 	panic("not implemented; see 839595a")
// }

func (r *Range) toState(new RangeState, rg RangeGetter) error {
	panic("not implemented; see 839595a")
}

// MayBecomeReady returns whether the given placement is permitted to advance
// from PsPrepared to PsReady.
func (r *Range) MayBecomeReady(p *Placement) bool {

	// Sanity check.
	if p.State != PsPrepared {
		log.Printf("called MayBecomeReady in weird state: %s", p.State)
		return false
	}

	// TODO: Should read the number of replicas from the config, not hard-code
	//       it to one! Probably needs to move to the keyspace.

	for _, p2 := range r.Placements {
		if p2.State == PsReady {
			return false
		}
	}

	return true
}

// MayBeTaken returns whether the given placement, which is assumed to be in
// state PsReady, may advance to PsTaken. The only circumstance where this is
// true is when the placement is being replaced by another replica (i.e. a move)
// or when the entire range has been subsumed.
//
// TODO: Implement the latter, when (re-)implementing splits and joins. This
//       probably needs to move to the keyspace, for that.
//
func (r *Range) MayBeTaken(p *Placement) bool {

	// Sanity check.
	if p.State != PsReady {
		log.Printf("called MayBeTaken in weird state: %s", p.State)
		return false
	}

	if !p.WantMove {
		return false
	}

	var replacement *Placement
	for _, p2 := range r.Placements {
		if p2.IsReplacing == p.NodeID {
			replacement = p2
			break
		}
	}

	// p wants to be moved, but no replacement placement has been created yet.
	// Not sure how we ended up here, but it's valid.
	if replacement == nil {
		return false
	}

	if replacement.State == PsPrepared {
		return true
	}

	return false
}
