package ranje

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	pers Persister
	Meta Meta

	State    StateLocal
	Parents  []Ident
	Children []Ident

	// Which node currently has the range, and which it is moving to.
	// TODO: Each of these are probably only valid in some states. Doc that.
	// Invariant: These will always be on different Nodes, so callers can
	// unambiguously look up a single placement for a given (Range, Node) pair.
	CurrentPlacement *Placement
	NextPlacement    *Placement

	// Guards everything.
	// TODO: Can we get rid of this and just use the keyspace lock?
	sync.Mutex

	// Indicates that this range needs persisting before the keyspace lock is
	// released. We've made changes locally which will be lost if we crash.
	// TODO: Also store the old state, so we can roll back instead of crash?
	dirty bool
}

func (r *Range) LogString() string {
	c := ""
	if r.CurrentPlacement != nil {
		c = fmt.Sprintf(" c=(%s:%s)", r.CurrentPlacement.NodeID, r.CurrentPlacement.State)
	}

	n := ""
	if r.NextPlacement != nil {
		n = fmt.Sprintf(" n=(%s:%s)", r.NextPlacement.NodeID, r.NextPlacement.State)
	}

	return fmt.Sprintf("{%s %s%s%s}", r.Meta, r.State, c, n)
}

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.State)
}

func (r *Range) placementStateChanged(rg RangeGetter) {
	if r.CurrentPlacement != nil {
		if r.CurrentPlacement.State == SpGone {
			log.Printf("forgetting CurrentPlacement")
			r.CurrentPlacement = nil
		}
	}
	if r.NextPlacement != nil {
		if r.NextPlacement.State == SpGone {
			log.Printf("forgetting NextPlacement")
			r.NextPlacement = nil
		}
	}

	if r.CurrentPlacement == nil && r.NextPlacement == nil {
		r.toState(Pending, rg)
	}
}

// ToState change the state of the range to s or returns an error.
func (r *Range) toState(new StateLocal, rg RangeGetter) error {
	r.Lock()
	defer r.Unlock()
	old := r.State

	if old == new {
		log.Printf("R%v: %s -> %s (redundant)", r.Meta.Ident.Key, old, new)
		return nil
	}

	if new == Unknown {
		return errors.New("can't transition range into Unknown")
	}

	// Obsolete is terminal. The range should be destroyed.
	if old == Obsolete {
		return errors.New("can't transition range out of SpDropped")
	}

	ok := false

	if old == Pending && new == Placing { // 1
		ok = true
	}

	// TODO: THIS IS ONLY INITIAL PLACEMENT
	if old == Placing && new == Ready { // 2
		ok = true
	}

	// SHOULD THIS GO BACK TO PENDING?
	if old == Ready && new == Placing {
		ok = true
	}

	if old == Ready && new == Moving { // 7
		ok = true
	}

	if old == Moving && new == Ready { // 8
		ok = true
	}

	if old == Ready && new == Splitting { // 9
		ok = true
	}

	if old == Ready && new == Joining { // 10
		ok = true
	}

	if old == Ready && new == Pending { // NEEDS NUM
		ok = true
	}

	if (old == Splitting || old == Joining) && new == Obsolete {
		if !childrenReady(r, rg) {
			return fmt.Errorf("invalid state transition: %s -> %s; children not ready", old, new)
		}

		ok = true
	}

	if !ok {
		return fmt.Errorf("invalid range state transition: %s -> %s", old, new)
	}

	r.State = new
	r.dirty = true

	log.Printf("R%v: %s -> %s", r.Meta.Ident.Key, old, new)

	return nil
}

// Clear the current placement. This should be called when a range is dropped
// from a node.
func (r *Range) DropPlacement() {
	r.Lock()
	defer r.Unlock()

	if r.State != Obsolete {
		panic("can't drop current placement until range is obsolete")
	}

	if r.CurrentPlacement == nil {
		// This method should not even be called in this state!
		panic("can't drop current placement when it is nil")
	}

	if r.CurrentPlacement.State != SpDropped {
		panic("can't drop current placement until it's dropped")
	}

	r.CurrentPlacement = nil
}

// Clear the next placement. This should be called when an operation fails.
// Caller must NOT hold the range lock.
func (r *Range) ClearNextPlacement() {
	r.Lock()
	defer r.Unlock()

	if r.NextPlacement == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	r.NextPlacement = nil
}

// childrenReady returns true if all of the ranges children are ready. (Doesn't
// care if the range has no children.)
func childrenReady(r *Range, rg RangeGetter) bool {
	for _, rID := range r.Children {
		rr, err := rg.Get(rID)
		if err != nil {
			panic(err)
		}

		if rr.State != Ready {
			return false
		}
	}

	return true
}
