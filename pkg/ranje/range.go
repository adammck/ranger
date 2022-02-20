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
	// TODO: Placement fucks with these directly. Don't do that.
	CurrentPlacement *Placement
	NextPlacement    *Placement

	// The number of times this range has failed to be placed since it was last
	// Ready. Incremented by State.
	placeErrorCount int

	// Guards everything.
	sync.Mutex
}

func (r *Range) Put() error {
	err := r.pers.PutRange(r)
	if err != nil {
		panic(fmt.Sprintf("failed to put range %d: %s", r.Meta.Ident.Key, err))
		//return err
	}

	return nil
}

func (r *Range) AssertState(s StateLocal) {
	if r.State != s {
		panic(fmt.Sprintf("range failed state assertion %s != %s %s", r.State.String(), s.String(), r))
	}
}

func (r *Range) SameMeta(id Ident, start, end []byte) bool {
	// TODO: This method is batshit
	return r.Meta.Ident.Key == id.Key && r.Meta.Start == Key(start) && r.Meta.End == Key(end)
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

// TODO: Use Placement instead of this!
func (r *Range) MoveSrc() *Placement {

	// TODO: Only really need RLock here.
	r.Lock()
	defer r.Unlock()

	if r.CurrentPlacement == nil {
		return nil
	}

	// TODO: Is this necessary?
	if r.CurrentPlacement.State != SpReady {
		panic(fmt.Sprintf("movesrc called on range %s, where placement is not ready", r.String()))
	}

	return r.CurrentPlacement
}

// MustState attempts to change the state of the range to s, and panics if the
// transition is invalid. Callers should only ever attempt valid state changes
// anyway, but...
func (r *Range) MustState(s StateLocal, rg RangeGetter) {
	err := r.toState(s, rg)
	if err != nil {
		panic(fmt.Sprintf("MustState: %s", err.Error()))
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

	// Straight from Pending to PlaceError means that we can't even attempt a
	// placement. Probably no candidate nodes available. Currently increments
	// error count anyway, which might result in range quarantine!
	if old == Pending && new == PlaceError { // NEEDS NUM
		r.placeErrorCount += 1
		ok = true
	}

	// TODO: THIS IS ONLY INITIAL PLACEMENT
	if old == Placing && new == Ready { // 2
		r.placeErrorCount = 0
		ok = true
	}

	// Started placing the range, but it failed. Maybe that's because the range
	// is toxic. Or maybe just unlucky timing and the destination ndoe died.
	if old == Placing && new == PlaceError { // 3
		r.placeErrorCount += 1
		ok = true
	}

	// WRONG
	// NOT (4)
	// THIS SHOULD GO BACK TO PLACING
	if old == PlaceError && new == Pending {
		ok = true
	}

	if old == PlaceError && new == Quarantined { // 5
		ok = true
	}

	// Doesn't happen automatically. Only when forced by an operator.
	if old == Quarantined && new == Placing { // 6
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

	// Try to persist the new state, and rewind+abort if it fails.
	err := r.pers.PutRange(r)
	if err != nil {
		r.State = old
		return fmt.Errorf("while persisting range: %s", err)
	}

	log.Printf("R%v: %s -> %s", r.Meta.Ident.Key, old, new)

	return nil
}

// TODO: Remove this. Callers can just call Put.
func (r *Range) InitPersist() error {

	// Calling this method any time other than immediately after instantiating
	// is a bug. Every other transition should happen via ToState.
	if r.State != Pending {
		panic("InitPersist called on non-pending range")
	}

	return r.pers.PutRange(r)
}

// TODO: Remove this. It's pointless now.
func (r *Range) ParentIdents() []Ident {
	return r.Parents
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

// CompleteNextPlacement moves the next placement to current. This should be
// called when an operation succeeds.
// Caller must NOT hold the range lock.
func (r *Range) CompleteNextPlacement() error {
	r.Lock()
	defer r.Unlock()

	if r.NextPlacement == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	r.CurrentPlacement = r.NextPlacement
	r.NextPlacement = nil

	return nil
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

func (r *Range) NeedsQuarantine() bool {
	return r.placeErrorCount >= 3
}
