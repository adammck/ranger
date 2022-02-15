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

	state    StateLocal
	parents  []*Range
	children []*Range

	// Which node currently has the range, and which it is moving to.
	// TODO: Each of these are probably only valid in some states. Doc that.
	// TODO: Placement fucks with these directly. Don't do that.
	curr *DurablePlacement
	next *DurablePlacement

	// The number of times this range has failed to be placed since it was last
	// Ready. Incremented by State.
	placeErrorCount int

	// Guards everything.
	sync.Mutex
}

func (r *Range) Child(index int) (*Range, error) {
	// TODO: Locking!

	if index < 0 {
		return nil, errors.New("negative index")
	}
	if index > len(r.children)-1 {
		return nil, errors.New("invalid index")
	}

	return r.children[index], nil
}

func (r *Range) AssertState(s StateLocal) {
	if r.state != s {
		panic(fmt.Sprintf("range failed state assertion %s != %s %s", r.state.String(), s.String(), r))
	}
}

func (r *Range) SameMeta(id Ident, start, end []byte) bool {
	// TODO: This method is batshit
	return r.Meta.Ident.Key == id.Key && r.Meta.Start == Key(start) && r.Meta.End == Key(end)
}

func (r *Range) LogString() string {
	c := "nil"
	if r.curr != nil {
		c = fmt.Sprintf("(%s:%s)", r.curr.NodeID(), r.curr.State())
	}

	n := "nil"
	if r.next != nil {
		n = fmt.Sprintf("(%s:%s)", r.next.NodeID(), r.next.State())
	}

	return fmt.Sprintf("{%s:%s c=%s n=%s}", r.Meta, r.state, c, n)
}

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.state)
}

func (r *Range) Placement() *DurablePlacement {
	return r.curr
}

func (r *Range) NextPlacement() *DurablePlacement {
	return r.next
}

// TODO: Use Placement instead of this!
func (r *Range) MoveSrc() *DurablePlacement {

	// TODO: Only really need RLock here.
	r.Lock()
	defer r.Unlock()

	if r.curr == nil {
		return nil
	}

	// TODO: Is this necessary?
	if r.curr.state != SpReady {
		panic(fmt.Sprintf("movesrc called on range %s, where placement is not ready", r.String()))
	}

	return r.curr
}

// MustState attempts to change the state of the range to s, and panics if the
// transition is invalid. Callers should only ever attempt valid state changes
// anyway, but...
func (r *Range) MustState(s StateLocal) {
	err := r.ToState(s)
	if err != nil {
		panic(fmt.Sprintf("MustState: %s", err.Error()))
	}
}

func (r *Range) State() StateLocal {
	return r.state
}

// ToState change the state of the range to s or returns an error.
func (r *Range) ToState(new StateLocal) error {
	r.Lock()
	defer r.Unlock()
	old := r.state

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

	// This transition should only happen via CheckState.
	if (old == Splitting || old == Joining) && new == Obsolete {
		if !r.childrenReady() {
			return fmt.Errorf("invalid state transition: %s -> %s; children not ready", old, new)
		}

		ok = true
	}

	if !ok {
		return fmt.Errorf("invalid range state transition: %s -> %s", old, new)
	}

	// Update durable storage first
	err := r.pers.PutState(r, new)
	if err != nil {
		r.state = old
		return fmt.Errorf("while persisting range state: %s", err)
	}

	r.state = new

	// Notify parent(s) of state change, so they can change their own state in
	// response.
	// TODO: Does this need to be transactional with the above state change? This won't be called again during rehydration.
	for _, parent := range r.parents {
		err := parent.ChildStateChanged()
		if err != nil {
			r.state = old
			return err
		}
	}

	log.Printf("R%v: %s -> %s", r.Meta.Ident.Key, old, new)

	return nil
}

func (r *Range) InitPersist() error {

	// Calling this method any time other than immediately after instantiating
	// is a bug. Every other transition should happen via ToState.
	if r.state != Pending {
		panic("InitPersist called on non-pending range")
	}

	return r.pers.Create(r)
}

func (r *Range) ParentIdents() []Ident {
	ids := make([]Ident, len(r.parents))
	for i, r := range r.parents {
		ids[i] = r.Meta.Ident
	}
	return ids
}

func (r *Range) Parents() []*Range {
	return r.parents
}

func (r *Range) ChildStateChanged() error {

	// Splitting and joining ranges become obsolete once their children become ready.
	if r.state == Splitting || r.state == Joining {
		if r.childrenReady() {
			return r.ToState(Obsolete)
		}
	}

	return nil
}

// Caller must NOT hold the range lock.
func (r *Range) ClearNextPlacement() {
	r.Lock()
	defer r.Unlock()

	if r.next == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	r.next = nil
}

// Caller must NOT hold the range lock.
func (r *Range) CompleteNextPlacement() error {
	r.Lock()
	defer r.Unlock()

	if r.next == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	// During inital placement, it's okay for there to be no current placement.
	// Otherwise notify the placement that we're about to destroy it. (This is
	// a gross hack, because Node also has a pointer to the placement for now.)
	// TODO: Should this be a totally separate path?
	if r.curr != nil {
		err := r.curr.Forget()
		if err != nil {
			return err
		}
	}

	r.curr = r.next
	r.next = nil

	return nil
}

// childrenReady returns true if all of the ranges children are ready. (Doesn't
// care if the range has no children.)
func (r *Range) childrenReady() bool {
	for _, rr := range r.children {
		if rr.state != Ready {
			return false
		}
	}

	return true
}

func (r *Range) NeedsQuarantine() bool {
	return r.placeErrorCount >= 3
}
