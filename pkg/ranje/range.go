package ranje

import (
	"errors"
	"fmt"
	"sync"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	Meta Meta

	state    StateLocal
	parents  []*Range
	children []*Range

	// Which nodes currently have this range, and what state are they in? May be
	// empty or have n entries, depending on the local state of the range.
	placements []*Placement

	// The number of times this range has failed to be placed since it was last
	// Ready. Incremented by State.
	placeErrorCount int

	// Hints
	// Public so the balancer can mess with them.
	// TODO: Should that happen via accessors instead?
	ForceNodeIdent string

	// Guards everything.
	sync.Mutex
}

// Contains returns true if the given key is within the range.
// TODO: Test this.
func (r *Range) Contains(k Key) bool {
	if r.Meta.Start != ZeroKey {
		if k < r.Meta.Start {
			return false
		}
	}

	if r.Meta.End != ZeroKey {
		// Note that the range end is exclusive!
		if k >= r.Meta.End {
			return false
		}
	}

	return true
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

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.state)
}

// TODO: Replace this with a statusz-type page
func (r *Range) DumpForDebug() {
	f := ""
	if r.ForceNodeIdent != "" {
		f = fmt.Sprintf(" (forcing to: %s)", r.ForceNodeIdent)
	}
	fmt.Printf(" - %s%s\n", r.String(), f)
}

// MustState attempts to change the state of the range to s, and panics if the
// transition is invalid. Callers should only ever attempt valid state changes
// anyway, but...
func (r *Range) MustState(s StateLocal) {
	err := r.State(s)
	if err != nil {
		panic(fmt.Sprintf("MustState: %s", err.Error()))
	}
}

// State change the state of the range to s or returns an error.
func (r *Range) State(new StateLocal) error {
	r.Lock()
	defer r.Unlock()
	old := r.state
	ok := false

	if new == Unknown {
		return errors.New("can't transition range into Unknown")
	}

	// Obsolete is terminal. The range should be destroyed.
	if old == Obsolete {
		return errors.New("can't transition range out of SpDropped")
	}

	if old == Pending && new == Placing { // 1
		ok = true
	}

	if old == Placing && new == Ready { // 2
		r.placeErrorCount = 0
		ok = true
	}

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

	r.state = new

	// Notify parent(s) of state change, so they can change their own state in
	// response.
	for _, parent := range r.parents {
		err := parent.CheckState()
		if err != nil {
			r.state = old
			return err
		}
	}

	fmt.Printf("%s %s -> %s\n", r.String(), old, new)
	return nil
}

func (r *Range) CheckState() error {

	// Splitting and joining ranges become obsolete once their children become ready.
	if r.state == Splitting || r.state == Joining {
		if r.childrenReady() {
			return r.State(Obsolete)
		}
	}

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

// func (r *Range) DoPlacement(n *Node) error { // 1
// 	r.AssertState(Placing)

// 	p := NewPlacement(r, n)

// 	return nil
// }
