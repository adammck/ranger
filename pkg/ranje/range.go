package ranje

import (
	"fmt"
)

// type Meta struct {
// 	ident ident.Ident
// 	start Key // inclusive
// 	end   Key // exclusive
// }

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	// TODO: Replace these with a Meta
	Ident int
	start Key // inclusive
	end   Key // exclusive
	// END TODO

	state    StateLocal
	parents  []*Range
	children []*Range

	// Which nodes currently have this range, and what state are they in? May be
	// empty or have n entries, depending on the local state of the range.
	placements []*Placement

	// Hints
	// Public so the balancer can mess with them.
	// TODO: Should that happen via accessors instead?
	ForceNodeIdent string
}

// Contains returns true if the given key is within the range.
// TODO: Test this.
func (r *Range) Contains(k Key) bool {
	if r.start != ZeroKey {
		if k < r.start {
			return false
		}
	}

	if r.end != ZeroKey {
		// Note that the range end is exclusive!
		if k >= r.end {
			return false
		}
	}

	return true
}

func (r *Range) SameMeta(id Ident, start, end []byte) bool {
	// TODO: This method is batshit
	return uint64(r.Ident) == id.Key && r.start == Key(start) && r.end == Key(end)
}

func (r *Range) String() string {
	var s, e string

	if r.start == ZeroKey {
		s = "[-inf"
	} else {
		s = fmt.Sprintf("(%s", r.start)
	}

	if r.end == ZeroKey {
		e = "+inf]"
	} else {
		e = fmt.Sprintf("%s]", r.end)
	}

	return fmt.Sprintf("{%d %s %s, %s}", r.Ident, r.state, s, e)
}

func (r *Range) State(s StateLocal) error {
	old := r.state
	new := s
	ok := false

	if old == Pending && new == Ready {
		ok = true
	}

	if old == Ready && new == Splitting {
		ok = true
	}

	if old == Ready && new == Joining {
		ok = true
	}

	// This transition should only happen via CheckState.
	if (old == Splitting || old == Joining) && new == Obsolete {
		if !r.childrenReady() {
			return fmt.Errorf("invalid state transition: %s -> %s; children not ready", old, new)
		}

		ok = true
	}

	// TODO Assigned -> Splitting
	// TODO Assigned -> Merging
	// TODO Splitting -> Discarding
	// TODO Merging -> Discarding

	if !ok {
		return fmt.Errorf("invalid state transition: %s -> %s", old, new)
	}

	r.state = s

	// Notify parent(s) of state change, so they can change their own state in
	// response.
	for _, parent := range r.parents {
		err := parent.CheckState()
		if err != nil {
			r.state = old
			return err
		}
	}

	//fmt.Printf("%s -> %s\n", old, new)
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
