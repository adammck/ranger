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

// TODO: This is only used by Keyspace.LogString, which is only used by tests!
//       So move it to the tests or use it elsewhere.
func (r *Range) LogString() string {
	ps := ""

	// TODO: Use Placement.LogString
	for i, p := range r.Placements {
		ps = fmt.Sprintf("%s p%d=%s:%s", ps, i, p.NodeID, p.State)

		if p.IsReplacing != "" {
			ps = fmt.Sprintf("%s:replacing(%v)", ps, p.IsReplacing)
		}
	}

	return fmt.Sprintf("{%s %s%s}", r.Meta, r.State, ps)
}

// TODO: Make this less weird.
func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta, r.State)
}

// ???
func (r *Range) MinReady() int {
	return 1
}

func (r *Range) toState(new RangeState, rg RangeGetter) error {
	r.Lock()
	defer r.Unlock()

	ok := false
	old := r.State

	for _, t := range RangeStateTransitions {
		if t.from == old && t.to == new {
			ok = true
			break
		}
	}

	if !ok {
		return fmt.Errorf("invalid range state transition: %s -> %s", old.String(), new.String())
	}

	r.State = new
	r.dirty = true

	// TODO: What is this?
	log.Printf("%s %s -> %s", r, old, new)

	return nil
}
