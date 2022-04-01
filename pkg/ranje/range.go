package ranje

import (
	"fmt"
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

	for i, p := range r.Placements {
		ps = fmt.Sprintf(" p%d=%s:%s", i, p.NodeID, p.State)
	}

	return fmt.Sprintf("{%s %s%s}", r.Meta, r.State, ps)
}

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.State)
}

func (r *Range) placementStateChanged(rg RangeGetter) {
	panic("not implemented; see 839595a")
}

func (r *Range) toState(new RangeState, rg RangeGetter) error {
	panic("not implemented; see 839595a")
}
