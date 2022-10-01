package ranje

import (
	"fmt"
	"log"
	"sync"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	Meta Meta

	State    RangeState
	Parents  []Ident
	Children []Ident

	// TODO: Docs
	Placements []*Placement

	// Guards everything.
	// TODO: Can we get rid of this and just use the keyspace lock?
	sync.Mutex

	// Not persisted.
	onObsolete func()

	// Indicates that this range needs persisting before the keyspace lock is
	// released. We've made changes locally which will be lost if we crash.
	// TODO: Also store the old state, so we can roll back instead of crash?
	// TODO: Invert so that zero value is the default: needing pesisting.
	dirty bool
}

func NewRange(rID Ident) *Range {
	return &Range{
		Meta: Meta{
			Ident: rID,
		},

		// Born active, to be placed right away.
		// TODO: Maybe we need a pending state first? Do the parent ranges need
		//       to do anything else after this range is created but before it's
		//       placed?
		State: RsActive,

		// Starts dirty, because it hasn't been persisted yet.
		dirty: true,
	}
}

// TODO: This is only used by Keyspace.LogString, which is only used by tests!
//       So move it to the tests or use it elsewhere.
func (r *Range) LogString() string {
	ps := ""

	// TODO: Use Placement.LogString
	for i, p := range r.Placements {
		ps = fmt.Sprintf("%s p%d=%s:%s", ps, i, p.NodeID, p.State)

		// if p.GivenUp {
		// 	ps = fmt.Sprintf("%s:GivenUp", ps)
		// }

		// if p.Attempts > 0 {
		// 	ps = fmt.Sprintf("%s:att=%d", ps, p.Attempts)
		// }

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

func (r *Range) Dirty() bool {
	r.Lock()
	defer r.Unlock()
	return r.dirty
}

// ???
func (r *Range) MinReady() int {
	return 1
}

func (r *Range) ToState(new RangeState) error {
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

	// Special case: When entering RsObsolete, fire the optional callback.
	// TODO: Maybe make this into a generic "when entering state x" callback?
	if new == RsObsolete {
		if r.onObsolete != nil {
			r.onObsolete()
		}
	}

	r.State = new
	r.dirty = true

	// TODO: What is this?
	log.Printf("%s %s -> %s", r, old, new)

	return nil
}

func (r *Range) OnObsolete(f func()) {
	r.Lock()
	defer r.Unlock()

	if r.onObsolete != nil {
		// This really shouldn't be possible, so panic.
		// TODO: Relax this later. It indicates a bug, but not a serious one.
		panic(fmt.Sprintf("range %d has non-nil onObsolete callback", r.Meta.Ident))
	}

	if r.State == RsObsolete {
		panic(fmt.Sprintf("cant attach onObsolete callback to obsolete range: %d", r.Meta.Ident))
	}

	r.onObsolete = f
}

// PlacementByNodeID returns the placement of this range with the given NodeID,
// or nil if no such range exists. This was added just for testing, and it
// should not be used elsewhere.
func (r *Range) PlacementByNodeID(nodeID string) *Placement {
	for _, p := range r.Placements {
		if p.NodeID == nodeID {
			return p
		}
	}

	return nil
}
