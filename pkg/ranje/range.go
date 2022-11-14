package ranje

import (
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/adammck/ranger/pkg/api"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	Meta api.Meta

	State    api.RangeState
	Parents  []api.RangeID
	Children []api.RangeID

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

func NewRange(rID api.RangeID) *Range {
	return &Range{
		Meta: api.Meta{
			Ident: rID,
		},

		// Born active, to be placed right away.
		// TODO: Maybe we need a pending state first? Do the parent ranges need
		//       to do anything else after this range is created but before it's
		//       placed?
		State: api.RsActive,

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
		ps = fmt.Sprintf("%s p%d=%s:%s", ps, i, p.NodeID, p.StateCurrent)

		// if p.GivenUpOnActivate {
		// 	ps = fmt.Sprintf("%s:GivenUpOnActivate", ps)
		// }

		// if p.GiveUpOnDeactivate {
		// 	ps = fmt.Sprintf("%s:GiveUpOnDeactivate", ps)
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

// MinPlacements returns the number of placements (in any state) that this range
// should have. If it has fewer than this, more should be created asap.
func (r *Range) MinPlacements() int {
	return 1
}

// MaxActive returns the maximum number of active placements that this range
// should ever have. Ranger will aim for exactly this number; any fewer, and
// more should be activated asap.
func (r *Range) MaxActive() int {
	return 1
}

// MinActive returns the minimum number of active placements that this range
// should ever have. This is the *lower bound*; there will usually be more, but
// during operations will be allowed to proceed so long as the number of active
// ranges is not below this number. It can be equal to this number!
func (r *Range) MinActive() int {
	return 0
}

func (r *Range) ToState(new api.RangeState) error {
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
	if new == api.RsObsolete {
		if r.onObsolete != nil {
			r.onObsolete()
		}
	}

	r.State = new
	r.dirty = true

	log.Printf("R%s: %s -> %s", r.Meta.Ident, old, new)

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

	if r.State == api.RsObsolete {
		panic(fmt.Sprintf("cant attach onObsolete callback to obsolete range: %d", r.Meta.Ident))
	}

	r.onObsolete = f
}

// PlacementByNodeID returns the placement of this range with the given NodeID,
// or nil if no such range exists. This was added just for testing, and it
// should not be used elsewhere.
func (r *Range) PlacementByNodeID(nodeID api.NodeID) *Placement {
	for _, p := range r.Placements {
		if p.NodeID == nodeID {
			return p
		}
	}

	return nil
}

// TODO: Remove this once replication arrives.
func (r *Range) PlacementIndex(nodeID api.NodeID) int {
	for i, p := range r.Placements {
		if p.NodeID == nodeID {
			return i
		}
	}

	return math.MaxInt
}
