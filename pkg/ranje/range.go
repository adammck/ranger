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

	// The replication config for this range. It's a pointer so it can point
	// back to the keyspace's default replication config and be updated
	// together. In theory it can be changed on a per-range basis, but that
	// isn't well tested as of today.
	repl *ReplicationConfig
}

func NewRange(rID api.RangeID, repl *ReplicationConfig) *Range {
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

		repl: repl,
	}
}

func (r *Range) Repair(repl *ReplicationConfig) {
	r.repl = repl
}

func (r *Range) NewPlacement(nodeID api.NodeID) *Placement {
	p := &Placement{
		rang:         r,
		NodeID:       nodeID,
		StateCurrent: api.PsPending,
		StateDesired: api.PsPending,
	}

	r.Placements = append(r.Placements, p)

	return p
}

// Special constructor for placements replacing some other placement.
//
// TODO: Remove this and fix doMove (the only caller) to taint the src placement
//       itself. Moves are really just tainting a placement and then creating a
//       new one-- the new one will naturally replace the old tainted one.
func (r *Range) NewReplacement(destNodeID api.NodeID, src *Placement) *Placement {
	p := &Placement{
		rang:         r,
		NodeID:       destNodeID,
		StateCurrent: api.PsPending,
		StateDesired: api.PsPending,
		IsReplacing:  src.NodeID,
	}

	r.Placements = append(r.Placements, p)

	return p
}

// DestroyPlacement removes the given placement from the range. This is the only
// was that should happen, to ensure that the onDestroy callback is called.
func (r *Range) DestroyPlacement(p *Placement) {
	for i, pp := range r.Placements {
		if p != pp {
			continue
		}

		if p.onDestroy != nil {
			p.onDestroy()
		}

		r.Placements = append(r.Placements[:i], r.Placements[i+1:]...)
		return
	}

	// This really should never happen. It indicates a concurrency bug, because
	// any thread destroying placements should be holding the keyspace lock.
	panic(fmt.Sprintf(
		"tried to destroy non-existent placement: r=%s, dest=%s",
		r.Meta.Ident, p.NodeID))
}

// TODO: This is only used by Keyspace.LogString, which is only used by tests!
//       So move it to the tests or use it elsewhere.
func (r *Range) LogString() string {
	ps := ""

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

		// if p.IsReplacing != "" {
		// 	ps = fmt.Sprintf("%s:replacing(%v)", ps, p.IsReplacing)
		// }

		if p.Tainted {
			ps = fmt.Sprintf("%s:tainted", ps)
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
	return r.repl.MinPlacements
}

// MaxPlacements return the maximum number of placements that this range should
// ever have. It's fine to be lower, but no more than this will be created, even
// if that means that an operation can't proceed.
func (r *Range) MaxPlacements() int {
	return r.repl.MaxPlacements
}

// MaxActive returns the maximum number of active placements that this range
// should ever have. Ranger will aim for exactly this number; any fewer, and
// more should be activated asap.
func (r *Range) MaxActive() int {
	return r.repl.MaxActive
}

// MinActive returns the minimum number of active placements that this range
// should ever have. This is the *lower bound*; there will usually be more, but
// during operations will be allowed to proceed so long as the number of active
// ranges is not below this number. It can be equal to this number!
func (r *Range) MinActive() int {
	return r.repl.MinActive
}

// NumPlacements calls the given func for each placement, and returns the number
// of which return true. This is useful when checking whether there are enough
// placements with some complex property.
func (r *Range) NumPlacements(f func(p *Placement) bool) int {
	n := 0

	for _, p := range r.Placements {
		if f(p) {
			n += 1
		}
	}

	return n
}

// NumPlacementsInState returns the number of placements currently in the given
// state. It's just a handy wrapper around a common usage of NumPlacements.
//
// TODO: This is currently only used when comparing to r.MaxActive! Maybe change
//       it to just do that? Seems kind of weird.
//
func (r *Range) NumPlacementsInState(s api.PlacementState) int {
	return r.NumPlacements(func(p *Placement) bool {
		return p.StateCurrent == s
	})
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
