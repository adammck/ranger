package ranje

import (
	"fmt"
	"log"
	"sync"

	"github.com/adammck/ranger/pkg/api"
)

// Placement represents a pair of range+node.
type Placement struct {
	// owned by Keyspace.
	// TODO: Make this a range Ident instead? Anyone who needs the range will
	//       probably have a RangeGetter like keyspace.
	rang *Range

	// NodeID is the Ident of the node this placement is assigned to. Immutable
	// after construction. (Create a new placement instead of changing it.)
	NodeID api.NodeID

	// StateCurrent is the controller-side state of the placement. It reflects
	// the actual remote state, as reported by the Rangelet via the Roster.
	//
	// Don't access this field directly! It's only public for deserialization
	// from the store. Modify it via ToState. This is currently violated all
	// over the place.
	StateCurrent api.PlacementState

	// StateDesired is the state the Orchestator would like this placement to be
	// in. The Actuator is responsible for telling the remote node about this.
	StateDesired api.PlacementState

	// Set by the orchestrator to indicate that this placement was created to
	// replace the placement of the same range on some other node. Should be
	// cleared once the placement activates.
	// TODO: Change this to some kind of uuid.
	IsReplacing api.NodeID `json:",omitempty"`

	// failures is updated by the actuator when an action is attempted a few
	// times but fails. This generally causes the placement to become wedged
	// until an operator intervenes.
	failures map[api.Action]bool

	// Not persisted.
	onDestroy func()
	onReady   func()

	// Guards everything.
	// TODO: What is "everything" ??
	// TODO: Change into an RWLock, check callers.
	// TODO: Should this also lock the range and node? I think no?
	sync.Mutex
}

// TODO: Get rid of this once deserialization works properly.
func (p *Placement) Repair(r *Range) {
	if p.rang != nil {
		panic("tried to repair valid placementn")
	}

	p.rang = r
}

// TODO: Rename this to just String?
func (p *Placement) LogString() string {
	return fmt.Sprintf("{%s %s:%s}", p.rang.Meta, p.NodeID, p.StateCurrent)
}

func (p *Placement) Range() *Range {
	return p.rang
}

// TODO: Remove this method (and Placement.IsReplacing).
func (p *Placement) DoneReplacing() {
	p.IsReplacing = ""
}

func (p *Placement) Want(new api.PlacementState) error {
	if err := CanTransitionPlacement(p.StateCurrent, new); err != nil {
		return err
	}

	p.StateDesired = new
	return nil
}

func (p *Placement) ToState(new api.PlacementState) error {
	if err := CanTransitionPlacement(p.StateCurrent, new); err != nil {
		return err
	}

	// Special case: When entering PsActive, fire the optional callback.
	if new == api.PsActive {
		if p.onReady != nil {
			p.onReady()
		}
	}

	old := p.StateCurrent
	p.StateCurrent = new
	p.failures = nil
	p.rang.dirty = true

	log.Printf("R%sP%d: %s -> %s", p.rang.Meta.Ident, p.rang.PlacementIndex(p.NodeID), old, new)

	return nil
}

func (p *Placement) OnReady(f func()) {
	p.Lock()
	defer p.Unlock()

	if p.onReady != nil {
		panic("placement already has non-nil onReady callback")
	}

	if p.StateCurrent != api.PsPending {
		panic(fmt.Sprintf(
			"can't attach onReady callback to non-pending placement (s=%v, rID=%v, nID=%v)",
			p.StateCurrent, p.rang.Meta.Ident, p.NodeID))
	}

	p.onReady = f
}

func (p *Placement) OnDestroy(f func()) {
	p.onDestroy = f
}

// Failed returns true if the given action has been attempted but has failed.
func (p *Placement) Failed(a api.Action) bool {
	if p.failures == nil {
		return false
	}

	return p.failures[a]
}

func (p *Placement) SetFailed(a api.Action, value bool) {
	if p.failures == nil {
		// lazy init, since most placements don't fail.
		p.failures = map[api.Action]bool{}
	}

	p.failures[a] = value
}
