package ranje

import (
	"fmt"
	"log"
	"sync"

	"github.com/adammck/ranger/pkg/api"
)

// Placement represents a pair of range+node.
type Placement struct {
	rang   *Range // owned by Keyspace.
	NodeID string

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
	// cleared once the placement becomes ready.
	// TODO: Change this to some kind of uuid.
	IsReplacing string `json:",omitempty"` // NodeID

	// failures is updated by the actuator when an action is attempted a few
	// times but fails. This generally causes the placement to become wedged
	// until an operator intervenes.
	failures map[api.Action]bool

	// Not persisted.
	replaceDone func()
	onReady     func()

	// Guards everything.
	// TODO: What is "everything" ??
	// TODO: Change into an RWLock, check callers.
	// TODO: Should this also lock the range and node? I think no?
	sync.Mutex
}

func NewPlacement(r *Range, nodeID string) *Placement {
	return &Placement{
		rang:         r,
		NodeID:       nodeID,
		StateCurrent: api.PsPending,
		StateDesired: api.PsPending,
	}
}

// Special constructor for placements replacing some other placement.
func NewReplacement(r *Range, destNodeID, srcNodeID string, done func()) *Placement {
	return &Placement{
		rang:         r,
		NodeID:       destNodeID,
		StateCurrent: api.PsPending,
		StateDesired: api.PsPending,
		IsReplacing:  srcNodeID,
		replaceDone:  done,
	}
}

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

func (p *Placement) DoneReplacing() {
	p.IsReplacing = ""

	// Callback to unblock operator Move RPCs.
	// TODO: This is kind of dumb. Would be better to store the callbacks
	//       somewhere else, and look them up when calling this method.
	if p.replaceDone != nil {
		p.replaceDone()
	}
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

	p.StateCurrent = new
	p.failures = nil
	p.rang.dirty = true

	// TODO: Make this less weird
	log.Printf("R%d P %s -> %s", p.rang.Meta.Ident, p.StateCurrent, new)

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
