package ranje

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
)

// Placement represents a pair of range+node.
type Placement struct {
	// owned by Keyspace.
	//
	// TODO: Make this a range Ident instead? Anyone who needs the range will
	// probably have a RangeGetter like keyspace.
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

	// Set by the orchestrator to indicate that this placement should be
	// deactivated and dropped when possible. This won't actually happen until
	// it's possible to do so within the min/max placement boundaries.
	//
	// (Adding a placement without tainting the old one will result in the new
	// one sitting at Inactive indefinitely, since there's no reason for the old
	// one to deactivate itself.)
	Tainted bool `json:",omitempty"`

	// Must be set before setting StateDesired to PsActive.
	// TODO: Make this private, set it via Want (or new Activate method?)
	ActivationLeaseExpires time.Time

	// failures is updated by the actuator when an action is attempted a few
	// times but fails. This generally causes the placement to become wedged
	// until an operator intervenes.
	failures map[api.Action]bool

	// Not persisted.
	onDestroy func()

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

func (p *Placement) Want(new api.PlacementState) error {
	if new == api.PsActive && p.ActivationLeaseExpires.IsZero() {
		// This should never happen, and indicates a bug
		return fmt.Errorf("can't activate range without lease")
	}

	if err := CanTransitionPlacement(p.StateCurrent, new); err != nil {
		return err
	}

	p.StateDesired = new
	return nil
}

func (p *Placement) GiveLease(t time.Time) {
	p.ActivationLeaseExpires = t
}

func (p *Placement) ToState(new api.PlacementState) error {
	if err := CanTransitionPlacement(p.StateCurrent, new); err != nil {
		return err
	}

	old := p.StateCurrent
	p.StateCurrent = new
	p.failures = nil
	p.rang.dirty = true

	log.Printf("R%sP%d: %s -> %s", p.rang.Meta.Ident, p.rang.PlacementIndex(p.NodeID), old, new)

	return nil
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
