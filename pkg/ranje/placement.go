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

	// Controller-side State machine.
	// Never modify this field directly! It's only public for deserialization
	// from the store. Modify it via ToState.
	State api.PlacementState

	// Set by the orchestrator to indicate that this placement was created to
	// replace the placement of the same range on some other node. Should be
	// cleared once the placement becomes ready.
	// TODO: Change this to some kind of uuid.
	IsReplacing string `json:",omitempty"` // NodeID

	// How many times has this placement failed to advance to the next state?
	// Orchestrator uses this to determine whether to give up or try again.
	// Reset by ToState.
	// TODO: Split this up into separate transitions, like DropAttempts.
	Attempts int

	// FailedGive is set by the orchestrator if the placement has been asked to
	// give (to the specified NodeID) a few times but has failed. This indicates
	// that it won't be attempted again.
	// TODO: There is no failed give! The placement is just destroyed.
	//FailedGive bool

	// Once this is set, the placement is destined to be destroyed. It's never
	// unset. Might take a few ticks in order to unwind things gracefully,
	// depending on the state which the placement and its family are in.
	FailedActivate bool

	// The placement was attempted to be deactivated a few times, but the node
	// refused. This is a really weird situation. But we need to stop trying
	// eventually, so the replacements can be dropped and (presumably) an
	// operator can be alerted.
	FailedDeactivate bool

	// How many times has this place been commanded to drop?
	DropAttempts int

	// The placement is inactive, but failed to drop. Probably no harm done,
	// except that the node can't release the resources. An operator should be
	// alerted.
	FailedDrop bool

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
		rang:   r,
		NodeID: nodeID,
		State:  api.PsPending,
	}
}

// Special constructor for placements replacing some other placement.
func NewReplacement(r *Range, destNodeID, srcNodeID string, done func()) *Placement {
	return &Placement{
		rang:        r,
		NodeID:      destNodeID,
		State:       api.PsPending,
		IsReplacing: srcNodeID,
		replaceDone: done,
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
	return fmt.Sprintf("{%s %s:%s}", p.rang.Meta, p.NodeID, p.State)
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

func (p *Placement) ToState(new api.PlacementState) error {
	ok := false
	old := p.State

	for _, t := range PlacementStateTransitions {
		if t.from == old && t.to == new {
			ok = true
			break
		}
	}

	if !ok {
		return fmt.Errorf("invalid placement state transition: %s -> %s", old.String(), new.String())
	}

	// Special case: When entering PsActive, fire the optional callback.
	if new == api.PsActive {
		if p.onReady != nil {
			p.onReady()
		}
	}

	p.State = new
	p.Attempts = 0
	p.FailedActivate = false
	p.FailedDeactivate = false
	p.rang.dirty = true

	// TODO: Make this less weird
	log.Printf("R%d P %s -> %s", p.rang.Meta.Ident, old, new)

	return nil
}

func (p *Placement) OnReady(f func()) {
	p.Lock()
	defer p.Unlock()

	if p.onReady != nil {
		panic("placement already has non-nil onReady callback")
	}

	if p.State != api.PsPending {
		panic(fmt.Sprintf(
			"can't attach onReady callback to non-pending placement (s=%v, rID=%v, nID=%v)",
			p.State, p.rang.Meta.Ident, p.NodeID))
	}

	p.onReady = f
}
