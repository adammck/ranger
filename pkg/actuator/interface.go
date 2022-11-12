package actuator

import (
	"fmt"
	"log"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Impl interface {
	// TODO: This will definition change once the actuator initiates its own
	//       RPCs. It's only like this while I extract it from Orchestrator.
	Command(action api.Action, p *ranje.Placement, n *roster.Node) error

	// Wait blocks until any current actuations complete.
	Wait()
}

type Actuator struct {
	ks   *keyspace.Keyspace
	ros  *roster.Roster
	Impl Impl // TODO: Make private once orch tests fixed.
}

func New(ks *keyspace.Keyspace, ros *roster.Roster, impl Impl) *Actuator {
	return &Actuator{
		ks:   ks,
		ros:  ros,
		Impl: impl,
	}
}

// Tick checks every placement, and actuates it (e.g. sends an RPC) if the
// current state is not the desired state.
func (a *Actuator) Tick() {
	rs, unlock := a.ks.Ranges()
	defer unlock()

	for _, r := range rs {
		for _, p := range r.Placements {
			a.Consider(p)
		}
	}
}

func (a *Actuator) Wait() {
	a.Impl.Wait()
}

// TODO: Move these to scope?
const maxGiveFailures = 3
const maxTakeFailures = 3
const maxServeFailures = 3
const maxDropFailures = 30 // Not actually forever.

// TODO: Move this out to some outer actuator.
func (a *Actuator) Consider(p *ranje.Placement) {
	if p.StateDesired == p.StateCurrent {
		log.Printf("Actuator.Consider(%s:%s): nothing to do", p.Range().Meta.Ident, p.NodeID)
		return
	}

	if p.StateDesired == api.PsUnknown {
		log.Printf("Actuator.Consider(%s:%s): unknown desired state", p.Range().Meta.Ident, p.NodeID)
		return
	}

	act, err := actuation(p)
	if err != nil {
		// TODO: Should we return an error instead? What could the caller do with it?
		log.Printf("Actuator.Consider(%s:%s): %s", p.Range().Meta.Ident, p.NodeID, err)
		return
	}

	n := a.ros.NodeByIdent(p.NodeID)
	if n == nil {
		// TODO: Rerturn an error from NodeByIdent instead?
		log.Printf("Actuator.Consider(%s:%s): no such node", p.Range().Meta.Ident, p.NodeID)
		return
	}

	// TODO: This is a mess; use an act->int map instead
	skip := false
	switch act {
	case api.Give:
		if p.Failures >= maxGiveFailures {
			log.Printf("given up on placing (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
			p.FailedGive = true
			skip = true
		} else {
			log.Printf("failures: %d", p.Failures)
		}
	case api.Serve:
		if p.Failures >= maxServeFailures {
			log.Printf("given up on serving prepared placement (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
			p.FailedActivate = true
			skip = true
		} else {
			log.Printf("failures: %d", p.Failures)
		}
	case api.Take:
		if p.Failures >= maxTakeFailures {
			log.Printf("given up on deactivating placement (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			p.FailedDeactivate = true
			skip = true
		} else {
			log.Printf("failures: %d", p.Failures)
		}
	case api.Drop:
		if p.DropFailures >= maxDropFailures {
			log.Printf("drop failed after %d attempts (rID=%s, n=%s, attempt=%d)", p.Failures, p.Range().Meta.Ident, n.Ident(), p.Failures)
			p.FailedDrop = true
			skip = true
		} else {
			log.Printf("failures: %d", p.DropFailures)
		}
	default:
		// TODO: Use exhaustive analyzer?
		panic(fmt.Sprintf("unknown action: %v", act))
	}

	if skip {
		log.Printf("Actuator.Consider(%s:%s): skipping", p.Range().Meta.Ident, p.NodeID)
		return
	}

	err = a.Impl.Command(act, p, n)
	if err != nil {
		incrementError(p, act, n)
	}
}

// TODO: Move this out to some outer actuator.
type transitions struct {
	from api.PlacementState
	to   api.PlacementState
	act  api.Action
}

// TODO: Move this out to some outer actuator.
// Somewhat duplicated from placement_state.go.
var actuations = []transitions{
	{api.PsPending, api.PsInactive, api.Give},
	{api.PsInactive, api.PsActive, api.Serve},
	{api.PsActive, api.PsInactive, api.Take},
	{api.PsInactive, api.PsDropped, api.Drop},
}

// TODO: Move this out to some outer actuator.
func actuation(p *ranje.Placement) (api.Action, error) {
	for _, aa := range actuations {
		if p.StateCurrent == aa.from && p.StateDesired == aa.to {
			return aa.act, nil
		}
	}

	return api.NoAction, fmt.Errorf(
		"no actuation: from=%s, to:%s",
		p.StateCurrent.String(),
		p.StateDesired.String())
}

// TODO: This is a mess; use an act->int map instead
func incrementError(p *ranje.Placement, action api.Action, n *roster.Node) {
	switch action {
	case api.Give:
		log.Printf("failures: %d", p.Failures)
		p.Failures += 1
		if p.Failures >= maxGiveFailures {
			log.Printf("given up on placing (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
			p.FailedGive = true
		}

	case api.Serve:
		log.Printf("failures: %d", p.Failures)
		p.Failures += 1
		if p.Failures >= maxServeFailures {
			log.Printf("given up on serving prepared placement (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
			p.FailedActivate = true
		}

	case api.Take:
		log.Printf("failures: %d", p.Failures)
		p.Failures += 1
		if p.Failures >= maxTakeFailures {
			log.Printf("given up on deactivating placement (rID=%s, n=%s, attempt=%d)", p.Range().Meta.Ident, n.Ident(), p.Failures)
			p.FailedDeactivate = true
		}

	case api.Drop:
		log.Printf("failures: %d", p.DropFailures)
		p.DropFailures += 1
		if p.DropFailures >= maxDropFailures {
			log.Printf("drop failed after %d attempts (rID=%s, n=%s, attempt=%d)", p.Failures, p.Range().Meta.Ident, n.Ident(), p.Failures)
			p.FailedDrop = true
		}

	default:
		// TODO: Use exhaustive analyzer?
		panic(fmt.Sprintf("unknown action: %v", action))
	}
}
