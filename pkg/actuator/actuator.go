package actuator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Impl interface {
	Command(action api.Action, p *ranje.Placement, n *roster.Node) error
}

type Actuator struct {
	ks   *keyspace.Keyspace
	ros  *roster.Roster
	Impl Impl // TODO: Make private once orch tests fixed.

	// TODO: Move this into the Tick method; just pass it along.
	wg sync.WaitGroup

	// RPCs which have been sent (via orch.RPC) but not yet completed. Used to
	// avoid sending the same RPC redundantly every single tick. (Though RPCs
	// *can* be re-sent an arbitrary number of times. Rangelet will dedup them.)
	// TODO: Use api.Command as the key.
	inFlight   map[string]struct{}
	inFlightMu sync.Mutex

	// TODO: Use api.Command as the key.
	// TODO: Trim contents periodically.
	failures   map[string][]time.Time
	failuresMu sync.Mutex
}

func New(ks *keyspace.Keyspace, ros *roster.Roster, impl Impl) *Actuator {
	return &Actuator{
		ks:       ks,
		ros:      ros,
		Impl:     impl,
		inFlight: map[string]struct{}{},
		failures: map[string][]time.Time{},
	}
}

func (a *Actuator) Run(t *time.Ticker) {
	// TODO: Replace this with something reactive; maybe chan from keyspace?
	for ; true; <-t.C {
		a.Tick()
	}
}

// Tick checks every placement, and actuates it (e.g. sends an RPC) if the
// current state is not the desired state.
func (a *Actuator) Tick() {
	rs, unlock := a.ks.Ranges()
	defer unlock()

	for _, r := range rs {
		for _, p := range r.Placements {
			a.consider(p)
		}
	}
}

func (a *Actuator) Wait() {
	a.wg.Wait()
}

var maxFailures = map[api.Action]int{
	api.Give:  3,
	api.Take:  3,
	api.Serve: 3,
	api.Drop:  30, // Not quite forever
}

func (a *Actuator) consider(p *ranje.Placement) {
	if p.StateDesired == p.StateCurrent {
		log.Printf("Actuator.Consider(%s:%s): nothing to do",
			p.Range().Meta.Ident, p.NodeID)
		return
	}

	if p.StateDesired == api.PsUnknown {
		log.Printf("Actuator.Consider(%s:%s): unknown desired state",
			p.Range().Meta.Ident, p.NodeID)
		return
	}

	action, err := actuation(p)
	if err != nil {
		// TODO: Should we return an error instead? What could the caller do with it?
		log.Printf("Actuator.Consider(%s:%s): %s",
			p.Range().Meta.Ident, p.NodeID, err)
		return
	}

	n := a.ros.NodeByIdent(p.NodeID)
	if n == nil {
		// TODO: Rerturn an error from NodeByIdent instead?
		log.Printf("Actuator.Consider(%s:%s): no such node",
			p.Range().Meta.Ident, p.NodeID)
		return
	}

	if p.Failed(action) {
		log.Printf("Actuator.Consider(%s:%s): command previously failed",
			p.Range().Meta.Ident, p.NodeID)
		return
	}

	a.Exec(action, p, n)
}

func (a *Actuator) Exec(action api.Action, p *ranje.Placement, n *roster.Node) {
	key := fmt.Sprintf("%v:%s:%s", action, p.Range().Meta.Ident, n.Remote.Ident)

	a.inFlightMu.Lock()
	_, ok := a.inFlight[key]
	if !ok {
		a.inFlight[key] = struct{}{}
	}
	a.inFlightMu.Unlock()

	if ok {
		log.Printf("dropping in-flight command: %s", key)
		return
	}

	a.wg.Add(1)

	go func() {

		// TODO: Inject some client-side chaos here, too. RPCs complete very
		//       quickly locally, which doesn't test our in-flight thing well.

		err := a.Impl.Command(action, p, n)
		if err != nil {
			a.incrementError(action, p, n)
		}

		a.inFlightMu.Lock()
		if _, ok := a.inFlight[key]; !ok {
			// Critical this works, because could drop all RPCs. Note that we
			// don't release the lock, so no more RPCs even if the panic is
			// caught, which it shouldn't be.
			panic(fmt.Sprintf("no record of in-flight command: %s", key))
		}
		log.Printf("command completed: %s", key)
		delete(a.inFlight, key)
		a.inFlightMu.Unlock()

		a.wg.Done()
	}()
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

func (a *Actuator) incrementError(action api.Action, p *ranje.Placement, n *roster.Node) {
	key := fmt.Sprintf("%v:%s:%s", action, p.Range().Meta.Ident, n.Remote.Ident)

	f := 0
	func() {
		a.failuresMu.Lock()
		defer a.failuresMu.Unlock()

		t, ok := a.failures[key]
		if !ok {
			t = []time.Time{}
		}

		t = append(t, time.Now())
		a.failures[key] = t

		f = len(t)
	}()

	if f >= maxFailures[action] {
		delete(a.failures, key)
		p.SetFailed(action, true)

		// TODO: Can this go somewhere else? The roster needs to know that the
		//       failure happened so it can avoid placing ranges on the node.
		if action == api.Give || action == api.Serve {
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
		}
	}
}
