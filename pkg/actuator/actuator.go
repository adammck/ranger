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
	// TODO: This can probably be simplified further. Ideally just the command,
	//       and the implementation can embed a keyspace or roster to look the
	//       other stuff up if they want.
	Command(cmd api.Command, p *ranje.Placement, n *roster.Node) error
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
	inFlight   map[api.Command]struct{}
	inFlightMu sync.Mutex

	// TODO: Use api.Command as the key.
	// TODO: Trim contents periodically.
	failures   map[api.Command][]time.Time
	failuresMu sync.RWMutex

	backoff time.Duration
}

func New(ks *keyspace.Keyspace, ros *roster.Roster, backoff time.Duration, impl Impl) *Actuator {
	return &Actuator{
		ks:       ks,
		ros:      ros,
		Impl:     impl,
		inFlight: map[api.Command]struct{}{},
		failures: map[api.Command][]time.Time{},
		backoff:  backoff,
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

	cmd := api.Command{
		RangeIdent: p.Range().Meta.Ident,
		NodeIdent:  n.Remote.Ident,
		Action:     action,
	}

	// TODO: Use a proper increasing backoff and jitter.
	// TODO: Also use clockwork to make this testable.
	if a.backoff > 0 && a.LastFailure(cmd).After(time.Now().Add(-a.backoff)) {
		log.Printf("Actuator.Consider(%s:%s): backing off",
			p.Range().Meta.Ident, p.NodeID)
		return
	}

	a.Exec(cmd, p, n)
}

func (a *Actuator) Exec(cmd api.Command, p *ranje.Placement, n *roster.Node) {
	a.inFlightMu.Lock()
	_, ok := a.inFlight[cmd]
	if !ok {
		a.inFlight[cmd] = struct{}{}
	}
	a.inFlightMu.Unlock()

	if ok {
		log.Printf("dropping in-flight command: %s", cmd)
		return
	}

	a.wg.Add(1)

	go func() {

		// TODO: Inject some client-side chaos here, too. RPCs complete very
		//       quickly locally, which doesn't test our in-flight thing well.

		err := a.Impl.Command(cmd, p, n)
		if err != nil {
			a.incrementError(cmd, p, n)
		}

		a.inFlightMu.Lock()
		if _, ok := a.inFlight[cmd]; !ok {
			// Critical this works, because could drop all RPCs. Note that we
			// don't release the lock, so no more RPCs even if the panic is
			// caught, which it shouldn't be.
			panic(fmt.Sprintf("no record of in-flight command: %s", cmd))
		}

		delete(a.inFlight, cmd)
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

func (a *Actuator) incrementError(cmd api.Command, p *ranje.Placement, n *roster.Node) {
	f := 0
	func() {
		a.failuresMu.Lock()
		defer a.failuresMu.Unlock()

		t, ok := a.failures[cmd]
		if !ok {
			t = []time.Time{}
		}

		t = append(t, time.Now())
		a.failures[cmd] = t

		f = len(t)
	}()

	if f >= maxFailures[cmd.Action] {
		delete(a.failures, cmd)
		p.SetFailed(cmd.Action, true)

		// TODO: Can this go somewhere else? The roster needs to know that the
		//       failure happened so it can avoid placing ranges on the node.
		if cmd.Action == api.Give || cmd.Action == api.Serve {
			n.PlacementFailed(p.Range().Meta.Ident, time.Now())
		}
	}
}

func (a *Actuator) LastFailure(cmd api.Command) time.Time {
	a.failuresMu.RLock()
	defer a.failuresMu.RUnlock()

	t, ok := a.failures[cmd]
	if !ok {
		return time.Time{}
	}

	return t[len(t)-1]
}
