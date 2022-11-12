package mock

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Actuator struct {
	ks  *keyspace.Keyspace
	ros *roster.Roster

	injects map[Command]*inject
	strict  bool

	commands   []Command
	unexpected []Command

	// mu guards everything.
	// No need for granularity.
	mu sync.Mutex
}

func New(ks *keyspace.Keyspace, ros *roster.Roster, strict bool) *Actuator {
	return &Actuator{
		ks:         ks,
		ros:        ros,
		injects:    map[Command]*inject{},
		strict:     strict,
		commands:   []Command{},
		unexpected: []Command{},
	}
}

// TODO: Move this out to some outer actuator.
func (a *Actuator) Tick() {
	rs, unlock := a.ks.Ranges()
	defer unlock()

	for _, r := range rs {
		for _, p := range r.Placements {
			a.Consider(p)
		}
	}
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

	a.Command(act, p, n)
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

func (a *Actuator) Reset() {
	a.commands = []Command{}
	a.unexpected = []Command{}
}

func (a *Actuator) Unexpected() []Command {
	return a.unexpected
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

// TODO: This is currently duplicated.
func (a *Actuator) Command(action api.Action, p *ranje.Placement, n *roster.Node) {
	s, err := a.cmd(action, p, n)
	if err != nil {
		incrementError(p, action, n)
		return
	}

	// TODO: This special case is weird. It was less so when Give was a
	//       separate method. Think about it or something.
	if action == api.Give {
		n.UpdateRangeInfo(&api.RangeInfo{
			Meta:  p.Range().Meta,
			State: s,
			Info:  api.LoadInfo{},
		})
	} else {
		n.UpdateRangeState(p.Range().Meta.Ident, s)
	}
}

func (a *Actuator) Wait() {
}

// command logs a command (to be retrived later via Commands), and returns the
// remote state which the (imaginary) remote node is now in, to be passed along.
// to the Roster. The default return given via def, but may be overriden via
// Expect to simulate failures.
func (a *Actuator) cmd(action api.Action, p *ranje.Placement, n *roster.Node) (api.RemoteState, error) {
	cmd := Command{
		rID: p.Range().Meta.Ident,
		nID: n.Ident(),
		act: action,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	log.Print(cmd.String())
	a.commands = append(a.commands, cmd)
	exp, ok := a.injects[cmd]

	// If strict mode is enabled (i.e. we expected all commands to be mocked),
	// and this command is not mocked, append it to unexpected commands. This
	// will (hopefully) be checked later by a test helper at the end of the
	// tick, to avoid the error message leading here, devoid of context.
	if a.strict && exp == nil {
		a.unexpected = append(a.unexpected, cmd)
		return api.NsUnknown, fmt.Errorf("no hook injected for command while strict enabled: %s", cmd.String())
	}

	// Default (no override) is to succeed and advance to the default.
	if !ok {
		return mustDefault(action), nil
	}

	if exp.success {
		return exp.ns, nil
	} else {
		// TODO: Allow actual errors to be injected?
		return api.NsUnknown, errors.New("injected error")
	}
}

// Default resulting state of each action. Note that we don't validate that the
// fake remote transition at all, because real nodes (with rangelets) can assume
// whatever state they like.
var defaults = map[api.Action]api.RemoteState{
	api.Give:  api.NsInactive,
	api.Serve: api.NsActive,
	api.Take:  api.NsInactive,
	api.Drop:  api.NsNotFound,
}

func mustDefault(action api.Action) api.RemoteState {
	s, ok := defaults[action]
	if !ok {
		panic(fmt.Sprintf("no default state for action: %s", action))
	}

	return s
}

type inject struct {
	success bool
	ns      api.RemoteState
}

func (ij *inject) Success() *inject {
	ij.success = true
	return ij
}

func (ij *inject) Failure() *inject {
	ij.success = false
	return ij
}

func (ij *inject) Response(ns api.RemoteState) *inject {
	ij.ns = ns
	return ij
}

func (a *Actuator) Inject(nID string, rID api.Ident, act api.Action) *inject {
	cmd := Command{
		nID: nID,
		rID: rID,
		act: act,
	}

	exp := &inject{
		success: true,
		ns:      api.NsUnknown,
	}

	a.mu.Lock()
	a.injects[cmd] = exp
	a.mu.Unlock()

	return exp
}

// Unject removes a hook.
func (a *Actuator) Unject(ij *inject) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range a.injects {
		if ij == v {
			delete(a.injects, k)
			return
		}
	}

	panic(fmt.Sprintf("unknown inject: %v", ij))
}

func (a *Actuator) Commands() string {
	a.mu.Lock()
	cmds := a.commands
	a.mu.Unlock()

	// Sort them into constant order.
	sort.Slice(cmds, func(i, j int) bool {
		return cmds[i].Less(cmds[j])
	})

	// Cast to strings.
	strs := make([]string, len(cmds))
	for i := range cmds {
		strs[i] = cmds[i].String()
	}

	// Return a single string.
	return strings.Join(strs, ", ")
}
