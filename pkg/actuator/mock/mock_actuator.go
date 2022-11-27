package mock

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Actuator struct {
	injects map[api.Command]*Inject
	strict  bool

	commands   []api.Command
	unexpected []api.Command

	// mu guards everything.
	// No need for granularity.
	mu sync.Mutex
}

func New(strict bool) *Actuator {
	return &Actuator{
		injects:    map[api.Command]*Inject{},
		strict:     strict,
		commands:   []api.Command{},
		unexpected: []api.Command{},
	}
}

func (a *Actuator) Reset() {
	a.commands = []api.Command{}
	a.unexpected = []api.Command{}
}

func (a *Actuator) Unexpected() []api.Command {
	return a.unexpected
}

// TODO: This is currently duplicated.
func (a *Actuator) Command(cmd api.Command, p *ranje.Placement, n *roster.Node) error {
	s, err := a.cmd(cmd.Action, p, n)
	if err != nil {
		return err
	}

	// TODO: This special case is weird. It was less so when Prepare was a
	//       separate method. Think about it or something.
	if cmd.Action == api.Prepare {
		n.UpdateRangeInfo(&api.RangeInfo{
			Meta:  p.Range().Meta,
			State: s,
			Info:  api.LoadInfo{},
		})
	} else {
		n.UpdateRangeState(p.Range().Meta.Ident, s)
	}

	return nil
}

// command logs a command (to be retrived later via Commands), and returns the
// remote state which the (imaginary) remote node is now in, to be passed along.
// to the Roster. The default return given via def, but may be overriden via
// Expect to simulate failures.
func (a *Actuator) cmd(action api.Action, p *ranje.Placement, n *roster.Node) (api.RemoteState, error) {
	cmd := api.Command{
		RangeIdent: p.Range().Meta.Ident,
		NodeIdent:  n.Ident(),
		Action:     action,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

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
	api.Prepare:    api.NsInactive,
	api.Serve:      api.NsActive,
	api.Deactivate: api.NsInactive,
	api.Drop:       api.NsNotFound,
}

func mustDefault(action api.Action) api.RemoteState {
	s, ok := defaults[action]
	if !ok {
		panic(fmt.Sprintf("no default state for action: %s", action))
	}

	return s
}

// TODO: Make private once orch tests fixed.
type Inject struct {
	success bool
	ns      api.RemoteState
}

func (ij *Inject) Success() *Inject {
	ij.success = true
	return ij
}

func (ij *Inject) Failure() *Inject {
	ij.success = false
	return ij
}

func (ij *Inject) Response(ns api.RemoteState) *Inject {
	ij.ns = ns
	return ij
}

func (a *Actuator) Inject(nID api.NodeID, rID api.RangeID, act api.Action) *Inject {
	cmd := api.Command{
		RangeIdent: rID,
		NodeIdent:  nID,
		Action:     act,
	}

	exp := &Inject{
		success: true,
		ns:      api.NsUnknown,
	}

	a.mu.Lock()
	a.injects[cmd] = exp
	a.mu.Unlock()

	return exp
}

// Unject removes a hook.
func (a *Actuator) Unject(ij *Inject) {
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
