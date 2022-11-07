package mock

import (
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
)

type Actuator struct {
	ks  *keyspace.Keyspace
	ros *roster.Roster
}

func New(ks *keyspace.Keyspace, ros *roster.Roster) *Actuator {
	return &Actuator{
		ks:  ks,
		ros: ros,
	}
}

func (a *Actuator) WaitRPCs() {
	// Do nothing
}

func (a *Actuator) Give(p *ranje.Placement, n *roster.Node) {
	n.UpdateRangeInfo(&info.RangeInfo{
		Meta:  p.Range().Meta,
		State: state.NsInactive,
		Info:  info.LoadInfo{},
	})
}

func (a *Actuator) Serve(p *ranje.Placement, n *roster.Node) {
	n.UpdateRangeState(p.Range().Meta.Ident, state.NsActive)
}

func (a *Actuator) Take(p *ranje.Placement, n *roster.Node) {
	n.UpdateRangeState(p.Range().Meta.Ident, state.NsInactive)
}

func (a *Actuator) Drop(p *ranje.Placement, n *roster.Node) {
	n.UpdateRangeState(p.Range().Meta.Ident, state.NsNotFound)
}
