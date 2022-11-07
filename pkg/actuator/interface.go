package actuator

import (
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Actuator interface {
	Give(p *ranje.Placement, n *roster.Node)
	Serve(p *ranje.Placement, n *roster.Node)
	Take(p *ranje.Placement, n *roster.Node)
	Drop(p *ranje.Placement, n *roster.Node)

	// TODO: Remove this from the interface. Test actuators which need
	//       synchronization can have their own methods for that.
	WaitRPCs()
}
