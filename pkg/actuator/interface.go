package actuator

import (
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Actuator interface {
	// TODO: This will definition change once the actuator initiates its own
	//       RPCs. It's only like this while I extract it from Orchestrator.
	Command(action api.Action, p *ranje.Placement, n *roster.Node)

	// Tick checks every placement, and actuates it (e.g. sends an RPC) if the
	// current state is not the desired state. This should probably be replaced
	// with something event-based.
	Tick()

	// Wait blocks until any current actuations complete.
	Wait()
}
