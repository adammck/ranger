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
	Wait()
}
