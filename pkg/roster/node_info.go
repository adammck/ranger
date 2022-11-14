package roster

import (
	"time"

	"github.com/adammck/ranger/pkg/api"
)

// TODO: Make node ID a proper type like range ID.

type NodeInfo struct {
	Time   time.Time
	NodeID api.NodeID
	Ranges []api.RangeInfo

	// Expired is true when the node was automatically expired because we
	// haven't been able to probe it in a while.
	Expired bool
}
