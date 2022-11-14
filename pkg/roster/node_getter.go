package roster

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

// NodeGetter allows callers to get a Node from its NodeIdent.
type NodeGetter interface {
	NodeByIdent(nID api.NodeID) (*Node, error)
}

// ErrNodeNotFound is returned by NodeGetter implementations when NodeByIdent is
// called with a node which does not exist.
type ErrNodeNotFound struct {
	NodeID api.NodeID
}

func (e ErrNodeNotFound) Error() string {
	return fmt.Sprintf("no such node: %s", e.NodeID)
}
