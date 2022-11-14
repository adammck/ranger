package roster

import "github.com/adammck/ranger/pkg/api"

// NodeGetter allows callers to get a Node from its NodeIdent.
// TODO: NodeByIdent should probably return an error, not nil.
type NodeGetter interface {
	NodeByIdent(nodeIdent api.NodeID) *Node
}
