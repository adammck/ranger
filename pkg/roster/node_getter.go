package roster

// NodeGetter allows callers to get a Node from its NodeIdent.
// TODO: NodeByIdent should probably return an error, not nil.
type NodeGetter interface {
	NodeByIdent(nodeIdent string) *Node
}
