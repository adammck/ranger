package api

// NodeID is the unique identity of a node.
// N.b. we used to just use naked strings for this, so it's possible that some
// of those hanging around. Use this instead.
type NodeID string

const ZeroNodeID NodeID = ""

func (nID NodeID) String() string {
	return string(nID)
}
