package ranje

// Placement represents a pair of range+node.
type Placement struct {
	rang *Range
	node *Node

	// Warning! This may not be accurate! The range may have changed state on
	// the remote node since the last successful probe, or the node may have
	// gone away. This is what we *think* the state is.
	state StateRemote

	// loadinfo?
	// The number of keys that the range has.
	K uint64
}

func (p *Placement) Addr() string {

	// This should definitely not ever happen
	if p.node == nil {
		panic("nil node for placement")
		//return ""
	}

	return p.node.addr()
}
