package ranje

// Placement represents a pair of range+node.
type Placement struct {
	rang  *Range
	node  *Node
	state StateRemote

	// loadinfo?
	// The number of keys that the range has.
	K uint64
}
