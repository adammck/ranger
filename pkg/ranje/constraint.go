package ranje

import "fmt"

type Constraint struct {
	NodeID string
}

func (c Constraint) String() string {
	if c.NodeID != "" {
		return fmt.Sprintf("nID=%s", c.NodeID)
	}

	return "any"
}

// AnyNode is an empty constraint, which matches... any node.
var AnyNode = Constraint{}
