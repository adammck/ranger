package ranje

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

type Constraint struct {
	NodeID api.NodeID
	Not    []api.NodeID
}

func (c Constraint) String() string {
	if c.NodeID != "" {
		return fmt.Sprintf("nID=%s", c.NodeID)
	}

	// TODO: Include Not nIDs in here.

	return "any"
}

func (c Constraint) WithNot(nID api.NodeID) Constraint {
	not := make([]api.NodeID, len(c.Not)+1)
	copy(not, c.Not)
	not[len(not)-1] = nID
	c.Not = not
	return c
}

// AnyNode is an empty constraint, which matches... any node.
var AnyNode = Constraint{}
