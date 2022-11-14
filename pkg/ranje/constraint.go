package ranje

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

type Constraint struct {
	NodeID api.NodeID
}

func (c Constraint) String() string {
	if c.NodeID != "" {
		return fmt.Sprintf("nID=%s", c.NodeID)
	}

	return "any"
}

// AnyNode is an empty constraint, which matches... any node.
var AnyNode = Constraint{}
