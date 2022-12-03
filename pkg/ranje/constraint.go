package ranje

import (
	"fmt"
	"strings"

	"github.com/adammck/ranger/pkg/api"
)

type Constraint struct {
	NodeID api.NodeID
	Not    []api.NodeID
}

func (c Constraint) Copy() Constraint {
	not := make([]api.NodeID, len(c.Not))
	copy(not, c.Not)

	return Constraint{
		NodeID: c.NodeID,
		Not:    not,
	}
}

func (c Constraint) String() string {
	tokens := []string{}

	if c.NodeID != "" {
		tokens = append(tokens, fmt.Sprintf("nID(%s)", c.NodeID))
	}

	if len(c.Not) > 0 {
		nots := make([]string, len(c.Not))
		for i := range c.Not {
			nots[i] = c.Not[i].String()
		}
		tokens = append(tokens, fmt.Sprintf("not(%s)", strings.Join(nots, ",")))
	}

	// TODO: Include Not nIDs in here.

	if len(tokens) == 0 {
		tokens = append(tokens, "any")
	}

	return fmt.Sprintf("Constraint{%s}", strings.Join(tokens, ","))
}

func (c Constraint) WithNot(nID api.NodeID) Constraint {
	new := c.Copy()
	new.Not = append(new.Not, nID)
	return new
}

func (c Constraint) WithNodeID(nID api.NodeID) Constraint {
	new := c.Copy()
	new.NodeID = nID
	return new
}

// AnyNode is an empty constraint, which matches... any node.
var AnyNode = Constraint{}
