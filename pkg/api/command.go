package api

import (
	"fmt"
)

type Command struct {
	RangeIdent RangeID
	NodeIdent  string
	Action     Action
}

func (t *Command) String() string {
	return fmt.Sprintf("%s(R%d, %s)", t.Action, t.RangeIdent, t.NodeIdent)
}

func (t *Command) Less(other Command) bool {
	if t.RangeIdent != other.RangeIdent {
		return t.RangeIdent < other.RangeIdent
	} else {
		return t.NodeIdent < other.NodeIdent
	}
}
