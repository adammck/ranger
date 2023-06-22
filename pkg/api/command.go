package api

import (
	"fmt"
	"strings"
	"time"
)

type Command struct {
	RangeIdent RangeID
	NodeIdent  NodeID
	Action     Action

	// Must be non-zero when Action == Activate, otherwise must be zero.
	Expires time.Time
}

func (c Command) String() string {
	s := make([]string, 2, 3)
	s[0] = fmt.Sprintf("R%d", c.RangeIdent)
	s[1] = string(c.NodeIdent)

	if c.Action == Activate {
		s = append(s, fmt.Sprint(c.Expires.Unix()))

		// This should never happen, because the command should never have been
		// allowed to be constructed this way. It's just a runtime assert.
		// TODO: Add a constructor with an err field.
		// TODO: Remove this check eventually.
		if c.Expires.IsZero() {
			panic("activate command with no lease")
		}
	}

	return fmt.Sprintf("%s(%s)", c.Action, strings.Join(s, ", "))
}

func (c Command) Less(other Command) bool {
	if c.RangeIdent != other.RangeIdent {
		return c.RangeIdent < other.RangeIdent
	} else {
		return c.NodeIdent < other.NodeIdent
	}
}
