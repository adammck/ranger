package api

import (
	"github.com/adammck/ranger/pkg/ranje"
)

type Placement struct {
	Node  string
	State ranje.PlacementState
}

type Parent struct {
	Meta       ranje.Meta
	Parents    []ranje.Ident
	Placements []Placement
}

// Same as roster/info.LoadInfo, to avoid circular import.
type LoadInfo struct {
	Keys int
}
