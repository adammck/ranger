package rangelet

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

// These don't match the Prepare/Give/Take/Drop terminology used internally by
// Ranger, but the Shard Manager paper uses *roughly* these terms. Better to follow those
// than invent our own.
type Node interface {

	// PrepareAddRange.
	PrepareAddRange(m ranje.Meta, p []Parent) error

	// AddRange
	AddRange(rID ranje.Ident) error

	// PrepareDropRange
	PrepareDropRange(rID ranje.Ident) error

	// DropRange
	// Range state will be set to NsDropping before calling this. If an error is
	// returned, the range will be forgotten. If no error is returned, the range
	// state will be set to NsDroppingError.
	DropRange(rID ranje.Ident) error
}
