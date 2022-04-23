package rangelet

import (
	"github.com/adammck/ranger/pkg/ranje"
)

// These don't match the Prepare/Give/Take/Drop terminology used internally by
// Ranger, but the Shard Manager paper uses these terms. Better to follow those
// than invent our own.
type Node interface {

	// PrepareAddShard.
	PrepareAddShard(m ranje.Meta) error

	// AddShard
	AddShard(rID ranje.Ident) error

	// PrepareDropShard
	PrepareDropShard(rID ranje.Ident) error

	// DropShard
	// Range state will be set to NsDropping before calling this. If an error is
	// returned, the range will be forgotten. If no error is returned, the range
	// state will be set to NsDroppingError.
	DropShard(rID ranje.Ident) error
}
