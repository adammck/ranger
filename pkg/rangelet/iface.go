package rangelet

import (
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
)

type Storage interface {
	Read() []*info.RangeInfo
	Write()
}

type Node interface {

	// These don't match the Prepare/Give/Take/Drop terminology used internally
	// by Ranger, but the Shard Manager paper uses these terms, so might as well
	// follow that.
	PrepareAddShard(m ranje.Meta) error
	AddShard(rID ranje.Ident) error
	PrepareDropShard(rID ranje.Ident) error

	// DropShard
	// Range state will be set to NsDropping before calling this. If an error is
	// returned, the range will be forgotten. If no error is returned, the range
	// state will be set to NsDroppingError.
	DropShard(rID ranje.Ident) error
}
