package api

import (
	"errors"

	"github.com/adammck/ranger/pkg/ranje"
)

var NotFound = errors.New("EOF")

// These don't match the Prepare/Give/Take/Drop terminology used internally by
// Ranger, but Shard Manager uses roughly (s/Shard/Range/g) these terms. Better
// to follow those than invent our own.
type Node interface {

	// GetLoadInfo returns the LoadInfo for the given range.
	// Implementations should return NotFound if (from their point of view) the
	// range doesn't exist. This can happen when GetLoadInfo and PrepareAddRange
	// and/or DropRange are racing.
	GetLoadInfo(rID ranje.Ident) (LoadInfo, error)

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
