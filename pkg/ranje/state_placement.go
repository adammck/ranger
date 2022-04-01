package ranje

import pb "github.com/adammck/ranger/pkg/proto/gen"

type PlacementState uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	PsUnknown PlacementState = iota

	PsPending
)

//go:generate stringer -type=PlacementState -output=zzz_state_placement_string.go

func (s PlacementState) ToProto() pb.PlacementState {
	switch s {
	case PsUnknown:
		return pb.PlacementState_PS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown PlacementState value!")
}
