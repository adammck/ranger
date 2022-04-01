package ranje

import pb "github.com/adammck/ranger/pkg/proto/gen"

type StatePlacement uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	SpUnknown StatePlacement = iota
)

//go:generate stringer -type=StatePlacement -output=zzz_state_placement_string.go

func (s StatePlacement) ToProto() pb.PlacementState {
	switch s {
	case SpUnknown:
		return pb.PlacementState_PS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown StatePlacement value!")
}
