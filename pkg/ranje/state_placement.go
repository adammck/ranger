package ranje

import pb "github.com/adammck/ranger/pkg/proto/gen"

type PlacementState uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	PsUnknown PlacementState = iota

	PsPending
	PsPrepared
	PsReady
	PsTaken
	PsGiveUp
	PsDropped
)

type PlacementStateTransition struct {
	from PlacementState
	to   PlacementState
}

var PlacementStateTransitions []PlacementStateTransition

func init() {
	PlacementStateTransitions = []PlacementStateTransition{
		// Happy Path
		{PsPending, PsPrepared},
		{PsPrepared, PsReady},
		{PsReady, PsTaken},
		{PsTaken, PsDropped},

		// Error paths
		{PsPending, PsGiveUp},
		{PsPrepared, PsGiveUp},
		{PsReady, PsGiveUp},
		{PsTaken, PsGiveUp},

		// Recovery?
		{PsGiveUp, PsDropped},
	}
}

//go:generate stringer -type=PlacementState -output=zzz_state_placement_string.go

func (s PlacementState) ToProto() pb.PlacementState {
	switch s {
	case PsUnknown:
		return pb.PlacementState_PS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown PlacementState value!")
}
