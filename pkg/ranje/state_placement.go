package ranje

import (
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

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

	case PsPending:
		return pb.PlacementState_PS_PENDING

	case PsPrepared:
		return pb.PlacementState_PS_PREPARED

	case PsReady:
		return pb.PlacementState_PS_READY

	case PsTaken:
		return pb.PlacementState_PS_TAKEN

	case PsGiveUp:
		return pb.PlacementState_PS_GIVE_UP

	case PsDropped:
		return pb.PlacementState_PS_DROPPED

	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown PlacementState value!")
}

func PlacementStateFromProto(s *pb.PlacementState) PlacementState {
	switch *s {
	case pb.PlacementState_PS_UNKNOWN:
		return PsUnknown

	case pb.PlacementState_PS_PENDING:
		return PsPending

	case pb.PlacementState_PS_PREPARED:
		return PsPrepared

	case pb.PlacementState_PS_READY:
		return PsReady

	case pb.PlacementState_PS_TAKEN:
		return PsTaken

	case pb.PlacementState_PS_GIVE_UP:
		return PsGiveUp

	case pb.PlacementState_PS_DROPPED:
		return PsDropped

	}

	log.Printf("warn: unknown PlacementState from proto: %v", *s)
	return PsUnknown
}
