package ranje

import (
	"fmt"
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type PlacementState uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	PsUnknown PlacementState = iota

	PsPending
	PsInactive
	PsActive
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
		{PsPending, PsInactive}, // Give
		{PsInactive, PsActive},  // Serve
		{PsActive, PsInactive},  // Take
		{PsInactive, PsDropped}, // Drop

		// Node crashed (or placement mysteriously vanished)
		{PsPending, PsGiveUp},
		{PsInactive, PsGiveUp},
		{PsActive, PsGiveUp},

		// Recovery?
		{PsGiveUp, PsDropped},
	}
}

//go:generate stringer -type=PlacementState -output=placement_state_string.go

func PlacementStateToProto(s PlacementState) pb.PlacementState {
	switch s {
	case PsUnknown:
		return pb.PlacementState_PS_UNKNOWN
	case PsPending:
		return pb.PlacementState_PS_PENDING
	case PsInactive:
		return pb.PlacementState_PS_INACTIVE
	case PsActive:
		return pb.PlacementState_PS_ACTIVE
	case PsGiveUp:
		return pb.PlacementState_PS_GIVE_UP
	case PsDropped:
		return pb.PlacementState_PS_DROPPED
	}

	panic(fmt.Sprintf("unknown PlacementState: %#v", s))
}

func PlacementStateFromProto(s *pb.PlacementState) PlacementState {
	switch *s {
	case pb.PlacementState_PS_UNKNOWN:
		return PsUnknown
	case pb.PlacementState_PS_PENDING:
		return PsPending
	case pb.PlacementState_PS_INACTIVE:
		return PsInactive
	case pb.PlacementState_PS_ACTIVE:
		return PsActive
	case pb.PlacementState_PS_GIVE_UP:
		return PsGiveUp
	case pb.PlacementState_PS_DROPPED:
		return PsDropped
	}

	log.Printf("warn: unknown PlacementState from proto: %v", *s)
	return PsUnknown
}
