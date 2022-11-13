package ranje

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func PlacementStateToProto(s api.PlacementState) pb.PlacementState {
	switch s {
	case api.PsUnknown:
		return pb.PlacementState_PS_UNKNOWN
	case api.PsPending:
		return pb.PlacementState_PS_PENDING
	case api.PsInactive:
		return pb.PlacementState_PS_INACTIVE
	case api.PsActive:
		return pb.PlacementState_PS_ACTIVE
	case api.PsGiveUp:
		return pb.PlacementState_PS_GIVE_UP
	case api.PsDropped:
		return pb.PlacementState_PS_DROPPED
	}

	panic(fmt.Sprintf("unknown PlacementState: %#v", s))
}

func PlacementStateFromProto(s *pb.PlacementState) api.PlacementState {
	switch *s {
	case pb.PlacementState_PS_UNKNOWN:
		return api.PsUnknown
	case pb.PlacementState_PS_PENDING:
		return api.PsPending
	case pb.PlacementState_PS_INACTIVE:
		return api.PsInactive
	case pb.PlacementState_PS_ACTIVE:
		return api.PsActive
	case pb.PlacementState_PS_GIVE_UP:
		return api.PsGiveUp
	case pb.PlacementState_PS_DROPPED:
		return api.PsDropped
	}

	log.Printf("warn: unknown PlacementState from proto: %v", *s)
	return api.PsUnknown
}
