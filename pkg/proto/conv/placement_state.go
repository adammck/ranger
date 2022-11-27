package conv

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func PlacementStateFromProto(ps pb.PlacementState) api.PlacementState {
	switch ps {
	case pb.PlacementState_PS_UNKNOWN:
		return api.PsUnknown
	case pb.PlacementState_PS_PENDING:
		return api.PsPending
	case pb.PlacementState_PS_INACTIVE:
		return api.PsInactive
	case pb.PlacementState_PS_ACTIVE:
		return api.PsActive
	case pb.PlacementState_PS_MISSING:
		return api.PsMissing
	case pb.PlacementState_PS_DROPPED:
		return api.PsDropped
	}

	log.Printf("warn: unknown pb.PlacementState: %#v", ps)
	return api.PsUnknown
}

func PlacementStateToProto(ps api.PlacementState) pb.PlacementState {
	switch ps {
	case api.PsUnknown:
		return pb.PlacementState_PS_UNKNOWN
	case api.PsPending:
		return pb.PlacementState_PS_PENDING
	case api.PsInactive:
		return pb.PlacementState_PS_INACTIVE
	case api.PsActive:
		return pb.PlacementState_PS_ACTIVE
	case api.PsMissing:
		return pb.PlacementState_PS_MISSING
	case api.PsDropped:
		return pb.PlacementState_PS_DROPPED
	}

	panic(fmt.Sprintf("unknown PlacementState: %#v", ps))
}
