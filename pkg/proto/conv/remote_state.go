package conv

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func RemoteStateFromProto(s pb.RangeNodeState) api.RemoteState {
	switch s {
	case pb.RangeNodeState_UNKNOWN:
		return api.NsUnknown
	case pb.RangeNodeState_INACTIVE:
		return api.NsInactive
	case pb.RangeNodeState_ACTIVE:
		return api.NsActive
	case pb.RangeNodeState_PREPARING:
		return api.NsPreparing
	case pb.RangeNodeState_ACTIVATING:
		return api.NsActivating
	case pb.RangeNodeState_DEACTIVATING:
		return api.NsDeactivating
	case pb.RangeNodeState_DROPPING:
		return api.NsDropping
	case pb.RangeNodeState_NOT_FOUND:
		return api.NsNotFound
	}

	log.Printf("warn: unknown pb.RangeNodeState: %#v", s)
	return api.NsUnknown
}

func RemoteStateToProto(rs api.RemoteState) pb.RangeNodeState {
	switch rs {
	case api.NsUnknown:
		return pb.RangeNodeState_UNKNOWN
	case api.NsInactive:
		return pb.RangeNodeState_INACTIVE
	case api.NsActive:
		return pb.RangeNodeState_ACTIVE
	case api.NsPreparing:
		return pb.RangeNodeState_PREPARING
	case api.NsActivating:
		return pb.RangeNodeState_ACTIVATING
	case api.NsDeactivating:
		return pb.RangeNodeState_DEACTIVATING
	case api.NsDropping:
		return pb.RangeNodeState_DROPPING
	case api.NsNotFound:
		return pb.RangeNodeState_NOT_FOUND
	}

	//return pb.RangeNodeState_UNKNOWN
	panic(fmt.Sprintf("unknown RemoteState: %#v", rs))
}
