package conv

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func RangeStateFromProto(rs pb.RangeState) api.RangeState {
	switch rs {
	case pb.RangeState_RS_UNKNOWN:
		return api.RsUnknown
	case pb.RangeState_RS_ACTIVE:
		return api.RsActive
	case pb.RangeState_RS_SUBSUMING:
		return api.RsSubsuming
	case pb.RangeState_RS_OBSOLETE:
		return api.RsObsolete
	}

	log.Printf("warn: unknown pb.RangeState: %#v", rs)
	return api.RsUnknown
}

func RangeStateToProto(rs api.RangeState) pb.RangeState {
	switch rs {
	case api.RsUnknown:
		return pb.RangeState_RS_UNKNOWN
	case api.RsActive:
		return pb.RangeState_RS_ACTIVE
	case api.RsSubsuming:
		return pb.RangeState_RS_SUBSUMING
	case api.RsObsolete:
		return pb.RangeState_RS_OBSOLETE
	}

	panic(fmt.Sprintf("unknown RangeState: %#v", rs))
}
