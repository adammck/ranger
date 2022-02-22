package roster

import (
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

// See: ranger/pkg/proto/node.proto:RangeInfo.State
// TODO: Remove this and replace callers with ranje.StatePlacement. This is a subset!
type State uint8

const (
	StateUnknown State = iota
	StateFetching
	StateFetched
	StateFetchFailed
	StateReady
	StateTaken
)

//go:generate stringer -type=State -output=state_string.go

func RemoteStateFromProto(s pb.RangeNodeState) State {
	switch s {
	case pb.RangeNodeState_FETCHING:
		return StateFetching
	case pb.RangeNodeState_FETCHED:
		return StateFetched
	case pb.RangeNodeState_FETCH_FAILED:
		return StateFetchFailed
	case pb.RangeNodeState_READY:
		return StateReady
	case pb.RangeNodeState_TAKEN:
		return StateTaken
	}

	return StateUnknown
}

func (rs State) ToProto() pb.RangeNodeState {
	switch rs {
	case StateFetching:
		return pb.RangeNodeState_FETCHING
	case StateFetched:
		return pb.RangeNodeState_FETCHED
	case StateFetchFailed:
		return pb.RangeNodeState_FETCH_FAILED
	case StateReady:
		return pb.RangeNodeState_READY
	case StateTaken:
		return pb.RangeNodeState_TAKEN
	}

	return pb.RangeNodeState_UNKNOWN
}

// TODO: Remove this whole type! Just use StatePlacement.
func (rs State) ToStatePlacement() ranje.StatePlacement {
	switch rs {
	case StateFetching:
		return ranje.SpFetching
	case StateFetched:
		return ranje.SpFetched
	case StateFetchFailed:
		return ranje.SpFetchFailed
	case StateReady:
		return ranje.SpReady
	case StateTaken:
		return ranje.SpTaken
	}

	return ranje.SpUnknown
}
