package roster

import (
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// See: ranger/pkg/proto/node.proto:RangeInfo.State
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
