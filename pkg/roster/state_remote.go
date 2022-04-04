package roster

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// See: ranger/pkg/proto/node.proto:RangeInfo.RemoteState
type RemoteState uint8

const (

	// Should never be in this state. Indicates a bug.
	NsUnknown RemoteState = iota

	// Valid states.
	NsPreparing
	NsPreparingError
	NsPrepared
	NsReadying
	NsReady
	NsTaking
	NsTaken
	NsDropping
	NsDropped
)

//go:generate stringer -type=RemoteState -output=state_string.go

func RemoteStateFromProto(s pb.RangeNodeState) RemoteState {
	switch s {
	case pb.RangeNodeState_UNKNOWN:
		return NsUnknown
	case pb.RangeNodeState_PREPARING:
		return NsPreparing
	case pb.RangeNodeState_PREPARING_ERROR:
		return NsPreparingError
	case pb.RangeNodeState_PREPARED:
		return NsPrepared
	case pb.RangeNodeState_READYING:
		return NsReadying
	case pb.RangeNodeState_READY:
		return NsReady
	case pb.RangeNodeState_TAKING:
		return NsTaking
	case pb.RangeNodeState_TAKEN:
		return NsTaken
	case pb.RangeNodeState_DROPPING:
		return NsDropping
	case pb.RangeNodeState_DROPPED:
		return NsDropped
	}

	panic(fmt.Sprintf("RemoteStateFromProto got unknown node state: %#v", s))
	//return StateUnknown
}

func (rs RemoteState) ToProto() pb.RangeNodeState {
	switch rs {
	case NsUnknown:
		return pb.RangeNodeState_UNKNOWN
	case NsPreparing:
		return pb.RangeNodeState_PREPARING
	case NsPreparingError:
		return pb.RangeNodeState_PREPARING_ERROR
	case NsPrepared:
		return pb.RangeNodeState_PREPARED
	case NsReadying:
		return pb.RangeNodeState_READYING
	case NsReady:
		return pb.RangeNodeState_READY
	case NsTaking:
		return pb.RangeNodeState_TAKING
	case NsTaken:
		return pb.RangeNodeState_TAKEN
	case NsDropping:
		return pb.RangeNodeState_DROPPING
	case NsDropped:
		return pb.RangeNodeState_DROPPED
	}

	panic(fmt.Sprintf("ToProto got unknown node state: %#v", rs))
	//return pb.RangeNodeState_UNKNOWN
}
