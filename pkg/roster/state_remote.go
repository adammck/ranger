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
	NsReadyingError
	NsReady
	NsTaking
	NsTakingError
	NsTaken
	NsDropping
	NsDroppingError

	// Special case: This is never returned by probes, since those only include
	// the state of ranges which the node has. This is returned by redundant
	// Drop RPCs which instruct nodes to drop a range that they don't have.
	// (Maybe it was already dropped, or maybe the node never had it. Can't
	// know.) This is a success, not an error, because those RPCs may be
	// received multiple times during a normal drop, and should be treated
	// idempotently. But we don't want to return NsUnknown, because we do know.
	NsNotFound
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
	case pb.RangeNodeState_NOT_FOUND:
		return NsNotFound
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
	case NsNotFound:
		return pb.RangeNodeState_NOT_FOUND
	}

	panic(fmt.Sprintf("ToProto got unknown node state: %#v", rs))
	//return pb.RangeNodeState_UNKNOWN
}
