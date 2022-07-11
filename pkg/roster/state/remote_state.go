package state

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// See: ranger/pkg/proto/node.proto:RangeNodeState
type RemoteState uint8

const (

	// Should never be in this state. Indicates a bug.
	NsUnknown RemoteState = iota

	// Stable states
	NsInactive
	NsActive

	// During transitions
	NsLoading      // Pending  -> PreReady
	NsActivating   // PreReady -> Ready
	NsDeactivating // Ready    -> PreReady
	NsDropping     // PreReady -> NotFound

	// Special case: This is never returned by probes, since those only include
	// the state of ranges which the node has. This is returned by redundant
	// Drop RPCs which instruct nodes to drop a range that they don't have.
	// (Maybe it was already dropped, or maybe the node never had it. Can't
	// know.) This is a success, not an error, because those RPCs may be
	// received multiple times during a normal drop, and should be treated
	// idempotently. But we don't want to return NsUnknown, because we do know.
	NsNotFound
)

//go:generate stringer -type=RemoteState -output=remote_state_string.go

func RemoteStateFromProto(s pb.RangeNodeState) RemoteState {
	switch s {
	case pb.RangeNodeState_UNKNOWN:
		return NsUnknown

	case pb.RangeNodeState_INACTIVE:
		return NsInactive
	case pb.RangeNodeState_ACTIVE:
		return NsActive

	case pb.RangeNodeState_LOADING:
		return NsLoading
	case pb.RangeNodeState_ACTIVATING:
		return NsActivating
	case pb.RangeNodeState_DEACTIVATING:
		return NsDeactivating
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

	case NsInactive:
		return pb.RangeNodeState_INACTIVE
	case NsActive:
		return pb.RangeNodeState_ACTIVE

	case NsLoading:
		return pb.RangeNodeState_LOADING
	case NsActivating:
		return pb.RangeNodeState_ACTIVATING
	case NsDeactivating:
		return pb.RangeNodeState_DEACTIVATING
	case NsDropping:
		return pb.RangeNodeState_DROPPING

	case NsNotFound:
		return pb.RangeNodeState_NOT_FOUND
	}

	panic(fmt.Sprintf("ToProto got unknown node state: %#v", rs))
	//return pb.RangeNodeState_UNKNOWN
}
