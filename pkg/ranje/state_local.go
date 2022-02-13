package ranje

import (
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type StateLocal uint8

// TODO: Is joining and splitting the same state?

const (
	// ???
	Unknown StateLocal = iota

	// Pending: The default state. The range is known (by the controller) but
	// hasn't been assigned to any node. When a range is in this state, the
	// controller should find a node and Give the range to it asap.
	Pending

	// Placing: The balancer is finding a node with capacity to place this
	//          range, and exchanging RPCs.
	Placing

	// PlaceError: We tried to place the range on a node, but it failed. It may
	//             proceed back to Pending, to be placed, or Quarantined, to
	//             wait for operator intervention.
	PlaceError

	// Quarantined: The range is not placed on any nodes, and should remain so
	//              until an operator intervenes. This indicates that the range
	Quarantined

	// Ready: The range is owned by a node, and is in the steady state.
	Ready

	// Moving: The range is moving to a different node. It doesn't need
	// splitting, but we want to reduce load on the current node.
	Moving

	// Splitting: The controller has decided to split the range. The child
	// ranges have been created and are in the process of becoming ready.
	Splitting

	// Joining: Same as splitting.
	Joining

	// Obsolete: The range was split or joined, and the child ranges are now
	// ready, so this range can now be unassigned from its node.
	Obsolete
)

//go:generate stringer -type=StateLocal -output=zzz_state_local_string.go

func FromProto(s *pb.RangeNodeState) StateLocal {
	// switch *s {
	// case pb.RangeInfo_UNKNOWN:
	// 	return Unknown
	// case pb.RangeInfo_FETCHING:
	// 	return Fetching
	// case pb.RangeInfo_FETCHED:
	// 	return Fetched
	// case pb.RangeInfo_FETCH_FAILED:
	// 	return FetchFailed
	// case pb.RangeInfo_READY:
	// 	return Ready
	// case pb.RangeInfo_TAKEN:
	// 	return Taken
	// }

	log.Printf("warn: got unknown state from proto: %s", *s)
	return Unknown
}

func (s *StateLocal) ToProto() pb.RangeNodeState {
	return pb.RangeNodeState_UNKNOWN
}
