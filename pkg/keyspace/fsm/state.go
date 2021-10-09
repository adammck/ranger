package fsm

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type State uint8

// TODO: Is joining and splitting the same state?

const (
	// ???
	Unknown State = iota

	// Pending: The default state. The range is known (by the controller) but
	// hasn't been assigned to any node. When a range is in this state, the
	// controller should find a node and Give the range to it asap.
	//
	// -> Ready: The node accepted it, and didn't need to do anything to become
	//           ready. Maybe it was a genesis range?
	//
	// -> Fetching: The node accepted it, and is getting ready to serve it. The
	//              controller should wait.
	Pending

	// Fetching: The range is assigned to a node, and the node is getting ready
	// to serve it. The controller should keep probing for updates.
	//
	// -> FetchFailed: The node tried to fetch the range, but couldn't, for
	//                 whatever reason.
	//
	// -> FetchRevoked: The controller lost contact with the node while it was
	//                  fetching the range. Maybe it OOMed?
	//
	//Fetching -- TODO

	// Ready: The range is owned by a node, and is in the steady state.
	Ready

	// Splitting: The controller has decided to split the range. The child
	// ranges have been created and are in the process of becoming ready.
	Splitting

	// Joining: Same as splitting.
	Joining

	// Obsolete: The range was split or joined, and the child ranges are now
	// ready, so this range can now be unassigned from its node.
	Obsolete

	// Discarding
)

//go:generate stringer -type=State

func FromProto(s *pb.RangeInfo_State) State {
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

	fmt.Printf("warn: got unknown state from proto: %s\n", *s)
	return Unknown
}

func (s *State) ToProto() pb.RangeInfo_State {
	return pb.RangeInfo_UNKNOWN
}
