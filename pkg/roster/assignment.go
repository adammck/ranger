package roster

import (
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// See: ranger/pkg/proto/node.proto:RangeInfo.State
type NodeRangeState uint8

const (
	rsUnknown NodeRangeState = iota
	rsFetching
	rsFetched
	rsFetchFailed
	rsReady
	rsTaken
)

func FromProto(s pb.RangeInfo_State) NodeRangeState {
	switch s {
	case pb.RangeInfo_FETCHING:
		return rsFetching
	case pb.RangeInfo_FETCHED:
		return rsFetched
	case pb.RangeInfo_FETCH_FAILED:
		return rsFetchFailed
	case pb.RangeInfo_READY:
		return rsReady
	case pb.RangeInfo_TAKEN:
		return rsTaken
	}

	return rsUnknown
}

func (rs NodeRangeState) ToProto() pb.RangeInfo_State {
	switch rs {
	case rsFetching:
		return pb.RangeInfo_FETCHING
	case rsFetched:
		return pb.RangeInfo_FETCHED
	case rsFetchFailed:
		return pb.RangeInfo_FETCH_FAILED
	case rsReady:
		return pb.RangeInfo_READY
	case rsTaken:
		return pb.RangeInfo_TAKEN
	}

	return pb.RangeInfo_UNKNOWN
}

// See: ranger/pkg/proto/node.proto:RangeInfo
type Info struct {
	R *keyspace.Range
	S NodeRangeState

	// The number of keys that the range has.
	K uint64
}
