package info

import (
	"fmt"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/state"
)

// TODO: Make node ID a proper type like range ID.

type NodeInfo struct {
	Time   time.Time
	NodeID string
	Ranges []RangeInfo

	// Expired is true when the node was automatically expired because we
	// haven't been able to probe it in a while.
	Expired bool
}

// Same as rangelet.LoadInfo
// TODO: Would be nice to just use a rangelet.LoadInfo here, but circular
//       import! Remove roster/info import from rangelet, then resolve this.
type LoadInfo struct {
	Keys uint64
}

func (li *LoadInfo) ToProto() *pb.LoadInfo {
	return &pb.LoadInfo{
		Keys: li.Keys,
	}
}

func LoadInfoFromProto(pbli *pb.LoadInfo) LoadInfo {
	return LoadInfo{
		Keys: pbli.Keys,
	}
}

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  ranje.Meta
	State state.RemoteState
	Info  LoadInfo
}

func (ri *RangeInfo) ToProto() *pb.RangeInfo {
	return &pb.RangeInfo{
		Meta:  ri.Meta.ToProto(),
		State: ri.State.ToProto(),
		Info:  ri.Info.ToProto(),
	}
}

func RangeInfoFromProto(r *pb.RangeInfo) (RangeInfo, error) {
	if r.Meta == nil {
		return RangeInfo{}, fmt.Errorf("missing: meta")
	}

	m, err := ranje.MetaFromProto(r.Meta)
	if err != nil {
		return RangeInfo{}, fmt.Errorf("parsing meta: %v", err)
	}

	return RangeInfo{
		Meta:  *m,
		State: state.RemoteStateFromProto(r.State),
		Info:  LoadInfoFromProto(r.Info),
	}, nil
}
