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
	Keys   uint64
	Splits []ranje.Key
}

func LoadInfoToProto(li LoadInfo) *pb.LoadInfo {
	splits := make([]string, len(li.Splits))
	for i := range li.Splits {
		splits[i] = string(li.Splits[i])
	}

	return &pb.LoadInfo{
		Keys:   li.Keys,
		Splits: splits,
	}
}

func LoadInfoFromProto(pbli *pb.LoadInfo) LoadInfo {
	splits := make([]ranje.Key, len(pbli.Splits))
	for i := range pbli.Splits {
		splits[i] = ranje.Key(pbli.Splits[i])
	}

	return LoadInfo{
		Keys:   pbli.Keys,
		Splits: splits,
	}
}

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  ranje.Meta
	State state.RemoteState
	Info  LoadInfo
}

func RangeInfoToProto(ri RangeInfo) *pb.RangeInfo {
	return &pb.RangeInfo{
		Meta:  ranje.MetaToProto(ri.Meta),
		State: state.RemoteStateToProto(ri.State),
		Info:  LoadInfoToProto(ri.Info),
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
