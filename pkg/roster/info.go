package roster

import (
	"fmt"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
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

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  ranje.Meta
	State RemoteState
	// TODO: LoadInfo goes here!!
}

func (ri *RangeInfo) ToProto() *pb.RangeInfo {
	return &pb.RangeInfo{
		Meta:  ri.Meta.ToProto(),
		State: ri.State.ToProto(),
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

	// TODO: Update the map rather than overwriting it every time.
	return RangeInfo{
		Meta:  *m,
		State: RemoteStateFromProto(r.State),
		// TODO: LoadInfo
	}, nil
}
