package info

import (
	"fmt"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/roster/state"
)

// TODO: Make node ID a proper type like range ID.

type NodeInfo struct {
	Time   time.Time
	NodeID string
	Ranges []api.RangeInfo

	// Expired is true when the node was automatically expired because we
	// haven't been able to probe it in a while.
	Expired bool
}

func LoadInfoToProto(li api.LoadInfo) *pb.LoadInfo {
	splits := make([]string, len(li.Splits))
	for i := range li.Splits {
		splits[i] = string(li.Splits[i])
	}

	return &pb.LoadInfo{
		Keys:   uint64(li.Keys),
		Splits: splits,
	}
}

func LoadInfoFromProto(pbli *pb.LoadInfo) api.LoadInfo {
	splits := make([]api.Key, len(pbli.Splits))
	for i := range pbli.Splits {
		splits[i] = api.Key(pbli.Splits[i])
	}

	return api.LoadInfo{
		Keys:   int(pbli.Keys),
		Splits: splits,
	}
}

func RangeInfoToProto(ri api.RangeInfo) *pb.RangeInfo {
	return &pb.RangeInfo{
		Meta:  conv.MetaToProto(ri.Meta),
		State: state.RemoteStateToProto(ri.State),
		Info:  LoadInfoToProto(ri.Info),
	}
}

func RangeInfoFromProto(r *pb.RangeInfo) (api.RangeInfo, error) {
	if r.Meta == nil {
		return api.RangeInfo{}, fmt.Errorf("missing: meta")
	}

	m, err := conv.MetaFromProto(r.Meta)
	if err != nil {
		return api.RangeInfo{}, fmt.Errorf("parsing meta: %v", err)
	}

	return api.RangeInfo{
		Meta:  *m,
		State: state.RemoteStateFromProto(r.State),
		Info:  LoadInfoFromProto(r.Info),
	}, nil
}
