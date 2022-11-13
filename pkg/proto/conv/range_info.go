package conv

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func RangeInfoFromProto(r *pb.RangeInfo) (api.RangeInfo, error) {
	if r.Meta == nil {
		return api.RangeInfo{}, fmt.Errorf("missing: meta")
	}

	m, err := MetaFromProto(r.Meta)
	if err != nil {
		return api.RangeInfo{}, fmt.Errorf("parsing meta: %v", err)
	}

	return api.RangeInfo{
		Meta:  *m,
		State: RemoteStateFromProto(r.State),
		Info:  LoadInfoFromProto(r.Info),
	}, nil
}

func RangeInfoToProto(ri api.RangeInfo) *pb.RangeInfo {
	return &pb.RangeInfo{
		Meta:  MetaToProto(ri.Meta),
		State: RemoteStateToProto(ri.State),
		Info:  LoadInfoToProto(ri.Info),
	}
}
