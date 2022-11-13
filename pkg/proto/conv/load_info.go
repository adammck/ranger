package conv

import (
	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func LoadInfoFromProto(li *pb.LoadInfo) api.LoadInfo {
	splits := make([]api.Key, len(li.Splits))
	for i := range li.Splits {
		splits[i] = api.Key(li.Splits[i])
	}

	return api.LoadInfo{
		Keys:   int(li.Keys),
		Splits: splits,
	}
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
