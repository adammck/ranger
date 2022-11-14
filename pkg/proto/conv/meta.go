package conv

import (
	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func MetaFromProto(m *pb.RangeMeta) (*api.Meta, error) {
	id, err := RangeIDFromProto(m.Ident)
	if err != nil {
		return nil, err
	}

	return &api.Meta{
		Ident: id,
		Start: api.Key(m.Start),
		End:   api.Key(m.End),
	}, nil
}

func MetaToProto(m api.Meta) *pb.RangeMeta {
	return &pb.RangeMeta{
		Ident: RangeIDToProto(m.Ident),
		Start: []byte(m.Start),
		End:   []byte(m.End),
	}
}
