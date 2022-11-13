package ranje

import (
	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

func MetaFromProto(p *pb.RangeMeta) (*api.Meta, error) {
	id, err := IdentFromProto(p.Ident)
	if err != nil {
		return nil, err
	}

	return &api.Meta{
		Ident: id,
		Start: api.Key(p.Start),
		End:   api.Key(p.End),
	}, nil
}

func MetaToProto(m api.Meta) *pb.RangeMeta {
	return &pb.RangeMeta{
		Ident: IdentToProto(m.Ident),
		Start: []byte(m.Start),
		End:   []byte(m.End),
	}
}
