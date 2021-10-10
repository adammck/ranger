package ranje

import (
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type Meta struct {
	Ident Ident
	Start Key // inclusive
	End   Key // exclusive
}

func MetaFromProto(p *pb.RangeMeta) (*Meta, error) {
	id, err := IdentFromProto(p.Ident)
	if err != nil {
		return nil, err
	}

	return &Meta{
		Ident: *id,
		Start: Key(p.Start),
		End:   Key(p.End),
	}, nil
}

func (m *Meta) ToProto() *pb.RangeMeta {
	return &pb.RangeMeta{
		Ident: m.Ident.ToProto(),
		Start: []byte(m.Start),
		End:   []byte(m.End),
	}
}
