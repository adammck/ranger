package ranje

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// Meta is a range minus all the state.
// Should be immutable after construction.
type Meta struct {
	Ident Ident
	Start Key // inclusive
	End   Key // exclusive
}

func (m *Meta) String() string {
	var s, e string

	if m.Start == ZeroKey {
		s = "[-inf"
	} else {
		s = fmt.Sprintf("(%s", m.Start)
	}

	if m.End == ZeroKey {
		e = "+inf]"
	} else {
		e = fmt.Sprintf("%s]", m.End)
	}

	return fmt.Sprintf("M{%s %s, %s}", m.Ident.String(), s, e)
}

func (m *Meta) Contains(k Key) bool {
	if m.Start != ZeroKey {
		if k < m.Start {
			return false
		}
	}

	if m.End != ZeroKey {
		// Note that the range end is exclusive!
		if k >= m.End {
			return false
		}
	}

	return true
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
