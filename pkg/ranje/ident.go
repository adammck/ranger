package ranje

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// Ident is the unique identity of a range.

// See: ranger/pkg/proto/models.Ident
type Ident struct {
	Scope string
	Key   uint64
}

func (id *Ident) String() string {
	//return fmt.Sprintf("Ident{%s:%d}", id.Scope, id.Key)
	return fmt.Sprintf("%#v", id)
}

func IdentFromProto(p *pb.Ident) Ident {
	return Ident{
		Scope: p.Scope,
		Key:   p.Key,
	}
}

func (id *Ident) ToProto() *pb.Ident {
	return &pb.Ident{
		Scope: id.Scope,
		Key:   id.Key,
	}
}
