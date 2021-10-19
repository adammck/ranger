package ranje

import (
	"errors"
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
	if id.Scope == "" {
		return fmt.Sprintf("I{%d}", id.Key)
	}

	return fmt.Sprintf("I{%s:%d}", id.Scope, id.Key)
}

func IdentFromProto(p *pb.Ident) (*Ident, error) {
	id := &Ident{
		Scope: p.Scope,
		Key:   p.Key,
	}

	// Empty scope is fine.

	if p.Key == 0 {
		return id, errors.New("missing: key")
	}

	return id, nil
}

func (id *Ident) ToProto() *pb.Ident {
	return &pb.Ident{
		Scope: id.Scope,
		Key:   id.Key,
	}
}
