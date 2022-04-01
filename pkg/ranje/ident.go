package ranje

import (
	"errors"
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// Ident is the unique identity of a range. It's just a uint64.

// See: ranger/pkg/proto/models.Ident
type Ident uint64

var IdentInvalid Ident

func (id Ident) String() string {
	return fmt.Sprintf("%d", id)
}

func IdentFromProto(p *pb.Ident) (Ident, error) {
	id := Ident(p.Key)

	// Empty scope is fine.

	if id == 0 {
		return id, errors.New("missing: key")
	}

	return id, nil
}

func (id Ident) ToProto() *pb.Ident {
	return &pb.Ident{
		Key: uint64(id),
	}
}
