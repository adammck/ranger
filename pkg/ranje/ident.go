package ranje

import (
	"errors"
	"fmt"
)

// Ident is the unique identity of a range. It's just a uint64.

type Ident uint64

var ZeroRange Ident

func (id Ident) String() string {
	return fmt.Sprintf("%d", id)
}

func IdentFromProto(p uint64) (Ident, error) {
	id := Ident(p)

	if id == ZeroRange {
		return id, errors.New("missing: key")
	}

	return id, nil
}

func (id Ident) ToProto() uint64 {
	return uint64(id)
}
