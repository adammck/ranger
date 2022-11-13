package conv

import (
	"errors"

	"github.com/adammck/ranger/pkg/api"
)

func IdentFromProto(p uint64) (api.Ident, error) {
	id := api.Ident(p)

	if id == api.ZeroRange {
		return id, errors.New("missing: key")
	}

	return id, nil
}

func IdentToProto(ident api.Ident) uint64 {
	return uint64(ident)
}
