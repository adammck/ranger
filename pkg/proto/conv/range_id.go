package conv

import (
	"errors"

	"github.com/adammck/ranger/pkg/api"
)

func RangeIDFromProto(p uint64) (api.RangeID, error) {
	id := api.RangeID(p)

	if id == api.ZeroRange {
		return id, errors.New("missing: key")
	}

	return id, nil
}

func RangeIDToProto(ident api.RangeID) uint64 {
	return uint64(ident)
}
