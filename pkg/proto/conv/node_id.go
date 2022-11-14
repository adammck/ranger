package conv

import (
	"errors"

	"github.com/adammck/ranger/pkg/api"
)

var ErrMissingNodeID = errors.New("missing node ID")

func NodeIDFromProto(p string) (api.NodeID, error) {
	id := api.NodeID(p)

	if id == api.ZeroNodeID {
		return id, ErrMissingNodeID
	}

	return id, nil
}

func NodeIDToProto(nID api.NodeID) string {
	return string(nID)
}
