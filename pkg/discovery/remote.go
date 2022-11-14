package discovery

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

type Remote struct {
	Ident string
	Host  string
	Port  int
}

func (r *Remote) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// NodeID returns the remote ident as an api.NodeID, since that's most often how
// it's used, though it isn't one.
func (r *Remote) NodeID() api.NodeID {
	return api.NodeID(r.Ident)
}
