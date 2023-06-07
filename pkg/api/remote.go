package api

import (
	"fmt"
)

// Remote represents a service listening on some remote host and port. They're
// returned by discovery. This is most often used to refer to nodes/rangelets,
// but isn't limited to that -- clients use it to find the controller, and I
// have vague ideas about rangelets finding distributors in future. That is why
// Ident is a string and not simply a NodeID.
//
// Ident must be globally unique and stable within a ranger installation, since
// they are used to refer to *logical* service instances (which may have state)
// as they are rescheduled between machines. (For example, a k8s pod using local
// storage may be rescheduled on the same host with a different ip, or even on
// a different host if using e.g. an EBS volume.)
//
// TODO: Should we remove support for non-node/rangelet use-cases? It would
// simplify the api. If not, should we store the remote type, too?
type Remote struct {
	Ident string
	Host  string
	Port  int
}

// Addr returns an address which can be dialled to connect to the remote.
func (r Remote) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// NodeID returns the remote ident as a NodeID, since that's most often how it's
// used, though it isn't one.
func (r Remote) NodeID() NodeID {
	return NodeID(r.Ident)
}
