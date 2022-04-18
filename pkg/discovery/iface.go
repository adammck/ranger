package discovery

import "fmt"

type Remote struct {
	Ident string
	Host  string
	Port  int
}

func (r *Remote) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// Discoverable is an interface to make oneself discoverable (by name), and
// discovering other services by name.
//
// This is not a general-purpose service discovery interface! This is just the
// specific thing that I need for this library, to avoid letting Consul details
// get all over the place.
type Discoverable interface {
	Start() error
	Stop() error
	Get(string) ([]Remote, error)
}
