package discovery

import "github.com/adammck/ranger/pkg/api"

// Discoverable is an interface to make oneself discoverable (by name), and
// discovering other services by name.
//
// This is not a general-purpose service discovery interface! This is just the
// specific thing that I need for this library, to avoid letting Consul details
// get all over the place.
//
// TODO: Extract the parts of these implementations which belong in Discoverer!
//       Some services don't need to do both things. And even if they do, it's
//       possible that they want to use different implementations.
//
type Discoverable interface {
	Start() error
	Stop() error
	Get(string) ([]api.Remote, error)
}

// Discoverer is an interface to find other services by name.
type Discoverer interface {
	Discover(svcName string, add, remove func(api.Remote)) Getter
}

type Getter interface {

	// Get returns all of the currently known remotes.
	// TODO: Support some kind of filters here, like region and AZ.
	Get() ([]api.Remote, error)

	// Stop terminates this getter. It should not call the remove callback for
	// any known remotes. Get will return no results after this is called.
	Stop() error
}
