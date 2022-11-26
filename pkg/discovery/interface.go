// This is not a general-purpose service discovery interface! This is just the
// specific things that I need for this library, to avoid letting Consul details
// get all over the place.

package discovery

import "github.com/adammck/ranger/pkg/api"

// Discoverable is an interface to make oneself discoverable (by type). For
// environments where discoverability is implicit, this is unnecessary.
type Discoverable interface {
	Start() error
	Stop() error
}

// Discoverer is an interface to find other services by type (e.g. node).
type Discoverer interface {
	Discover(svcName string, add, remove func(api.Remote)) Getter
}

// Getter is returned by Discoverer.Discover.
type Getter interface {

	// Get returns all of the currently known remotes.
	// TODO: Support some kind of filters here, like region and AZ.
	// TODO: Update callers to use add/remove callbacks and remove this method.
	Get() ([]api.Remote, error)

	// Stop terminates this getter. It should not call the remove callback for
	// any known remotes. Get will return no results after this is called.
	Stop() error
}
