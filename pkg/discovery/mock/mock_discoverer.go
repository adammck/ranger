package mock

import (
	"sync"

	"github.com/adammck/ranger/pkg/api"
	discovery "github.com/adammck/ranger/pkg/discovery"
)

type Discoverer struct {
	getters []*discoveryGetter

	// svcName (e.g. "node") -> remotes
	remotes   map[string][]api.Remote
	remotesMu sync.RWMutex
}

func NewDiscoverer() *Discoverer {
	return &Discoverer{
		remotes: map[string][]api.Remote{},
	}
}

type discoveryGetter struct {
	disc    *Discoverer
	svcName string

	// Functions to be called when new remotes are added and removed.
	add    func(api.Remote)
	remove func(api.Remote)
}

func (d *Discoverer) Discover(svcName string, add, remove func(api.Remote)) discovery.Getter {
	dg := &discoveryGetter{
		disc:    d,
		svcName: svcName,
		add:     add,
		remove:  remove,
	}
	d.getters = append(d.getters, dg)
	return dg
}

func (dg *discoveryGetter) Get() ([]api.Remote, error) {
	dg.disc.remotesMu.RLock()
	defer dg.disc.remotesMu.RUnlock()

	remotes, ok := dg.disc.remotes[dg.svcName]
	if !ok {
		return []api.Remote{}, nil
	}

	res := make([]api.Remote, len(remotes))
	copy(res, remotes)

	return res, nil
}

func (dg *discoveryGetter) Stop() error {
	return nil
}

// test helpers

func (d *Discoverer) Add(svcName string, remote api.Remote) {
	d.remotesMu.RLock()
	defer d.remotesMu.RUnlock()

	// TODO: Need to init slice?
	d.remotes[svcName] = append(d.remotes[svcName], remote)

	for _, dg := range d.getters {
		dg.add(remote)
	}
}
