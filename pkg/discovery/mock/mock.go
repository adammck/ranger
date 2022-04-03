package consul

import (
	"sync"

	"github.com/adammck/ranger/pkg/discovery"
)

// TODO: Methods to add/remove remotes.
type MockDiscovery struct {
	Remotes map[string][]discovery.Remote
	sync.RWMutex
}

func New() *MockDiscovery {
	return &MockDiscovery{
		Remotes: map[string][]discovery.Remote{},
	}
}

// interface

func (d *MockDiscovery) Start() error {
	return nil
}

func (d *MockDiscovery) Stop() error {
	return nil
}

func (d *MockDiscovery) Get(name string) ([]discovery.Remote, error) {
	d.RLock()
	defer d.RUnlock()

	rems, ok := d.Remotes[name]
	if !ok {
		return []discovery.Remote{}, nil
	}

	return rems, nil
}

// test helpers

func (d *MockDiscovery) Set(name string, remotes []discovery.Remote) {
	d.Lock()
	defer d.Unlock()
	d.Remotes[name] = remotes
}

func (d *MockDiscovery) Add(name string, remote discovery.Remote) {
	d.Lock()
	defer d.Unlock()
	d.Remotes[name] = append(d.Remotes[name], remote)
}
