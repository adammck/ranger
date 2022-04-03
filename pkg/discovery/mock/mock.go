package consul

import (
	"sync"

	"github.com/adammck/ranger/pkg/discovery"
)

// TODO: Methods to add/remove remotes.
type MockDiscovery struct {
	remotes map[string][]discovery.Remote
	sync.RWMutex
}

func New(remotes map[string][]discovery.Remote) (*MockDiscovery, error) {
	return &MockDiscovery{remotes: remotes}, nil
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

	rems, ok := d.remotes[name]
	if !ok {
		return []discovery.Remote{}, nil
	}

	return rems, nil
}

// test helpers

func (d *MockDiscovery) Set(name string, remotes []discovery.Remote) {
	d.Lock()
	defer d.Unlock()
	d.remotes[name] = remotes
}
