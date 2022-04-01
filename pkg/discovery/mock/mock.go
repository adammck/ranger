package consul

import (
	discovery "github.com/adammck/ranger/pkg/discovery"
)

// TODO: Methods to add/remove remotes.
type MockDiscovery struct {
}

func New() (*MockDiscovery, error) {
	return &MockDiscovery{}, nil
}

func (d *MockDiscovery) Start() error {
	return nil
}

func (d *MockDiscovery) Stop() error {
	return nil
}

func (d *MockDiscovery) Get(name string) ([]discovery.Remote, error) {
	return []discovery.Remote{}, nil
}
