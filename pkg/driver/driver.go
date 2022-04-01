package driver

import "github.com/adammck/ranger/pkg/ranje"

type Driver struct {
	ks *ranje.Keyspace
}

func New(ks *ranje.Keyspace) *Driver {
	return &Driver{
		ks: ks,
	}
}

func (d *Driver) Step() {
	d.ks.Step()
}
