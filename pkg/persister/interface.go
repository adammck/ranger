package persister

import "github.com/adammck/ranger/pkg/ranje"

type Persister interface {

	// GetRanges returns the latest snapshot of all known ranges. It's called
	// once, at controller startup.
	GetRanges() ([]*ranje.Range, error)

	// PutRanges writes all of the given Ranges to the store. Implementations
	// must be transactional, so either they all succeed or none do.
	PutRanges([]*ranje.Range) error
}
