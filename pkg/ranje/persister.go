package ranje

type Persister interface {

	// GetRanges returns the latest snapshot of all known ranges. It's called
	// once, at controller startup.
	GetRanges() ([]*Range, error)

	PutRange(*Range) error
}
